package net.topikachu.rag.service.etl;

import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import lombok.extern.slf4j.Slf4j;
import net.topikachu.rag.business.document.entity.AclRefreshStatus;
import net.topikachu.rag.business.document.entity.DocumentStatus;
import net.topikachu.rag.business.document.entity.KnowledgeParentBlock;
import net.topikachu.rag.business.document.mapper.DocumentMapper;
import net.topikachu.rag.observability.TracingSupport;
import net.topikachu.rag.service.etl.fileParseStrategy.FileParseStrategy;
import net.topikachu.rag.service.etl.fileParseStrategy.FileParseStrategyFactory;
import org.springframework.ai.document.Document;
import org.springframework.ai.reader.tika.TikaDocumentReader;
import org.springframework.ai.transformer.splitter.TextSplitter;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.time.LocalDateTime;
import java.util.*;

/**
 * ETL 摄取管线：驱动文档状态机 UPLOADED → SPLITTING → VECTORIZING → COMPLETED/FAILED，
 * 协调读取、分块、向量化写入及失败补偿清理。
 */
@Component
@Slf4j
public class EtlPipeline {

    private final DocReader documentReader;
    private final HybridVectorWriter hybridVectorWriter;
    private final TextSplitter textSplitter;
    private final DocumentMapper documentMapper;
    private final DocumentChunkMetadataBuilder metadataBuilder;
    private final TracingSupport tracingSupport;
    private final FileParseStrategyFactory fileParseStrategyFactory;
    private final EtlStatusManager etlStatusManager;
    private final KnowledgeParentBlockService parentBlockService;

    // 各阶段独立超时：read/split 受 CPU 和 Tika 解析影响，vectorize 受网络和 Milvus 写入影响，需分别控制
    @org.springframework.beans.factory.annotation.Value("${rag.etl.timeout.read-split-seconds:60}")
    private long SplitTimeoutSeconds;

    @org.springframework.beans.factory.annotation.Value("${rag.etl.timeout.vectorize-seconds:300}")
    private long vectorizeTimeoutSeconds;

    // reading 阶段超时最长（900s），因为 Tika 解析大文件（如扫描 PDF OCR）可能耗时数分钟
    @org.springframework.beans.factory.annotation.Value("${rag.etl.timeout.read-seconds:900}")
    private long readingTimeoutSeconds;

    @org.springframework.beans.factory.annotation.Value("${rag.retrieval.parent-block-size:1200}")
    private int parentBlockSize;

    @org.springframework.beans.factory.annotation.Value("${rag.retrieval.parent-block-overlap:200}")
    private int parentBlockOverlap;

    public EtlPipeline(HybridVectorWriter hybridVectorWriter,
                       TextSplitter textSplitter,
                       DocReader documentReader,
                       DocumentMapper documentMapper,
                       DocumentChunkMetadataBuilder metadataBuilder,
                       TracingSupport tracingSupport,
                       FileParseStrategyFactory fileParseStrategyFactory,
                       EtlStatusManager etlStatusManager,
                       KnowledgeParentBlockService parentBlockService) {
        this.hybridVectorWriter = hybridVectorWriter;
        this.textSplitter = textSplitter;
        this.documentReader = documentReader;
        this.documentMapper = documentMapper;
        this.metadataBuilder = metadataBuilder;
        this.tracingSupport = tracingSupport;
        this.fileParseStrategyFactory = fileParseStrategyFactory;
        this.etlStatusManager = etlStatusManager;
        this.parentBlockService = parentBlockService;
    }

    // 目录扫描式批量摄入的预留入口，当前未实现具体逻辑，始终返回空流
    public Flux<Document> ingestionFlux() {
        return documentReader.scanDirectory()
                .flatMap(path -> Mono.fromCallable(() -> {
                            String hash = computeHash(path);
                            Long count = documentMapper.selectCount(
                                    Wrappers.<net.topikachu.rag.business.document.entity.Document>lambdaQuery()
                                            .eq(net.topikachu.rag.business.document.entity.Document::getFileHash, hash));
                            if (count > 0)
                                return null;
                            return hash;
                        }).subscribeOn(Schedulers.boundedElastic())
                        .flatMapMany(hash -> {
                            if (hash == null)
                                return Mono.empty();
                            return Mono.empty();
                        }));
    }

    /**
     * 文档入库（ETL）的核心流水线，用 Reactor 响应式风格串起了「读取切分 → 写入向量 → 更新状态 → 异常处理」的完整流程
     * ingestionByPath(path, uuid, user, tags)
     *         │
     *         ▼
     *   EtlContext.of(...)
     *         │
     *         ▼
     *  readAndSplit ──timeout──▶ flatMap(writeParentBlocksAndVectors) ──timeout──▶ completeStatus
     *         │                                                                        │
     *         └──────────────── onErrorResume ──▶ failAndCleanup ◀────────────────────┘
     *         │
     *         ▼
     *  traceMono("etl.ingestion", tags)
     *         │
     *         ▼
     *    Mono<Void>
     *
     * 一句话总结：这是一个带分级超时、失败清理、链路追踪的文档 ETL 响应式流水线，把"切分 → 向量化 → 状态更新"串成一条可观测、可容错的 Mono<Void>
     *
     * readAndSplit(ctx)                     ← Extract（读文件）
     *     │
     *     ▼
     * writeParentBlocksAndVectors(ctx, ...) ← Transform（切分好的块 → 生成向量 embedding）
     *     │
     *     ▼
     * completeStatus(ctx)                   ← Load（写入向量库 + 更新文档状态为"完成"）
     *
     * @param path 文件路径
     * @param docUuid 文档 UUID
     * @param userId 用户 ID
     * @param tags 标签列表
     * @return Mono<Void>：只关心完成/失败信号，不产生数据元素（典型的"命令式副作用"方法）
     */
    public Mono<Void> ingestionByPath(Path path, String docUuid, String userId, List<String> tags) {
        // 把零散参数打包成一个 EtlContext，贯穿整个流水线
        EtlContext ctx = EtlContext.of(path, docUuid, userId, tags);

        // 每个 stage 独立超时（非总超时）：readAndSplit 和 writeParentBlocksAndVectors 各自有截止时间
        Mono<Void> pipeline = readAndSplit(ctx) // 读文件、切分成父块/子块，返回 Mono<ParentChildDocs>
                .timeout(java.time.Duration.ofSeconds(SplitTimeoutSeconds)) //给第一步加截止时间（切分超时），是 readAndSplit 的上游操作符，所以只约束切分
                .flatMap(parentChildDocs -> writeParentBlocksAndVectors(ctx, parentChildDocs)) // 写入父块 + 生成向量（第二段耗时操作）
                .timeout(java.time.Duration.ofSeconds(vectorizeTimeoutSeconds)) // 给第二步加截止时间（向量化超时），第二个 timeout 作用在 flatMap 的返回结果上，约束的是 writeParentBlocksAndVectors
                // 两者不是对一个总时长设限，而是各管一段。这比"整个流水线一个总超时"更合理：切分快、向量慢的情况下不会互相挤压配额。
                .then(completeStatus(ctx)) // 忽略上一步结果，执行状态更新（标记成功）
                .doOnSuccess(v -> log.info("IngestionByPath finished: {}", ctx.path())) // 成功后打日志，纯副作用，不影响流
                .onErrorResume(error -> failAndCleanup(ctx, error)); // 任何环节出错都转到 failAndCleanup（标记失败 + 清理已写入的数据）

        // 用 tracingSupport 把整条 pipeline 包进一个 trace span，名字 "etl.ingestion"。
        // ctx.traceTags() 携带 tags（如 docUuid、userId），方便在链路追踪系统里检索
        // 这是观测性（observability）的典型用法：业务代码不侵入，统一在入口包一层
        return tracingSupport.traceMono("etl.ingestion", ctx.traceTags(), pipeline);
    }

    /**
     * 这是一个用 Java record 定义的 上下文对象（Context Object），专门用于在 ETL 流水线的各个阶段之间传递共享数据。它就是你前面看到的那条 ingestionByPath 流水线的"数据胶囊"
     * Java 14 引入、16 正式定稿的 record 是一种不可变数据载体的语法糖
     * public record EtlContext(Path path, String docUuid, ...) {}
     * 编译器会自动帮你生成：
     * 所有字段的 private final 声明；
     * 全参构造方法；
     * 每个字段的访问器（path()、docUuid()……注意没有 get 前缀）；
     * equals() / hashCode() / toString()。
     * 关键特性：不可变（immutable）。字段一旦赋值就不能改，天然线程安全。这对响应式编程（Flux/Mono 可能跨线程调度）非常重要。
     *
     * 业务数据	path、docUuid、userId、tags	流水线真正要处理的文档信息
     * 派生数据	fileName、fileExtension	从 path 自动推导，方便下游直接用
     * 可观测数据	traceTags	给 tracing 系统用的标签（含 docUuid、userId、tags、扩展名等）
     * 为什么要预先把 fileName、fileExtension 算好？
     * 因为它们会被流水线里的多个 stage 反复用到（比如决定用哪个 splitter、写日志、做 trace 标签）。
     * 与其每次从 path 解析，不如在构造时算一次，后续只读。这是典型的"构造期计算、使用期读取"优化。
     *
     * @param path 文件路径
     * @param docUuid 文档唯一标识
     * @param userId 上传/归属的用户
     * @param tags 业务标签（用户自定义）
     * @param fileName 文件名（从 path 推导）
     * @param fileExtension 扩展名（如 "pdf"、"md"）
     * @param traceTags // 链路追踪用的标签键值对
     */
    public record EtlContext(
            Path path, // 文件路径
            String docUuid, // 文档唯一标识
            String userId,  // 上传/归属的用户 ID
            List<String> tags, // 业务标签（用户自定义）
            String fileName, // 文件名（从 path 推导）
            String fileExtension,  // 扩展名（如 "pdf"、"md"）
            Map<String, Object> traceTags // 链路追踪用的标签键值对
    ) {
        // 静态工厂方法 of
        // 作用：给调用方一个更简单的构造入口。
        // 调用方只需传 4 个"原始"参数（path、docUuid、userId、tags）；
        // fileName、fileExtension、traceTags 三个字段由 of 自动推导，无需调用方操心。
        // 这就是静态工厂方法的价值：隐藏构造细节，保证对象创建时的一致性和完整性。
        // 你前面代码里的 EtlContext.of(path, docUuid, userId, tags) 就是调它。
        public static EtlContext of(Path path, String docUuid, String userId, List<String> tags) {
            String fileName = path.getFileName().toString();
            String fileExt = extension(fileName);
            Map<String, Object> traceTags = etlTraceTags(path, docUuid, userId, tags);
            traceTags.put("document.file_ext", fileExt);
            return new EtlContext(
                    path,
                    docUuid,
                    userId,
                    tags,
                    fileName,
                    fileExt,
                    traceTags
            );
        }

        // 两个 tagsWith 方法：不可变对象的"扩展"技巧

        Map<String, Object> tagsWith(String key, Object value) {
            Map<String, Object> merged = new LinkedHashMap<>(traceTags); // 先复制一份
            merged.put(key, value); // 再改副本
            return merged; // 这是不可变对象的经典模式：不修改原对象，而是返回一个"加了新标签"的新 Map。
        }

        Map<String, Object> tagsWith(Map<String, Object> extra) {
            Map<String, Object> merged = new LinkedHashMap<>(traceTags); // 先复制一份
            merged.putAll(extra); // 再改副本
            return merged;// 这是不可变对象的经典模式：不修改原对象，而是返回一个"加了新标签"的新 Map。
        }
        /**
         * 为什么要这样做？
         * EtlContext 本身是 record，不可变，它的 traceTags 字段也不应该被外部随意改动——否则会污染整个流水线的追踪上下文。
         * 但流水线的不同阶段又需要往 trace 标签里塞不同的信息（比如切分阶段加 chunk_count，向量化阶段加 embedding_model）。
         * 于是提供 tagsWith(...)：给我一份带着新标签的副本，谁想加就生成自己的副本，互不干扰。
         * 用 LinkedHashMap 是为了保留插入顺序，让 trace 标签输出时顺序稳定、可读。
         */
    }

    /**
     * 类型流转图
     * transitionTo(...)                     Mono<Void>
     *         │  .then(...)
     *         ▼
     * Mono.fromCallable(strategy.readAndSplit)   Mono<ParentChildDocuments>   ← 阻塞，跑在 boundedElastic
     *         │  .flatMap(...)
     *         ▼
     * loadDocumentByDocUuid(docUuid)        Mono<StoredDocument>          ← 异步 DB 查询
     *         │  .map(enrichMetadata)
     *         ▼
     * enrichMetadata(...)                    ParentChildDocuments          ← 融合元数据
     *         │  .doOnNext(log)
     *         ▼
     * Mono<ParentChildDocuments>            ← 返回给上游 ingestionByPath
     * ETL 的第一阶段（Extract + 部分 Transform）
     * 这是整条 ETL 流水线的第一个 stage，对应前面讲的 Extract（抽取），同时做了部分 Transform（切分）。
     * 它负责：更新状态 → 解析并切分文件 → 加载数据库里的文档记录 → 丰富元数据 → 打日志
     * @param ctx 上下文对象
     * @return Mono<ChunkUtils.ParentChildDocuments> —— 一个包含父块（parentBlocks）和子块（childDocuments）的容器
     * 这是典型的 RAG 分块策略：
     * 父块（parent）：较大的语义单元（如整段、整节），用于给 LLM 提供完整上下文。
     * 子块（child）：较小的检索单元（如句子、短段），用于向量检索。
     * 检索时用子块命中，返回时用父块做上下文，兼顾精度和完整性。
     */
    private Mono<ChunkUtils.ParentChildDocuments> readAndSplit(EtlContext ctx) {
        /**
         * 这里用了一个很漂亮的分层：
         * 外层：tracingSupport.traceMono(...) 负责可观测性（链路追踪）。
         * 内层：纯粹的 ETL 业务逻辑。
         */
        return tracingSupport.traceMono("etl.read_split", // span 名字
                ctx.tagsWith(Map.of("etl.stage", "read_split")),  // span 标签  这正好用上了上一节讲的 tagsWith 方法：在副本上追加 etl.stage 标签，不污染原始 traceTags。这样在追踪系统里能看到这一 span 属于哪个阶段。
                /* ─────────── 被包裹的 pipeline ─────────── */
                // 把文档状态从（通常是 PENDING/UPLOADED）转换为 SPLITTING，并带上说明文案
                // 返回 Mono<Void>，表示"状态更新完成"这个信号
                // 这是一种状态机驱动的设计：前端或用户能实时看到文档"正在切分"。
                // 为什么要先更新状态再干活？
                // 因为下游可能有 UI 轮询或 WebSocket 推送，状态是用户可见的进度。先置 SPLITTING 再开始切分，用户就知道流程启动了。
                etlStatusManager.transitionTo(ctx.docUuid(), ctx.userId, DocumentStatus.SPLITTING, "Reading and splitting...")  // ① 状态机：置为 SPLITTING
                        // 忽略上一步（状态更新）的返回值，等它完成后接着执行这个 Mono
                        .then(Mono.fromCallable(() -> {  // ② 真正读文件 + 切分 ，把阻塞式代码（读文件、解析、切分都是 IO + CPU 密集）包装成响应式 Mono。
                            // 策略模式（Strategy Pattern）： 根据文件扩展名（pdf、md、docx……）从工厂里取对应的解析策略。
                            // 这就是「工厂 + 策略」组合：不同文件类型用不同解析器，开闭原则——新增文件类型只需加一个策略实现，不改这里。
                            FileParseStrategy strategy = fileParseStrategyFactory
                                    .getFileParseStrategyOrNull(ctx.fileExtension());
                            if (strategy != null) {
                                return strategy.readAndSplit(ctx.fileExtension(), ctx);
                            }
                            return genericReadAndSplit(ctx);
                        }).subscribeOn(Schedulers.boundedElastic()))
                        .flatMap(parentChildDocs -> loadDocumentByDocUuid(ctx.docUuid()) // ③ 从库加载文档
                                .map(storedDoc -> enrichMetadata(ctx, parentChildDocs, storedDoc))) // ④ 丰富元数据
                        .doOnNext(parentChildDocs -> log.info( // ⑤ 成功日志
                                "Read and split: docUuid={}, parentBlocks={}, childChunks={}",
                                ctx.docUuid(),
                                parentChildDocs.parentBlocks().size(),
                                parentChildDocs.childDocuments().size())));
    }

    /**
     * Tika 通用解析兜底：当文件类型无专用解析策略时（如 txt, rtf），走固定窗口分块
     * 这是 readAndSplit 里的兜底分支：当 fileParseStrategyFactory 找不到匹配特定文件扩展名的策略时，就走这里。
     * 它用 Apache Tika 做通用文本抽取，再切分成父子块。
     * @param ctx 上下文
     * @return 而是同步的 ParentChildDocuments
     */
    private ChunkUtils.ParentChildDocuments genericReadAndSplit(EtlContext ctx) {
        // Apache Tika 是一个强大的内容检测 + 文本抽取库，能从 PDF、Word、Excel、PPT、HTML、纯文本等上千种格式里提取纯文本。
        // TikaDocumentReader 是 Spring AI 提供的封装，构造参数是资源 URI（这里用 path.toUri().toString() 把文件路径转成 file:///... 形式）。
        // Spring AI 的 Document 通常包含 text 内容 + metadata（如来源、页数等
        // 返回 List 是因为有些格式会解析出多个文档（比如多页 PDF 可能按页分）
        List<Document> docs = new TikaDocumentReader(ctx.path().toUri().toString()).get(); // 注意：这里有个潜在资源泄漏点——TikaDocumentReader 是否需要在用完后 close()？取决于其实现。若它持有输入流，长时间运行可能出问题。
        // 清洗文档
        // 去掉不可见字符、乱码、控制字符；
        // 压缩连续空白、去掉页眉页脚；
        // 过滤空文本或纯噪声的 Document；
        // 统一换行符、编码规范化。
        docs = sanitizeDocuments(ctx.path(), docs); // 返回新的列表（函数式风格，不原地修改），赋回 docs
        if (docs.isEmpty()) {
            throw new IllegalStateException("No usable text extracted: " + ctx.path());
        }
        // 构建父子块 切分逻辑：把清洗后的文本按固定窗口切分成父块 / 子块结构。
        // 名字里的 "FixedWindow" 暗示切分策略是按固定长度/滑动窗口，而不是按语义（如按标题、段落）或递归字符切分。
        // 典型的 fixed-window 策略：
        //父块：较大的窗口（如 2000 字符）；
        //子块：从父块内再按更小窗口（如 400 字符）切，带重叠（overlap）；
        //每个子块记录它属于哪个父块，形成父子映射。
        return buildFixedWindowParentChildDocuments(ctx, docs); // 传入 ctx 是为了给每个块附上文档级元数据（docUuid、userId、fileName、tags 等）
    }

    /**
     * 固定窗口的父子块切分
     * 负责把清洗后的文本按固定窗口切成「父块 → 子块」的两级结构，并为每块生成可追溯的 ID 和元数据。它是整条 ETL 里Transform（转换）的核心环节。
     * RAG 里经典的 Parent-Child 分块：检索用子块（精准），喂给 LLM 用父块（完整上下文）
     *
     * joinedText（整个文档合并后的文本）
     *         │
     *         ▼  splitByFixedWindow(parentBlockSize, parentBlockOverlap)
     *    ┌─────────┬─────────┬─────────┐
     *    │ piece 1 │ piece 2 │ piece 3 │  ... （父块，带 overlap）
     *    └────┬────┴────┬────┴────┬────┘
     *         │         │         │
     *         ▼         ▼         ▼   textSplitter（递归切成 ~200 字）
     *     子块1..n   子块n+1..m  子块m+1..k
     *         │         │         │
     *         ▼         ▼         ▼
     *    parentBlocks = [KnowledgeParentBlock, ...]
     *    childDocuments = [Document(带 parent_block_id/child_index/evidence_id), ...]
     * @param ctx 上下文
     * @param docs
     * @return 返回父块列表 + 子块列表的容器，交给上游 readAndSplit 继续 enrichMetadata。
     */
    private ChunkUtils.ParentChildDocuments buildFixedWindowParentChildDocuments(EtlContext ctx, List<Document> docs) {
        // 从第一个 Document 的 metadata 复制一份作为基础元数据。
        // 这里假设所有 Document（多页）的元数据大体一致（来源、标题、语言等），取第一个即可
        // 用 LinkedHashMap 保证插入顺序稳定，方便后续输出/调试
        // 后续每个父块都会基于这份 base 复制，再叠加自己的字段
        Map<String, Object> baseMetadata = new LinkedHashMap<>(docs.get(0).getMetadata());
        // 合并所有文档的文本,为什么要合并？ 因为接下来要按固定窗口切分，若分页切会切断跨页语义。先合并成整体，再统一切分，能保持连续性
        // 注意:大文件场景可考虑 StringBuilder 或 Collectors.joining
        String joinedText = docs.stream()
                .map(Document::getText) // 取文本
                .filter(Objects::nonNull) // 去掉 null
                .map(String::trim) // 去首尾空白
                .filter(text -> !text.isBlank()) // 去掉纯空白
                .reduce((left, right) -> left + "\n\n" + right) // 用空行拼接 用 "\n\n"（双换行）拼接，保留段落感；
                .orElse(""); // 全空则 "" , 兜底，避免 Optional 空导致 NPE
        // 调用工具方法，按固定窗口把整体文本切成若干 pieces
        // parentBlockSize：父块大小（字符数，如 2000）；
        // arentBlockOverlap：父块间重叠（如 200），防止切在关键语义中间导致信息丢失
        // 每个 piece 就是一个父块的文本。
        List<String> pieces = ChunkUtils.splitByFixedWindow(joinedText, parentBlockSize, parentBlockOverlap);
        List<net.topikachu.rag.business.document.entity.KnowledgeParentBlock> parentBlocks = new ArrayList<>();
        List<Document> childDocuments = new ArrayList<>();
        int childIndex = 1;

        // 遍历父块，构建父块 + 子块
        for (int i = 0; i < pieces.size(); i++) {
            int parentIndex = i + 1;  // 父块序号（从 1 开始）
            Map<String, Object> metadata = new LinkedHashMap<>(baseMetadata); // 复制 base
            metadata.put("parent_index", parentIndex); // 第几个父块；
            // 分块策略、字段含义可能随版本演进。存版本号可以让旧数据保留旧语义、新数据用新语义，将来重建索引或迁移时能区分处理。
            metadata.put("chunk_schema_version", KnowledgeParentBlockService.CHUNK_SCHEMA_VERSION); // 分块 schema 版本号（关键！见下文）；
            metadata.put("parent_block_id", ChunkUtils.buildParentBlockId( // 父块唯一 ID，由 docUuid + 文本 + index 生成（buildParentBlockId）
                    ctx.docUuid(), pieces.get(i), parentIndex));

            // 包装成 Document，再转成业务实体 KnowledgeParentBlock 加入 parentBlocks。
            Document parentDocument = new Document(pieces.get(i), metadata);
            parentBlocks.add(ChunkUtils.toKnowledgeParentBlock(parentDocument));

            // 父块 → 子块：递归切分为 200 字片段，子块继承父块的 metadata
            // 这是父子结构的关键：每个父块再被 textSplitter 切成更小的子块。
            // 子块继承父块的 metadata（因为 parentDocument 的 metadata 已含 base + 父块字段），再叠加子块专属字段：
            List<Document> children = textSplitter.apply(List.of(parentDocument));
            for (Document child : children) {
                // 这里直接改了 child 的 metadata（而非复制）。如果 child 的 metadata 与外部共享，可能产生意外副作用。
                // 更安全的做法是 new LinkedHashMap<>(child.getMetadata())。
                Map<String, Object> childMeta = child.getMetadata();
                childMeta.put("chunk_schema_version", KnowledgeParentBlockService.CHUNK_SCHEMA_VERSION); // 同样带上 schema 版本
                childMeta.put("parent_block_id", metadata.get("parent_block_id")); // 关联到父块，检索命中小块 → 找到大块
                childMeta.put("parent_index", parentIndex); // 子块属于第几个父块
                childMeta.put("child_index", childIndex); // 全局子块序号（跨父块累加，从 1 开始）
                childMeta.put("evidence_id", ChunkUtils.buildEvidenceId(ctx.docUuid(), child, childIndex)); // 子块的唯一证据 ID（用于引用溯源）
                childMeta.put("source_location", "片段" + parentIndex); // 人类可读的位置，如"片段3"
                childDocuments.add(child);
                childIndex++;
            }
        }

        return new ChunkUtils.ParentChildDocuments(parentBlocks, childDocuments);
    }

    /**
     * readAndSplit(ctx)
     *     │
     *     ├─ transitionTo(SPLITTING)                       状态：切分中
     *     │
     *     ├─ strategy / genericReadAndSplit                 抽取 + 切分
     *     │       └─ buildFixedWindowParentChildDocuments   父块 + 子块（带切分元数据）
     *     │
     *     ├─ loadDocumentByDocUuid(docUuid)                 回查数据库文档记录
     *     │       │
     *     │       └─ .map(storedDoc -> enrichMetadata(...))  ← 本方法：注入业务元数据
     *     │
     *     └─ doOnNext(log)
     * 分块结果注入完整元数据
     * 这是 readAndSplit 里切分之后的收尾步骤，把「ETL 上下文 + 分块结果 + 数据库文档记录」三方信息融合，
     * 给每个父块和子块补上业务元数据（归属、权限、标签、版本等）。它是写入向量库前的最后一次"打标"。
     * @param etlContext 上下文：docUuid、fileName、tags
     * @param docs 切分产物：父块 + 子块
     * @param storedDocument 数据库记录
     * @return
     */
    private ChunkUtils.ParentChildDocuments enrichMetadata(EtlContext etlContext,
                                                           ChunkUtils.ParentChildDocuments docs,
                                                           net.topikachu.rag.business.document.entity.Document storedDocument) {
        List<String> effectiveTags = etlContext.tags;
        if (storedDocument != null && storedDocument.getTags() != null) {
            effectiveTags = storedDocument.getTags();
        }

        // 给父块打标
        // 是业务实体（KnowledgeParentBlock），有明确的 setter，逐个字段赋值。
        for (KnowledgeParentBlock parentBlock : docs.parentBlocks()) {
            parentBlock.setDocUuid(etlContext.docUuid()); // 归属文档
            parentBlock.setFileName(etlContext.fileName()); // 来源文件名（展示用）
            parentBlock.setSpaceCode(resolveSpaceCode(storedDocument)); // 知识空间编码（多租户/分类隔离）
            parentBlock.setTags(effectiveTags == null ? List.of() : List.copyOf(effectiveTags)); // 业务标签，List.copyOf(effectiveTags)：生成不可变副本，防止外部后续修改 effectiveTags 影响已入库的块。这是防御性拷贝
            parentBlock.setAclVersion(storedDocument == null ? 1 : storedDocument.getAclVersion()); // 权限版本号（访问控制）
            parentBlock.setChunkSchemaVersion(KnowledgeParentBlockService.CHUNK_SCHEMA_VERSION); // 分块 schema 版本
        }

        // 给子块打标（关键差异）
        // 子块：是 Spring AI 的 Document，元数据是 Map<String, Object>，没有强类型字段，所以用 Map 操作。
        for (Document child : docs.childDocuments()) {
            // 一个专门的构建器，把三方信息（storedDocument、docUuid、fileName、tags、原有 metadata、aclVersion）统一组装成一份全新的元数据 Map
            Map<String, Object> rebuiltMetadata = metadataBuilder.build(
                    storedDocument,
                    etlContext.docUuid,
                    etlContext.fileName(),
                    effectiveTags,
                    child.getMetadata(),
                    storedDocument == null ? 1 : storedDocument.getAclVersion());
            child.getMetadata().clear(); // 清空原 metadata。
            child.getMetadata().putAll(rebuiltMetadata); // 填入重建后的。
        }
        return docs;
    }

    // 未指定 spaceCode 时默认归入 "public"，避免向量数据无法按空间检索
    private String resolveSpaceCode(net.topikachu.rag.business.document.entity.Document storedDocument) {
        if (storedDocument == null || storedDocument.getSpaceCode() == null || storedDocument.getSpaceCode().isBlank()) {
            return "public";
        }
        return storedDocument.getSpaceCode().trim();
    }

    /**
     * writeParentBlocksAndVectors — ETL 的第二阶段（Load）
     * 这是整条 ETL 流水线的第二个 stage，对应前面讲的 Load（加载）。它负责：更新状态 → 写 MySQL 父块 → 写 Milvus 子向量，顺序严格不能颠倒
     * @param ctx 上下文 ctx
     * @param parentChildDocs 上一阶段切分好的 parentChildDocs（父块 + 子块）
     * @return Mono<Void> —— 只关心"完成/失败"信号，不产生数据
     */
    private Mono<Void> writeParentBlocksAndVectors(EtlContext ctx, ChunkUtils.ParentChildDocuments parentChildDocs) {
        // 构建 trace 标签, 生成带新标签的副本，不污染原 traceTags
        // 为什么要带上数量？
        // 在链路追踪系统里，一眼能看到这次处理了多少块。如果数量异常（比如 0 或 10000），能快速定位问题。这是"可观测性数据即业务数据"的实践
        Map<String, Object> traceTags = ctx.tagsWith(Map.of(
                "etl.stage", "vectorize", // 标明当前阶段；
                "etl.parent_block_count", parentChildDocs.parentBlocks().size(), // 父块数量
                "etl.chunk_count", parentChildDocs.childDocuments().size() // 子块数量
        ));
        return tracingSupport.traceMono("etl.vectorize", traceTags,
                // 把文档状态从 SPLITTING 转为 VECTORIZING，附带文案"Writing to Vector Store..."。
                // 用户在前端能看到进度从"切分中"变成"向量化中"。
                etlStatusManager.transitionTo(ctx.docUuid, ctx.userId, DocumentStatus.VECTORIZING, "Writing to Vector Store...") // ① 状态机
                        // 先写 MySQL 父块，再写 Milvus 子向量：子块通过 parent_block_id 引用父块，顺序不能颠倒
                        // 调用 replaceForDocument：替换该文档的所有父块。
                        //方法名里的 "replace" 很关键——它不是"append"，而是先删后插或幂等替换。这保证了：
                        //重跑安全：文档重新处理时不会产生重复父块；
                        //一致性：一次处理对应一份完整的父块集合
                        //为什么父块要写 MySQL？
                        //父块是较大的文本单元，用于给 LLM 提供上下文，不需要向量检索（检索靠子块）。
                        //存在关系库里便于按 parent_block_id 快速回查、做业务查询、管理。
                        .then(parentBlockService.replaceForDocument(ctx.docUuid(), parentChildDocs.parentBlocks()))
                        .then(Mono.defer(() -> { // Mono.defer 把内部的逻辑推迟到订阅时才执行
                            // 空文档（如全图片 PDF 无文字）跳过 Milvus 写入，否则会报参数错误
                            if (!parentChildDocs.childDocuments().isEmpty()) {
                                return hybridVectorWriter.write(parentChildDocs.childDocuments());
                            }
                            return Mono.empty();
                        })));
    }

    /**
     * genericReadAndSplit(ctx)
     *     ├─ new TikaDocumentReader(...).get()          抽取原始文本
     *     ├─ sanitizeDocuments(path, docs)              ← 本方法：清洗 + 过滤
     *     │       └─ 逐文档：sanitize → 日志 → 丢弃空 → 构造新 Document
     *     ├─ if (docs.isEmpty()) throw ...              全空则失败
     *     └─ buildFixedWindowParentChildDocuments(...)  切分
     * 文本清洗与低质量过滤
     * 这是 genericReadAndSplit 里 Tika 抽取之后的清洗步骤，负责对每个抽取出的 Document 做文本净化、质量检测、空内容丢弃，并保留元数据。
     * 它是 ETL 里 Transform（转换） 的细粒度体现
     * @param path 文件路径
     * @param docs  抽取的原始文档列表
     * @return 清洗后的新列表（可能比输入少，因为空文档被丢弃）
     */
    private List<Document> sanitizeDocuments(Path path, List<Document> docs) {
        List<Document> sanitizedDocs = new ArrayList<>();
        for (Document doc : docs) {
            // ① 调用 TextSanitizer 清洗，拿到结构化结果
            TextSanitizer.SanitizationResult result = TextSanitizer.sanitize(doc.getText());
            // ② 确定日志用的来源标签（页码 or 通用）
            Object pageNumber = doc.getMetadata().get("page_number");
            // 目的：让日志能精确定位是哪一页/哪一段出了问题，而不是笼统地说"这个文件有问题"。
            String sourceLabel = pageNumber != null ? "page " + pageNumber : "document chunk";
            // ③ 若被修改 → INFO 日志
            // ④ 若低质量 → WARN 日志
            // ⑤ 若实际为空 → 丢弃（continue）
            // ⑥ 否则 → 用清洗后文本 + 复制元数据构造新 Document

            // 只要清洗改动了文本，就打 INFO 日志。
            // 记录：移除了多少字符、是否规范化空白、原始长度、清洗后长度。
            // 用途：可观测性——知道清洗器实际做了什么，便于调参（比如发现某类文件总被移除大量字符）。
            if (result.wasModified()) { // 文本被清洗改动过
                log.info("Sanitized {} from {}: removedChars={}, normalizedWhitespace={}, originalLength={}, sanitizedLength={}",
                        sourceLabel, path.getFileName(), result.removedChars(), result.normalizedWhitespace(),
                        result.originalLength(), result.text().length());
            }

            // 如果抽取质量低（比如移除比例过高、有效码点太少），打 WARN。
            // 这不丢弃文档，只是告警——留给运维/开发者观察。
            // 典型场景：扫描版 PDF 经 OCR 后全是乱码、加密文档抽出乱字符、损坏文件。
            // 用 meaningfulCodePoints（有效 Unicode 码点）而非 length 判断，能正确识别emoji、中文、组合字符等的"真实信息量"。
            if (result.isLowQualityExtraction()) { // 抽取质量差（乱码多等）
                log.warn("Low-quality {} extraction for {}: removedRatio={}, meaningfulCodePoints={}",
                        sourceLabel, path.getFileName(), result.removedRatioPercent(), result.meaningfulCodePoints());
            }

            // "effectively empty" 比 isEmpty() 更严格——清洗后可能剩一些空白/不可见字符，语义上仍是空。
            // 满足则 continue 跳过，不加入结果列表。
            // 打 WARN 而非 INFO，因为"有内容却清洗后为空"是值得注意的现象。
            // 这一步保证了后续 buildFixedWindowParentChildDocuments 拿到的都是有实际内容的文档。
            if (result.isEffectivelyEmpty()) { // 清洗后实际为空
                log.warn("Dropping {} from {} after sanitization", sourceLabel, path.getFileName());
                continue;
            }

            // 构造新 Document,new HashMap<>(doc.getMetadata()) 是关键：防御性拷贝
            // 不复用原 Map，避免后续修改污染原文档；
            // 不复用原 Document，保证下游拿到的是"干净的"对象。
            sanitizedDocs.add(new Document(result.text(), new HashMap<>(doc.getMetadata())));
        }
        return sanitizedDocs;
    }

    /**
     * 失败兜底与补偿清理
     * 补偿清理：先删父块（MySQL），再标记 FAILED；即使删块失败也会继续更新状态，避免卡在中间态
     * 这是整条 ETL 流水线的错误处理终点，由 ingestionByPath 的 .onErrorResume(e -> failAndCleanup(ctx, e)) 触发。
     * 它负责：记录错误 → 清理脏数据 → 更新失败状态 → 重新抛出异常。对应前面提到的 Saga 补偿模式。
     *
     * 整条 ETL 是跨 MySQL + Milvus 的分布式操作，没有分布式事务。失败时靠补偿清理实现最终一致性：
     * 正常流程（正向）：
     *   transitionTo(SPLITTING) → 切分 → enrichMetadata
     *   → transitionTo(VECTORIZING) → 写 MySQL 父块 → 写 Milvus 子向量
     *   → completeStatus(成功)
     *
     * 失败流程（补偿）：
     *   failAndCleanup：
     *     删 MySQL 父块  → transitionToFailed → 抛出
     * @param ctx 上下文 ctx
     * @param error  触发的异常 error
     * @return 它返回错误信号（重新抛异常），而不是"吞掉错误后正常完成"
     */
    private Mono<Void> failAndCleanup(EtlContext ctx, Throwable error) {
        log.error("Error during ingestionByPath: {}", ctx.path, error);

        Map<String, Object> extra = new LinkedHashMap<>();
        extra.put("etl.stage", "failed"); // 标明失败阶段
        extra.put("error.type", error == null ? "" : error.getClass().getSimpleName()); // 便于按类型聚合分析
        extra.put("error.message", extractUserMessage(error)); // 用户可读的错误信息

        // 顺序很讲究：先清理 → 再标记失败 → 最后把异常继续往下传。
        // tracingSupport.traceMono("etl.fail_cleanup", ctx.tagsWith(extra), ...) 给整个失败清理流程包一个 span，名叫 "etl.fail_cleanup"。
        return tracingSupport.traceMono("etl.fail_cleanup", ctx.tagsWith(extra), // tracingSupport.traceMono("etl.fail_cleanup", ctx.tagsWith(extra), ...)
                parentBlockService.deleteByDocUuid(ctx.docUuid()) // ① 清理父块
                        .onErrorResume(cleanupErr -> {
                            log.warn("Cleanup failed", cleanupErr);
                            return Mono.empty();   // 清理失败不阻断状态更新
                        })
                        .then(etlStatusManager.transitionToFailed(ctx.docUuid(), ctx.userId(), error)) // ② 置失败状态
                        .then(Mono.error(error))); // ③ 重新抛出
    }

    /**
     * ingestionByPath
     *     │
     *     ├─ readAndSplit(ctx)                        状态: → SPLITTING
     *     ├─ writeParentBlocksAndVectors(ctx, docs)   状态: → VECTORIZING
     *     │                                              写 MySQL 父块 + Milvus 向量
     *     ├─ completeStatus(ctx)                      ← 本方法：状态 → COMPLETED
     *     │
     *     ├─ doOnSuccess(log "finished")
     *     └─ onErrorResume → failAndCleanup           状态 → FAILED + 清理
     *
     *  把前面所有阶段串起来看，DocumentStatus 的流转：
     * PENDING / UPLOADED
     *         │  transitionTo(SPLITTING)
     *         ▼
     *     SPLITTING ──────────────┐
     *         │  transitionTo(VECTORIZING)
     *         ▼                   │
     *    VECTORIZING ─────────────┤
     *         │                   │
     *         ├── 成功 ──────────►│
     *         │   transitionToCompleted()
     *         ▼                   │
     *     COMPLETED               │
     *                             │
     *         ┌── 失败（任意阶段）─┘
     *         │   transitionToFailed()
     *         ▼
     *      FAILED
     * @param ctx 上下文 ctx。
     * @return  只关心"完成/失败"信号。
     */
    private Mono<Void> completeStatus(EtlContext ctx) {
        // 目的：让"成功收尾"这一步在链路追踪里独立可见，能看到它的耗时和结果。
        return tracingSupport.traceMono("etl.finish", // span 名字
                ctx.tagsWith("etl.stage", "finish"), // span 标签
                etlStatusManager.transitionToCompleted(ctx.docUuid(), ctx.userId()));  // 被包裹的业务
    }

    /**
     * 构建链路追踪的基础标签
     * 初始（etlTraceTags）:
     *   document.doc_uuid, document.file_name, document.user_id, document.tags
     *         │
     *         ├─ of() 追加: document.file_ext
     *         │
     *         ├─ readAndSplit 追加: etl.stage=read_split
     *         ├─ writeParentBlocksAndVectors 追加: etl.stage=vectorize, etl.parent_block_count, etl.chunk_count
     *         ├─ completeStatus 追加: etl.stage=finish
     *         └─ failAndCleanup 追加: etl.stage=failed, error.type, error.message
     * 这是 EtlContext.of(...) 里调用的私有工具方法，负责从原始参数组装一份初始的追踪标签 Map。它是整个 ETL 可观测性的"起点"。
     * @param path 路径
     * @param docUuid 文档 UUID
     * @param userId 用户 ID
     * @param tags 标签
     * @return 一个 LinkedHashMap<String, Object>
     */
    private static Map<String, Object> etlTraceTags(Path path, String docUuid, String userId, List<String> tags) {
        // 为什么重要？标签最终会输出到追踪系统或日志里，稳定的顺序让对比、diff、阅读都更方便。HashMap 的顺序在不同 JVM/版本下可能不同，会造成"看起来变来变去"的假象。
        // 统一 document. 前缀
        Map<String, Object> traceTags = new LinkedHashMap<>();
        traceTags.put("document.doc_uuid", docUuid);
        traceTags.put("document.file_name", path == null ? "" : path.getFileName());
        traceTags.put("document.user_id", userId); // 用户 ID
        traceTags.put("document.tags", tags == null ? "" : String.join(",", tags)); // 标签列表
        return traceTags;
    }

    private static String extension(String fileName) {
        int idx = fileName.lastIndexOf('.');
        if (idx < 0 || idx == fileName.length() - 1) {
            return "";
        }
        return fileName.substring(idx + 1).toLowerCase(Locale.ROOT);
    }

    private Mono<net.topikachu.rag.business.document.entity.Document> loadDocumentByDocUuid(String docUuid) {
        return Mono.fromCallable(() -> loadDocumentByDocUuidBlocking(docUuid))
                .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     *  按 UUID 查文档（阻塞版）
     *  这是一个非常"朴素"的同步数据库查询方法：根据 docUuid 从 MySQL 里查出对应的文档记录。
     *  方法名里的 "Blocking" 是关键——它明确标注了这是一个阻塞操作，提醒调用方注意线程调度。
     * @param docUuid
     * @return
     */
    private net.topikachu.rag.business.document.entity.Document loadDocumentByDocUuidBlocking(String docUuid) {
        if (docUuid == null || docUuid.isBlank()) {
            return null;
        }
        return documentMapper.selectOne(
                Wrappers.<net.topikachu.rag.business.document.entity.Document>lambdaQuery()
                        .eq(net.topikachu.rag.business.document.entity.Document::getDocUuid, docUuid)
                        .last("LIMIT 1"));
    }

    private String extractUserMessage(Throwable e) {
        if (e == null)
            return "Unknown error";
        Throwable cause = e;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        String msg = cause.getMessage();
        if (msg == null || msg.isBlank()) {
            msg = cause.getClass().getSimpleName();
        }
        // 截断到 500 字符：数据库 status_message 字段长度有限，完整堆栈会撑爆列
        return msg.length() > 500 ? msg.substring(0, 500) : msg;
    }

    private String computeHash(Path path) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        try (InputStream in = Files.newInputStream(path);
             DigestInputStream dis = new DigestInputStream(in, md)) {
            byte[] buffer = new byte[8192];
            while (dis.read(buffer) != -1)
                ;
        }
        return HexFormat.of().formatHex(md.digest());
    }
}
