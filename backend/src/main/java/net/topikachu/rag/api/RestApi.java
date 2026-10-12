package net.topikachu.rag.api;

import lombok.extern.slf4j.Slf4j;
import net.topikachu.rag.agent.AgentChatService;
import net.topikachu.rag.auth.CurrentUserContext;
import net.topikachu.rag.auth.CurrentUserContextService;
import net.topikachu.rag.auth.SearchScope;
import net.topikachu.rag.business.document.service.DocumentService;
import net.topikachu.rag.observability.TracingSupport;
import net.topikachu.rag.service.chat.ChatService;
import net.topikachu.rag.service.chat.SourceValidationException;
import net.topikachu.rag.service.etl.EtlPipeline;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import java.io.Serializable;
import java.security.Principal;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.StringUtils;

import static org.springframework.ai.reader.tika.TikaDocumentReader.METADATA_SOURCE;

@RestController()
@RequestMapping("/api/v1")
@Slf4j
@RequiredArgsConstructor
public class RestApi {

	private final ChatService chatService;
	private final AgentChatService agentChatService;
	private final TracingSupport tracingSupport;
	private final CurrentUserContextService currentUserContextService;
	private final DocumentService documentService;

	private final EtlPipeline etlPipeline;

	@Value("${rag.agent.enable:true}")
	private boolean agentEnabled;

	@Value("${rag.agent.default-mode:rag}")
	private String agentDefaultMode;

	/**
	 * 基于 Spring WebFlux 的流式聊天接口 用于 RAG（检索增强生成）场景。它支持两种模式：快速模式（普通 RAG 问答）和 Agent 模式，并通过 SSE 把结果逐步推送给前端。
	 * 三个可选请求头（Locust 压测相关）：X-Locust-Run-Id、X-Question-Id、X-Question-Bucket，用于压测追踪。
	 * @param chatRequest
	 * @param conversationId
	 * @param locustRunId
	 * @param questionId
	 * @param questionBucket
	 * @param principalMono 响应式获取当前登录用户
	 * @return 返回类型 Flux<ServerSentEvent<Object>>：一个可发送多个 SSE 事件的流。
	 * 这是一个带权限校验、支持 Agent/快速双模式、以 SSE 流式返回答案和引用来源、并对不同异常做用户友好兜底的 RAG 聊天接口。
	 */
	@PostMapping(path = "/chat", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
	@PreAuthorize("hasAnyRole('USER', 'ADMIN')")
	public Flux<ServerSentEvent<Object>> chat(@RequestBody ChatRequest chatRequest,
			@RequestParam() String conversationId,
			@RequestHeader(value = "X-Locust-Run-Id", required = false) String locustRunId,
			@RequestHeader(value = "X-Question-Id", required = false) String questionId,
			@RequestHeader(value = "X-Question-Bucket", required = false) String questionBucket,
			Mono<Principal> principalMono) {
		//  解析用户上下文
		return principalMono.flatMapMany(principal -> {
			// 根据用户名解析出 CurrentUserContext
			CurrentUserContext currentUserContext = currentUserContextService.resolveByUsername(principal.getName());
			// 构造 conversationKey：确保会话 ID 带用户名前缀，避免不同用户会话冲突
			var conversationKey = conversationId.startsWith(principal.getName() + ":")
					? conversationId
					: String.format("%s:%s", principal.getName(), conversationId);

			// 解析搜索范围和模式 SearchScope：请求指定的知识空间和标签范围
			SearchScope requestedScope = new SearchScope(chatRequest.spaceCodes(), chatRequest.tags());

			String requestedMode = chatRequest.mode(); // 请求模式，缺省用 agentDefaultMode
			String mode = StringUtils.hasText(requestedMode) ? requestedMode : agentDefaultMode;

			boolean useAgent = agentEnabled && "agent".equalsIgnoreCase(mode); // 仅当系统开启 Agent 且模式为 agent 时为 true
			String msgId = StringUtils.hasText(chatRequest.msgId()) ? chatRequest.msgId() : ("msg-" + System.currentTimeMillis()); // 消息 ID，缺省用时间戳生成。
			// 调用 documentService 根据用户权限解析出实际可访问的搜索范围（可能对请求范围做权限过滤）
			return documentService.resolveEffectiveSearchScope(currentUserContext, requestedScope)
					.flatMapMany(searchScope -> {
						// 给当前链路打埋点/追踪标签，便于日志和监控
						tagCurrentChatTrace(principal.getName(), conversationId, conversationKey, mode, chatRequest.modelId(),
								locustRunId, questionId, questionBucket, searchScope);

						// 若系统未开启 Agent，但用户请求了 agent 模式，直接返回错误事件 + 结束事件。
						if (!agentEnabled && "agent".equalsIgnoreCase(mode)) {
							log.warn("Agent mode requested while disabled. conversationId={}, user={}", conversationId, principal.getName());
							return Flux.just(errorEvent(msgId, "当前系统未启用 Agent 模式，请切换到快速模式后重试。"),
									doneEvent(msgId));
						}

						// 分流：Agent 模式 vs 快速模式

						if (useAgent) {
							// Agent 模式：调用 agentChatService.streamEvents(...)，直接返回其 SSE 流。
							return agentChatService.streamEvents(
									chatRequest.userInput(),
									conversationKey,
									currentUserContext,
									searchScope,
									chatRequest.modelId(),
									msgId);
						}

						// 快速模式：调用 chatService.streamWithSources(...)，返回一个包含答案流和引用来源的响应对象，需要再加工。
						return chatService.streamWithSources(
										chatRequest.userInput(),
										conversationKey,
										currentUserContext,
										searchScope,
										chatRequest.modelId(),
										msgId)
					// 快速模式的结果加工
					.flatMapMany(response -> {
						List<Map<String, Object>> sourceMetadata = response.usedSources().stream()
								.map(source -> {
									// 把 usedSources()（引用的文档片段）转成 List<Map>，字段包括：evidenceId、doc_uuid、file_name、page_number、file_type。
									Map<String, Object> item = new LinkedHashMap<>();
									item.put("evidenceId", source.evidenceId());
									item.put("doc_uuid", source.docUuid());
									item.put("file_name", source.fileName());
									item.put("page_number", source.pageNumber());
									item.put("file_type", source.fileType());
									return item;
								})
								.collect(Collectors.toList());

						// 先发一个 event: sources 事件，携带这些引用元数据。
						ServerSentEvent<Object> sourceEvent = ServerSentEvent.builder()
								.event("sources")
								.data(sourceMetadata)
								.build();
						// 然后把答案流 response.flux() 的每个片段包装成 event: message 事件。
						Flux<ServerSentEvent<Object>> messageStream = response.flux()
								.map(content -> ServerSentEvent.builder()
										.event("message")
										.data((Object) content)
										.build());

						// 最后追加一个 done 事件，表示流结束  整体顺序：sources → 多个 message → done
						return Flux.concat(Flux.just(sourceEvent), messageStream, Flux.just(doneEvent(msgId)));
					})
					.onErrorResume(exp -> {
						// 默认错误文案
						String message = "系统繁忙，请稍后重试。";
						if (exp instanceof net.topikachu.rag.service.chat.RetrievalException retrievalException) {
							// RetrievalException  用其 getUserMessage()
							message = retrievalException.getUserMessage();
						} else if (exp instanceof SourceValidationException sourceValidationException) {
							// SourceValidationException → 用其 getUserMessage()
							message = sourceValidationException.getUserMessage();
						} else if (exp instanceof ResourceAccessException) {
							message = "模型服务响应超时，请稍后重试或切换模型。";
						}
						if (exp instanceof SourceValidationException
								|| exp instanceof net.topikachu.rag.service.chat.RetrievalException) {
							log.warn("Chat request failed with user-facing error: {}", message);
						} else {
							log.error("Error in chat", exp);
						}
						ServerSentEvent<Object> fallbackEvent = ServerSentEvent.builder()
								.event("message")
								.data((Object) message)
								.build();
						if (exp instanceof SourceValidationException) {
							return Flux.just(fallbackEvent, doneEvent(msgId));
						}
						return Flux.just(errorEvent(msgId, message), fallbackEvent, doneEvent(msgId));
					});
					})
					// 如果 resolveEffectiveSearchScope 失败，返回 error + done
					.onErrorResume(exp -> Flux.just(
							// 默认文案："请先选择知识空间。"
							errorEvent(msgId, exp.getMessage() == null ? "请先选择知识空间。" : exp.getMessage()),
							doneEvent(msgId)));
		});
	}

	@PostMapping(path = "/index", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
	@PreAuthorize("hasRole('ADMIN')")
	public Flux<String> index() {
		return etlPipeline.ingestionFlux()
				.map(document -> (String) document.getMetadata().get(METADATA_SOURCE));
	}

	record ChatRequest(String userInput, List<String> tags, List<String> spaceCodes, String modelId, String mode,
			String msgId) implements Serializable {
	}

	private ServerSentEvent<Object> errorEvent(String msgId, String message) {
		return ServerSentEvent.builder()
				.event("error")
				.data((Object) Map.of("msgId", msgId, "message", message))
				.build();
	}

	private ServerSentEvent<Object> doneEvent(String msgId) {
		return ServerSentEvent.builder()
				.event("done")
				.data((Object) Map.of("msgId", msgId))
				.build();
	}

	private void tagCurrentChatTrace(String username,
			String conversationId,
			String conversationKey,
			String mode,
			String modelId,
			String locustRunId,
			String questionId,
			String questionBucket,
			SearchScope searchScope) {
		Map<String, Object> tags = new LinkedHashMap<>();
		tags.put("langfuse.user.id", username);
		tags.put("langfuse.session.id", conversationKey);
		tags.put("conversation.id", conversationId);
		tags.put("chat.mode", mode);
		tags.put("chat.model_id", modelId);
		tags.put("chat.requested_spaces", searchScope == null ? "" : String.join(",", searchScope.requestedSpaceCodes()));
		tags.put("chat.requested_tags", searchScope == null ? "" : String.join(",", searchScope.requestedTags()));
		tags.put("locust.run_id", locustRunId);
		tags.put("question.id", questionId);
		tags.put("question.bucket", questionBucket);
		tracingSupport.tagCurrent(tags);
	}
}
