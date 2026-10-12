package net.topikachu.rag.api;

import lombok.RequiredArgsConstructor;
import net.topikachu.rag.common.AjaxResult;
import net.topikachu.rag.service.SysUserService;
import net.topikachu.rag.service.TokenService;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.security.Principal;
import java.time.Instant;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/auth")
@RequiredArgsConstructor
public class AuthController {

    private final TokenService tokenService;
    private final SysUserService sysUserService;
    private final JwtDecoder jwtDecoder;

    /**
     * 用户登录
     * @param body
     * @return
     *
     * Mono 是 Reactor 的响应式类型，表示"将来会返回 0 或 1 个结果"，是 WebFlux 的异步返回方式（对应传统 MVC 的 AjaxResult）
     */
    @PostMapping("/login")
    public Mono<AjaxResult> login(@RequestBody Map<String, String> body) {
        String username = body.get("username");
        String password = body.get("password");

        if (!StringUtils.hasText(username) || !StringUtils.hasText(password)) {
            // 立即返回一个包装好的错误结果（Mono.just(...) 把普通对象包成 Mono）
            return Mono.just(AjaxResult.error(400, "Username and password are required"));
        }
        // sysUserService 是阻塞 JDBC 调用，必须 offload 到 boundedElastic，避免阻塞 Netty 事件循环
        // 响应式框架里跑阻塞代码，必须 offload 到专用线程池。
        return Mono.fromCallable(() -> sysUserService.login(username, password)) // 把 sysUserService.login(...) 这个同步调用包装成惰性执行的 Mono（订阅时才执行）
                .subscribeOn(Schedulers.boundedElastic())// 关键！把执行切换到 boundedElastic 线程池
                .map(AjaxResult::success) // 登录成功，把返回结果包装成成功响应
                .onErrorResume(e -> Mono.just(AjaxResult.error(401, e.getMessage()))); // 捕获登录过程中的异常（如密码错误），转成 401 错误响应
    }

    /**
     * 用户注册
     * @param body
     * @return
     */
    @PostMapping("/register")
    @PreAuthorize("hasRole('ADMIN')")
    public Mono<AjaxResult> register(@RequestBody Map<String, String> body) {
        String username = body.get("username");
        String password = body.get("password");

        if (!StringUtils.hasText(username) || !StringUtils.hasText(password)) {
            return Mono.just(AjaxResult.error(400, "Username and password are required"));
        }

        return Mono.fromRunnable(() -> sysUserService.register(username, password))
                .subscribeOn(Schedulers.boundedElastic())
                .thenReturn(AjaxResult.success("User registered successfully", null))
                .onErrorResume(e -> Mono.just(AjaxResult.error(400, e.getMessage())));
    }

    /**
     * 修改密码
     * @param body 请求体 JSON，取 username、old_password、new_password
     * @param principalMono 响应式注入的当前登录用户（WebFlux 的写法），Principal 就是认证主体，getName() 得到用户名
     * @return Mono<AjaxResult> 异步统一响应
     */
    @PostMapping("/change-password")
    @PreAuthorize("hasAnyRole('USER', 'ADMIN')") // @PreAuthorize 是在进入方法前由 AOP 拦截的，如果权限不足，方法体根本不会执行，会直接走异常处理
    public Mono<AjaxResult> changePassword(@RequestBody Map<String, String> body, Mono<Principal> principalMono) {
        // principalMono 是一个 Mono<Principal>
        // 若用户已登录，会发出一个 Principal
        // 若未登录（匿名/无主体），它是空的 Mono（不发出任何元素）
        // .switchIfEmpty 当 Mono 为空时，替换成一个错误流 Mono.error(...)。 即：没有 Principal → 抛 IllegalStateException("Unauthorized")。
        // .flatMap(principal -> {...})：拿到 Principal 后，进入真正的业务逻辑。flatMap 用于把"一个元素"映射成"另一个 Mono 流"并扁平化。
        // 这里用 switchIfEmpty + 外层 onErrorResume 处理未认证情况。
        return principalMono.switchIfEmpty(Mono.error(new IllegalStateException("Unauthorized")))
                .flatMap(principal -> {
                    String username = principal.getName(); // 当前登录用户名（权威来源）
                    String bodyUsername = body.get("username"); // 请求体里的 username（可选）
                    String oldPassword = body.get("old_password");
                    String newPassword = body.get("new_password");

                    // 这是一个防止越权改别人密码的保护：即使有人伪造 username，也不能改他人密码
                    if (StringUtils.hasText(bodyUsername) && !username.equals(bodyUsername)) {
                        return Mono.just(AjaxResult.error(403, "Username mismatch with current user"));
                    }

                    if (!StringUtils.hasText(oldPassword) || !StringUtils.hasText(newPassword)) {
                        return Mono.just(AjaxResult.error(400, "old_password and new_password are required"));
                    }

                    // 执行改密（阻塞调用 offload）
                    // Mono.fromRunnable(...) 把无返回值的同步方法 changePassword(...) 包装成 Mono（订阅时才执行）
                    // .subscribeOn(Schedulers.boundedElastic())  关键！ 切换到弹性线程池执行，因为 sysUserService.changePassword 是阻塞 JDBC 调用，不能占用 Netty 事件循环
                    // fromRunnable 发出的流没有值，
                    // thenReturn 在它成功完成后发出成功结果（忽略原值）
                    // 捕获改密过程中的异常（如旧密码错误），转成 400 错误响应
                    return Mono.fromRunnable(() -> sysUserService.changePassword(username, oldPassword, newPassword))
                            .subscribeOn(Schedulers.boundedElastic())
                            .thenReturn(AjaxResult.success("Password changed successfully", null))
                            .onErrorResume(e -> Mono.just(AjaxResult.error(400, e.getMessage())));
                })
                .onErrorResume(e -> Mono.just(AjaxResult.error(401, e.getMessage())));
    }

    /**
     *
     * @param principalMono
     * @return
     */
    @GetMapping("/me/preferences")
    @PreAuthorize("hasAnyRole('USER', 'ADMIN')")
    public Mono<AjaxResult> getPreferences(Mono<Principal> principalMono) {
        return principalMono.switchIfEmpty(Mono.error(new IllegalStateException("Unauthorized")))
                .flatMap(principal -> Mono.fromCallable(() -> {
                            var user = sysUserService.findByUsername(principal.getName());
                            if (user == null) {
                                throw new IllegalArgumentException("User not found");
                            }
                            return AjaxResult.success(Map.of(
                                    "defaultSpaceCode",
                                    user.getDefaultSpaceCode() == null ? "" : user.getDefaultSpaceCode()));
                        })
                        .subscribeOn(Schedulers.boundedElastic()))
                .onErrorResume(e -> Mono.just(AjaxResult.error(401, e.getMessage())));
    }

    /**
     *
     * @param body
     * @param principalMono
     * @return
     */
    @PutMapping("/me/preferences")
    @PreAuthorize("hasAnyRole('USER', 'ADMIN')")
    public Mono<AjaxResult> updatePreferences(@RequestBody Map<String, String> body, Mono<Principal> principalMono) {
        return principalMono.switchIfEmpty(Mono.error(new IllegalStateException("Unauthorized")))
                .flatMap(principal -> Mono.fromRunnable(() -> {
                            var user = sysUserService.findByUsername(principal.getName());
                            if (user == null) {
                                throw new IllegalArgumentException("User not found");
                            }
                            sysUserService.updateDefaultSpace(user.getId(), body.get("defaultSpaceCode"));
                        })
                        .subscribeOn(Schedulers.boundedElastic())
                        .thenReturn(AjaxResult.success("Preferences updated successfully", null)))
                .onErrorResume(e -> Mono.just(AjaxResult.error(400, e.getMessage())));
    }

    /**
     * 刷新token
     * @param authHeader 验证头信息
     * @param body 传入的json实体
     * @return
     */
    @PostMapping("/refresh")
    public Mono<AjaxResult> refresh(@RequestHeader(value = "Authorization", required = false) String authHeader,
            @RequestBody(required = false) Map<String, String> body) {
        // 双来源获取 refresh token：同时支持 Authorization header 和 request body，header 优先出于日志安全
        String refreshToken = null;
        if (StringUtils.hasText(authHeader) && authHeader.startsWith("Bearer ")) {
            refreshToken = authHeader.substring(7);
        }
        if (!StringUtils.hasText(refreshToken) && body != null) {
            refreshToken = body.get("refresh_token");
        }
        if (!StringUtils.hasText(refreshToken)) {
            return Mono.just(AjaxResult.error(400, "Refresh token is required"));
        }

        final String token = refreshToken;
        // 手动解码 JWT 而非依赖 Spring Security 过滤器：refresh token 的 claim 结构和过期规则与 access token 不同
        return Mono.fromCallable(() -> {
                    Jwt jwt = jwtDecoder.decode(token);
                    if (jwt.getExpiresAt() != null && jwt.getExpiresAt().isBefore(Instant.now())) {
                        throw new IllegalArgumentException("Refresh token expired");
                    }
                    if (!"refresh".equals(jwt.getClaim("type"))) {
                        throw new IllegalArgumentException("Invalid token type");
                    }
                    String username = jwt.getSubject();
                    var user = sysUserService.findByUsername(username);
                    if (user == null) {
                        throw new IllegalArgumentException("User not found");
                    }
                    return tokenService.generateTokens(user);
                })
                .subscribeOn(Schedulers.boundedElastic())
                .map(AjaxResult::success)
                .onErrorResume(e -> Mono.just(AjaxResult.error(401, "Invalid refresh token: " + e.getMessage())));
    }
}
