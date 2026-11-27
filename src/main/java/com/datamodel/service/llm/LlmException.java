package com.datamodel.service.llm;

/**
 * 大模型调用/解析失败。message 会回显给前端与 run_log，绝不能包含 Key 或请求头。
 */
public class LlmException extends RuntimeException {

    private final String reason;
    private final Integer httpStatus;
    private final boolean retryable;

    public LlmException(String reason, String message) {
        this(reason, message, null, false);
    }

    public LlmException(String reason, String message, Integer httpStatus) {
        this(reason, message, httpStatus, false);
    }

    public LlmException(String reason, String message, Integer httpStatus, boolean retryable) {
        super(message);
        this.reason = reason;
        this.httpStatus = httpStatus;
        this.retryable = retryable;
    }

    public String getReason() {
        return reason;
    }

    public Integer getHttpStatus() {
        return httpStatus;
    }

    public boolean isRetryable() {
        return retryable;
    }
}
