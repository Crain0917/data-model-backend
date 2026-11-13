package com.datamodel.common;

/**
 * 业务错误码，与《API契约-v1》§0.2 保持一致。
 */
public enum ErrorCode {

    SUCCESS(0, 200, "成功"),
    PARAM_ERROR(4000, 400, "参数错误"),
    DAG_INVALID(4001, 400, "DAG 校验失败"),
    NOT_FOUND(4002, 404, "资源不存在"),
    CONFLICT(4003, 409, "资源冲突"),
    CONNECTION_FAILED(4004, 400, "数据源连通失败"),
    RUN_CONFLICT(4005, 409, "执行中冲突"),
    UPSTREAM_NOT_READY(4006, 409, "上游未就绪"),
    INTERNAL_ERROR(5000, 500, "内部错误"),
    DORIS_EXEC_FAILED(5001, 500, "Doris 执行失败"),
    DRIVER_ERROR(5002, 500, "数据源驱动/连接错误");

    private final int code;
    private final int httpStatus;
    private final String defaultMessage;

    ErrorCode(int code, int httpStatus, String defaultMessage) {
        this.code = code;
        this.httpStatus = httpStatus;
        this.defaultMessage = defaultMessage;
    }

    public int getCode() {
        return code;
    }

    public int getHttpStatus() {
        return httpStatus;
    }

    public String getDefaultMessage() {
        return defaultMessage;
    }
}
