package com.datamodel.common;

public class ApiResponse<T> {

    private int code;
    private T data;
    private String msg;

    public ApiResponse() {
    }

    public ApiResponse(int code, T data, String msg) {
        this.code = code;
        this.data = data;
        this.msg = msg;
    }

    public static <T> ApiResponse<T> ok(T data) {
        return new ApiResponse<>(ErrorCode.SUCCESS.getCode(), data, "");
    }

    public static <T> ApiResponse<T> ok() {
        return new ApiResponse<>(ErrorCode.SUCCESS.getCode(), null, "");
    }

    public static <T> ApiResponse<T> fail(ErrorCode errorCode, String msg, T data) {
        return new ApiResponse<>(errorCode.getCode(), data, msg);
    }

    public static <T> ApiResponse<T> fail(int code, String msg, T data) {
        return new ApiResponse<>(code, data, msg);
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
