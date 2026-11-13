package com.datamodel.dto;

public class TestConnectionResult {

    private boolean success;
    private long latencyMs;
    private String errorType;
    private String message;

    public static TestConnectionResult success(long latencyMs, String message) {
        TestConnectionResult r = new TestConnectionResult();
        r.success = true;
        r.latencyMs = latencyMs;
        r.message = message;
        return r;
    }

    public static TestConnectionResult failure(long latencyMs, String errorType, String message) {
        TestConnectionResult r = new TestConnectionResult();
        r.success = false;
        r.latencyMs = latencyMs;
        r.errorType = errorType;
        r.message = message;
        return r;
    }

    public boolean isSuccess() {
        return success;
    }

    public long getLatencyMs() {
        return latencyMs;
    }

    public String getErrorType() {
        return errorType;
    }

    public String getMessage() {
        return message;
    }
}
