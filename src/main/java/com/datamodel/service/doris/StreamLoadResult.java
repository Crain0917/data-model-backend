package com.datamodel.service.doris;

public class StreamLoadResult {

    private final boolean success;
    private final long loadedRows;
    private final String status;
    private final String message;
    private final long loadTimeMs;

    public StreamLoadResult(boolean success, long loadedRows, String status, String message, long loadTimeMs) {
        this.success = success;
        this.loadedRows = loadedRows;
        this.status = status;
        this.message = message;
        this.loadTimeMs = loadTimeMs;
    }

    public boolean isSuccess() {
        return success;
    }

    public long getLoadedRows() {
        return loadedRows;
    }

    public String getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }

    public long getLoadTimeMs() {
        return loadTimeMs;
    }
}
