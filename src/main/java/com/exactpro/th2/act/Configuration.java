package com.exactpro.th2.act;

public class Configuration {
    private static final long DEFAULT_BATCH_SIZE_BYTES = 1048576;
    private static final int DEFAULT_RESPONSE_TIMEOUT = 10_000;
    private static final int DEFAULT_BATCH_SIZE_COUNT = 1000;
    private static final int DEFAULT_MAX_FLUSH_TIME = 1000;

    private int responseTimeout = DEFAULT_RESPONSE_TIMEOUT;
    private long maxBatchSizeBytes = DEFAULT_BATCH_SIZE_BYTES;
    private int maxBatchSizeCount = DEFAULT_BATCH_SIZE_COUNT;
    private int maxFlushTime = DEFAULT_MAX_FLUSH_TIME;

    public int getMaxBatchSizeCount() {
        return maxBatchSizeCount;
    }

    public void setMaxBatchSizeCount(int maxBatchSizeCount) {
        this.maxBatchSizeCount = maxBatchSizeCount;
    }

    public int getMaxFlushTime() {
        return maxFlushTime;
    }

    public void setMaxFlushTime(int maxFlushTime) {
        this.maxFlushTime = maxFlushTime;
    }

    public int getResponseTimeout() {
        return responseTimeout;
    }

    public void setResponseTimeout(int responseTimeout) {
        this.responseTimeout = responseTimeout;
    }

    public long getMaxBatchSizeBytes() {
        return maxBatchSizeBytes;
    }

    public void setMaxBatchSizeBytes(long maxBatchSizeBytes) {
        this.maxBatchSizeBytes = maxBatchSizeBytes;
    }
}
