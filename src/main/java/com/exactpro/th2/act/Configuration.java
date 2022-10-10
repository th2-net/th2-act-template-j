package com.exactpro.th2.act;

public class Configuration {
    private static final long DEFAULT_BATCH_SIZE = 1048576;
    private static final int DEFAULT_RESPONSE_TIMEOUT = 10_000;

    private int responseTimeout = DEFAULT_RESPONSE_TIMEOUT;
    private long maxBatchSize = DEFAULT_BATCH_SIZE;

    public int getResponseTimeout() {
        return responseTimeout;
    }

    public void setResponseTimeout(int responseTimeout) {
        this.responseTimeout = responseTimeout;
    }

    public long getMaxBatchSize() {
        return maxBatchSize;
    }

    public void setMaxBatchSize(long maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }
}
