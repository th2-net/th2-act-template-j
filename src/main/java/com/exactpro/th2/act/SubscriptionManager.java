package com.exactpro.th2.act;

import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.schema.message.MessageListener;

public interface SubscriptionManager {
    void register(Direction direction, MessageListener<MessageBatch> listener);

    boolean unregister(Direction direction, MessageListener<MessageBatch> listener);
}
