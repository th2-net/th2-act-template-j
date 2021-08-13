package com.exactpro.th2.act;

import static java.util.Objects.requireNonNull;

import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.EventID;

public class ReceiverContext {
    private final ConnectionID connectionID;
    private final EventID parentId;

    public ReceiverContext(ConnectionID connectionID, EventID parentId) {
        this.connectionID = requireNonNull(connectionID, "'Connection id' parameter");
        this.parentId = requireNonNull(parentId, "'Parent id' parameter");
    }

    public ConnectionID getConnectionID() {
        return connectionID;
    }

    public EventID getParentId() {
        return parentId;
    }
}