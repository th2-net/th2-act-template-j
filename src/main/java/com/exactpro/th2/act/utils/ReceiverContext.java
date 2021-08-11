/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.act.utils;

import com.exactpro.th2.act.ResponseMonitor;
import com.exactpro.th2.act.receiver.AbstractMessageReceiver;
import com.exactpro.th2.common.event.IBodyData;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.EventID;

import java.util.Collection;

import static java.util.Objects.requireNonNull;

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

    @FunctionalInterface public interface ReceiverSupplier {
        AbstractMessageReceiver create(ResponseMonitor monitor, ReceiverContext context);
    }

    @FunctionalInterface public interface NoResponseBodySupplier {
        Collection<IBodyData> createNoResponseBody();
    }
}