/*
 * Copyright 2021-2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.act;

import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.utils.message.MessageHolder;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Objects;

public abstract class AbstractMessageReceiver implements AutoCloseable {
    private final ResponseMonitor monitor;

    protected AbstractMessageReceiver(ResponseMonitor monitor) {
        this.monitor = Objects.requireNonNull(monitor, "'Monitor' parameter");
    }

    @Override
    public abstract void close();

    @Nullable
    public abstract MessageHolder getResponseMessage();

    public abstract Collection<MessageID> processedMessageIDs();

    protected void signalAboutReceived() {
        monitor.responseReceived();
    }
}
