/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageID;
import com.google.protobuf.TextFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Objects;

public class MessageReceiver extends AbstractMessageReceiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageReceiver.class);

    private final SubscriptionManager subscriptionManager;
    private final Direction direction;
    private final Listener callback = this::processIncomingMessages;
    private final CheckRule checkRule;
    private volatile Message firstMatch;

    //FIXME: Add queue name
    public MessageReceiver(
            SubscriptionManager subscriptionManager,
            ResponseMonitor monitor,
            CheckRule checkRule,
            Direction direction
    ) {
        super(monitor);
        this.subscriptionManager = Objects.requireNonNull(subscriptionManager, "'Subscription manager' parameter");
        this.direction = Objects.requireNonNull(direction, "'Direction' parameter");
        this.checkRule = Objects.requireNonNull(checkRule, "'Check rule' parameter");
        this.subscriptionManager.register(direction, callback);
    }

    public void close() {
        subscriptionManager.unregister(direction, callback);
    }

    @Override
    @Nullable
    public Message getResponseMessage() {
        return firstMatch;
    }

    @Override
    public Collection<MessageID> processedMessageIDs() {
        return checkRule.processedIDs();
    }

    private void processIncomingMessages(Message message) {
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Message received message, size {} bytes", message.getSerializedSize());
            }
            if (hasMatch()) {
                LOGGER.debug("The match was already found. Skip message checking");
                return;
            }
            if (checkRule.onMessage(message)) {
                firstMatch = message;
                signalAboutReceived();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Found first match '{}'", TextFormat.shortDebugString(message));
                }
            }
        } catch (RuntimeException e) {
            LOGGER.error("Could not process incoming message", e);
        }
    }

    private boolean hasMatch() {
        return firstMatch != null;
    }
}
