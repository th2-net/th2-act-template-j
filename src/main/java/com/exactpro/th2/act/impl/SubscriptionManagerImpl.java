/*
 * Copyright 2021-2025 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.act.impl;

import com.exactpro.th2.act.GroupHolder;
import com.exactpro.th2.act.GroupListener;
import com.exactpro.th2.act.MessageListener;
import com.exactpro.th2.act.SubscriptionManager;
import com.exactpro.th2.act.TransportGroupHolder;
import com.exactpro.th2.common.schema.message.DeliveryMetadata;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Message;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage;
import com.exactpro.th2.common.utils.message.MessageHolder;
import com.exactpro.th2.common.utils.message.TransportMessageHolder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class SubscriptionManagerImpl implements com.exactpro.th2.common.schema.message.MessageListener<GroupBatch>, SubscriptionManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionManagerImpl.class);
    private final Map<Direction, List<MessageListener>> messageCallbacks;
    private final Map<Direction, List<GroupListener>> groupCallbacks;

    public SubscriptionManagerImpl() {
        this.messageCallbacks = Collections.unmodifiableMap(createCallbacks());
        this.groupCallbacks = Collections.unmodifiableMap(createCallbacks());
    }

    @Override
    public void register(Direction direction, MessageListener listener) {
        getListeners(messageCallbacks, direction).add(listener);
    }

    @Override
    public void register(Direction direction, GroupListener listener) {
        getListeners(groupCallbacks, direction).add(listener);
    }

    @Override
    public boolean unregister(Direction direction, MessageListener listener) {
        return getListeners(messageCallbacks, direction).remove(listener);
    }

    @Override
    public boolean unregister(Direction direction, GroupListener listener) {
        return getListeners(groupCallbacks, direction).remove(listener);
    }

    @Override
    public void handle(
            @SuppressWarnings("UseOfConcreteClass") DeliveryMetadata deliveryMetadata,
            @SuppressWarnings("ParameterNameDiffersFromOverriddenParameter") GroupBatch messageBatch
    ) {
        if (messageBatch.getGroups().isEmpty()) {
            LOGGER.warn("Empty batch received {}", messageBatch);
            return;
        }

        for (MessageGroup group : messageBatch.getGroups()) {
            if (group.getMessages().isEmpty()) {
                LOGGER.warn("Empty group received {}", group);
                return;
            }

            // as usual all messages in group has the same direction, but API provide ability to set different
            Direction groupDirection = group.getMessages().get(0).getId().getDirection();
            List<GroupListener> groupListeners = getListeners(groupCallbacks, groupDirection);
            if (!groupListeners.isEmpty()) {
                GroupHolder holder = new TransportGroupHolder(group, messageBatch.getBook(), messageBatch.getSessionGroup());
                for (GroupListener listener : groupListeners) {
                    try {
                        listener.handle(holder);
                    } catch (Exception e) {
                        LOGGER.error("Cannot handle group {}", group, e);
                    }
                }
            }

            for (Message<?> message : group.getMessages()) {
                if (!(message instanceof ParsedMessage)) {
                    LOGGER.warn("Unsupported message type: {}, message id {}", message.getClass().getSimpleName(), message.getId());
                    continue;
                }

                List<MessageListener> messageListeners = getListeners(messageCallbacks, message.getId().getDirection());
                if (messageListeners.isEmpty()) {
                    continue;
                }

                MessageHolder holder = new TransportMessageHolder((ParsedMessage) message, messageBatch.getBook(), messageBatch.getSessionGroup());
                for (MessageListener listener : messageListeners) {
                    try {
                        listener.handle(holder);
                    } catch (Exception e) {
                        LOGGER.error("Cannot handle message {}", message, e);
                    }
                }
            }
        }
    }

    private <T> List<T> getListeners(Map<Direction, List<T>> callbacks, Direction direction) {
        List<T> listeners = callbacks.get(direction);
        if (listeners == null) {
            throw new IllegalArgumentException("Unsupported direction " + direction);
        }
        return listeners;
    }

    private static <T> @NotNull Map<Direction, List<T>> createCallbacks() {
        Map<Direction, List<T>> callbacks = new EnumMap<>(Direction.class);
        callbacks.put(Direction.INCOMING, new CopyOnWriteArrayList<>());
        callbacks.put(Direction.OUTGOING, new CopyOnWriteArrayList<>());
        return callbacks;
    }
}
