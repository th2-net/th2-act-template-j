/*
 * Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.act.Listener;
import com.exactpro.th2.act.SubscriptionManager;
import com.exactpro.th2.common.schema.message.DeliveryMetadata;
import com.exactpro.th2.common.schema.message.MessageListener;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Message;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage;
import com.exactpro.th2.common.utils.message.TransportMessageHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class SubscriptionManagerImpl implements MessageListener<GroupBatch>, SubscriptionManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionManagerImpl.class);
    private final Map<Direction, List<Listener>> callbacks;

    public SubscriptionManagerImpl() {
        Map<Direction, List<Listener>> callbacks = new EnumMap<>(Direction.class);
        callbacks.put(Direction.INCOMING, new CopyOnWriteArrayList<>());
        callbacks.put(Direction.OUTGOING, new CopyOnWriteArrayList<>());
        this.callbacks = Collections.unmodifiableMap(callbacks);
    }

    @Override
    public void register(Direction direction, Listener listener) {
        List<Listener> listeners = getMessageListeners(direction);
        listeners.add(listener);
    }

    @Override
    public boolean unregister(Direction direction, Listener listener) {
        List<Listener> listeners = getMessageListeners(direction);
        return listeners.remove(listener);
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

            for (Message<?> message : group.getMessages()) {
                Direction direction = message.getId().getDirection();
                List<Listener> listeners = callbacks.get(direction);
                if (listeners == null) {
                    LOGGER.warn("Unsupported direction {}. batch has been skipped: {}", direction, messageBatch);
                    return;
                }

                if (!(message instanceof ParsedMessage)) {
                    LOGGER.warn("Unsupported message type: {}, message id {}", message.getClass().getSimpleName(), message.getId());
                    continue;
                }

                for (Listener listener : listeners) {
                    try {
                        listener.handle(new TransportMessageHolder((ParsedMessage) message, messageBatch.getBook(), messageBatch.getSessionGroup()));
                    } catch (Exception e) {
                        LOGGER.error("Cannot handle message {}", message, e);
                    }
                }
            }
        }
    }

    private List<Listener> getMessageListeners(Direction direction) {
        List<Listener> listeners = callbacks.get(direction);
        if (listeners == null) {
            throw new IllegalArgumentException("Unsupported direction " + direction);
        }
        return listeners;
    }
}
