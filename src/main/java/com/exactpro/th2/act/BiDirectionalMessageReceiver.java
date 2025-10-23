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

package com.exactpro.th2.act;

import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.utils.message.MessageHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction.INCOMING;
import static com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction.OUTGOING;

public class BiDirectionalMessageReceiver extends AbstractMessageReceiver {

    private enum State {
        START, OUTGOING_MATCHED, INCOMING_MATCHED
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(BiDirectionalMessageReceiver.class);

    private final SubscriptionManager subscriptionManager;
    private final CheckRule outgoingRule;
    private final Function<MessageHolder, CheckRule> incomingRuleSupplier;

    private final Queue<MessageHolder> incomingBuffer = new LinkedList<>();
    private final MessageListener incomingListener = this::processIncomingMessages;
    private final MessageListener outgoingListener = this::processOutgoingMessages;
    private final AtomicReference<CheckRule> incomingRule = new AtomicReference<>();

    private volatile State state = State.START;

    public BiDirectionalMessageReceiver(
            SubscriptionManager subscriptionManager,
            ResponseMonitor monitor,
            CheckRule outgoingRule,
            Function<MessageHolder, CheckRule> incomingRuleSupplier
    ) {
        super(monitor);
        this.subscriptionManager = Objects.requireNonNull(subscriptionManager, "'Subscription manager' parameter");
        this.outgoingRule = Objects.requireNonNull(outgoingRule, "'Outgoing rule' parameter");
        this.incomingRuleSupplier = Objects.requireNonNull(incomingRuleSupplier, "'Incoming rule supplier' parameter");

        subscriptionManager.register(INCOMING, incomingListener);
        subscriptionManager.register(OUTGOING, outgoingListener);
    }

    @Override
    @Nullable
    public MessageHolder getResponseMessage() {
        CheckRule rule = incomingRule.get();
        return rule == null ? null : rule.getResponse();
    }

    @Override
    public Collection<MessageID> processedMessageIDs() {
        CheckRule incoming = incomingRule.get();
        if (incoming == null || incoming.processedIDs().isEmpty()) {
            return outgoingRule.processedIDs();
        }
        Collection<MessageID> messageIDS = new ArrayList<>(outgoingRule.processedIDs().size() + incoming.processedIDs().size());
        messageIDS.addAll(outgoingRule.processedIDs());
        messageIDS.addAll(incoming.processedIDs());
        return messageIDS;
    }

    @Override
    public void close() {
        subscriptionManager.unregister(INCOMING, incomingListener);
        subscriptionManager.unregister(OUTGOING, outgoingListener);
    }

    private void processOutgoingMessages(MessageHolder message) {
        State current = state;
        if (current == State.OUTGOING_MATCHED || current == State.INCOMING_MATCHED) {
            // already has found everything for outgoing messages
            return;
        }
        if (outgoingRule.onMessage(message)) {
            MessageHolder response = outgoingRule.getResponse();
            if (response == null) {
                throw new IllegalStateException("Rules has found match in the batch but response is 'null'");
            }
            LOGGER.debug("Found match for outgoing rule. Match: {}", response);
            state = State.OUTGOING_MATCHED;
            CheckRule incomingRule = initOrGetIncomingRule(response);
            findInBuffer(incomingRule);
        }
    }

    private void processIncomingMessages(MessageHolder message) {
        if (state == State.START) {
            LOGGER.trace("Buffering message: {}", message);
            synchronized (incomingBuffer) {
                incomingBuffer.add(message);
            }
            if (state == State.START) {
                return;
            }
        }
        if (state == State.INCOMING_MATCHED) {
            // already has found the match
            return;
        }

        CheckRule incomingRule = initOrGetIncomingRule(outgoingRule.getResponse());
        if (findInBuffer(incomingRule)) {
            // match is found
            return;
        }
        if (incomingRule.onMessage(message)) {
            matchFound();
        }
    }

    private boolean findInBuffer(CheckRule incomingRule) {
        synchronized (incomingBuffer) {
            if (!incomingBuffer.isEmpty()) {
                if (findMatchInBuffer(incomingBuffer, incomingRule)) {
                    matchFound();
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean findMatchInBuffer(Queue<MessageHolder> buffer, CheckRule incomingRule) {
        MessageHolder message = buffer.poll();
        while (message != null) {
            if (incomingRule.onMessage(message)) {
                return true;
            }
            message = buffer.poll();
        }
        return false;
    }

    private void matchFound() {
        state = State.INCOMING_MATCHED;
        signalAboutReceived();
    }

    private CheckRule initOrGetIncomingRule(MessageHolder response) {
        return incomingRule.updateAndGet(rule -> {
            if (rule == null) {
                return incomingRuleSupplier.apply(response);
            }
            return rule;
        });
    }
}
