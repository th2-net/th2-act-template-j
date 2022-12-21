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

package com.exactpro.th2.act;

import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageID;
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

import static com.google.protobuf.TextFormat.shortDebugString;

public class BiDirectionalMessageReceiver extends AbstractMessageReceiver {

    private enum State {
        START, OUTGOING_MATCHED, INCOMING_MATCHED
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(BiDirectionalMessageReceiver.class);

    private final SubscriptionManager subscriptionManager;
    private final CheckRule outgoingRule;
    private final Function<Message, CheckRule> incomingRuleSupplier;

    private final Queue<Message> incomingBuffer = new LinkedList<>();
    private final Listener incomingListener = this::processIncomingMessages;
    private final Listener outgoingListener = this::processOutgoingMessages;
    private final AtomicReference<CheckRule> incomingRule = new AtomicReference<>();

    private volatile State state = State.START;

    public BiDirectionalMessageReceiver(
            SubscriptionManager subscriptionManager,
            ResponseMonitor monitor,
            CheckRule outgoingRule,
            Function<Message, CheckRule> incomingRuleSupplier
    ) {
        super(monitor);
        this.subscriptionManager = Objects.requireNonNull(subscriptionManager, "'Subscription manager' parameter");
        this.outgoingRule = Objects.requireNonNull(outgoingRule, "'Outgoing rule' parameter");
        this.incomingRuleSupplier = Objects.requireNonNull(incomingRuleSupplier, "'Incoming rule supplier' parameter");

        subscriptionManager.register(Direction.FIRST, incomingListener);
        subscriptionManager.register(Direction.SECOND, outgoingListener);
    }

    @Override
    @Nullable
    public Message getResponseMessage() {
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
        subscriptionManager.unregister(Direction.FIRST, incomingListener);
        subscriptionManager.unregister(Direction.SECOND, outgoingListener);
    }

    private void processOutgoingMessages(Message message) {
        State current = state;
        if (current == State.OUTGOING_MATCHED || current == State.INCOMING_MATCHED) {
            // already has found everything for outgoing messages
            return;
        }
        if (outgoingRule.onMessage(message)) {
            Message response = outgoingRule.getResponse();
            if (response == null) {
                throw new IllegalStateException("Rules has found match in the batch but response is 'null'");
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Found match for outgoing rule. Match: {}", shortDebugString(response));
            }
            state = State.OUTGOING_MATCHED;
            CheckRule incomingRule = initOrGetIncomingRule(response);
            findInBuffer(incomingRule);
        }
    }

    private void processIncomingMessages(Message message) {
        if (state == State.START) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Buffering message: {}", shortDebugString(message));
            }
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

    private static boolean findMatchInBuffer(Queue<Message> buffer, CheckRule incomingRule) {
        Message message = buffer.poll();
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

    private CheckRule initOrGetIncomingRule(Message response) {
        return incomingRule.updateAndGet(rule -> {
            if (rule == null) {
                return incomingRuleSupplier.apply(response);
            }
            return rule;
        });
    }
}
