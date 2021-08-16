/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

import static com.google.protobuf.TextFormat.shortDebugString;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.grpc.MessageBatchOrBuilder;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.schema.message.MessageListener;

public class EchoCheckMessageReceiver extends AbstractMessageReceiver {

    private static final Logger LOGGER = LoggerFactory.getLogger(EchoCheckMessageReceiver.class);
    private final SubscriptionManager subscriptionManager;
    private final CheckRule outgoingRule;
    private final CheckRule incomingRule;
    private final Queue<MessageBatch> incomingBuffer = new LinkedList<>();
    private volatile State state = State.START;
    private final MessageListener<MessageBatch> incomingListener = this::processIncomingMessages;
    private final MessageListener<MessageBatch> outgoingListener = this::processOutgoingMessages;

    public EchoCheckMessageReceiver(
            SubscriptionManager subscriptionManager,
            ResponseMonitor monitor,
            CheckRule outgoingRule,
            CheckRule incomingRule
    ) {
        super(monitor);
        this.subscriptionManager = Objects.requireNonNull(subscriptionManager, "'Subscription manager' parameter");
        this.outgoingRule = Objects.requireNonNull(outgoingRule, "'Outgoing rule' parameter");
        this.incomingRule = Objects.requireNonNull(incomingRule, "'Incoming rule supplier' parameter");

        subscriptionManager.register(Direction.FIRST, incomingListener);
        subscriptionManager.register(Direction.SECOND, outgoingListener);
    }

    private static boolean isMatchInBuffer(Queue<MessageBatch> buffer, CheckRule incomingRule) {
        MessageBatch batch;
        while ((batch = buffer.poll()) != null) {
            if (hasMatches(batch, incomingRule)) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasMatches(MessageBatchOrBuilder batch, CheckRule rule) {
        for (Message message : batch.getMessagesList()) {
            if (rule.onMessage(message)) {
                return true;
            }
        }
        return false;
    }

    @Override
    @Nullable
    public Message getResponseMessage() {
        return incomingRule.getResponse();
    }

    @Override
    public Collection<MessageID> processedMessageIDs() {

        if (incomingRule.processedIDs().isEmpty()) {
            return outgoingRule.processedIDs();
        }
        Collection<MessageID> messageIDS = new ArrayList<MessageID>(outgoingRule.processedIDs().size() + incomingRule.processedIDs().size());
        messageIDS.addAll(outgoingRule.processedIDs());
        messageIDS.addAll(incomingRule.processedIDs());
        return messageIDS;
    }

    @Override
    public void close() {
        subscriptionManager.unregister(Direction.FIRST, incomingListener);
        subscriptionManager.unregister(Direction.SECOND, outgoingListener);
    }

    private void processOutgoingMessages(String consumingTag, MessageBatchOrBuilder batch) {
        State current = state;
        if (current == State.OUTGOING_MATCHED || current == State.INCOMING_MATCHED) {
            // already has found everything for outgoing messages
            return;
        }
        if (hasMatches(batch, outgoingRule)) {
            Message response = outgoingRule.getResponse();
            if (response == null) {
                throw new IllegalStateException("Rules has found match in the batch but response is 'null'");
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Found match for outgoing rule. Match: {}", shortDebugString(response));
            }
            state = State.OUTGOING_MATCHED;
            isInBuffer(incomingRule);
        }
    }

    private void processIncomingMessages(String consumingTag, MessageBatch batch) {
        if (state == State.START) {
            synchronized (incomingBuffer) {
                incomingBuffer.add(batch);
            }
            if (state == State.START) {
                return;
            }
        }
        if (state == State.INCOMING_MATCHED) {
            // already has found the match
            return;
        }
        if (isInBuffer(incomingRule)) {
            // match is found
            return;
        }
        if (hasMatches(batch, incomingRule)) {
            matchFound();
        }
    }

    private boolean isInBuffer(CheckRule incomingRule) {
        synchronized (incomingBuffer) {
            if (!incomingBuffer.isEmpty()) {
                if (isMatchInBuffer(incomingBuffer, incomingRule)) {
                    matchFound();
                    return true;
                }
            }
        }
        return false;
    }

    private void matchFound() {
        state = State.INCOMING_MATCHED;
        signalAboutReceived();
    }
    public State getState(){
        return state;
    }


}
