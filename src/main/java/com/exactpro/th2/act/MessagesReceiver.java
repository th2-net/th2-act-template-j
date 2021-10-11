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
package com.exactpro.th2.act;

import static com.exactpro.th2.common.grpc.RequestStatus.Status.SUCCESS;

import java.util.Collection;
import java.util.Objects;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.act.ResponseMapper.ResponseStatus;
import com.exactpro.th2.act.grpc.MessageResponse;
import com.exactpro.th2.common.event.Event;
import com.exactpro.th2.common.event.Event.Status;
import com.exactpro.th2.common.event.bean.TreeTable;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.MessageMetadata;
import com.exactpro.th2.common.grpc.RequestStatus;
import com.exactpro.th2.common.schema.message.MessageListener;
import com.fasterxml.jackson.core.JsonProcessingException;

import io.grpc.stub.StreamObserver;

public class MessagesReceiver extends AbstractMessageReceiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessagesReceiver.class);

    private final SubscriptionManager subscriptionManager;
    private final Direction direction;
    private final CheckRule checkRule;
    private final EventSender eventSender;
    private final EventID parentId;
    private final StreamObserver<MessageResponse> responseObserver;
    private final long timeout;
    private final long START_MILLIS = System.currentTimeMillis();
    private final MessageListener<MessageBatch> callback = this::processIncomingMessages;

    //FIXME: Add queue name
    public MessagesReceiver(
            SubscriptionManager subscriptionManager,
            ResponseMonitor monitor,
            CheckRule checkRule,
            Direction direction,
            EventSender eventSender,
            EventID parentId,
            StreamObserver<MessageResponse> responseObserver,
            long timeout) {
        super(monitor);
        this.subscriptionManager = Objects.requireNonNull(subscriptionManager, "'Subscription manager' parameter");
        this.direction = Objects.requireNonNull(direction, "'Direction' parameter");
        this.checkRule = Objects.requireNonNull(checkRule, "'Check rule' parameter");
        this.eventSender = eventSender;
        this.parentId = parentId;
        this.responseObserver = responseObserver;
        this.subscriptionManager.register(direction, callback);
        this.timeout = timeout;
    }

    public void close() {
        subscriptionManager.unregister(direction, callback);
    }

    @Override
    @Nullable
    public Message getResponseMessage() {
        return null;
    }

    @Override
    public Collection<MessageID> processedMessageIDs() {
        return checkRule.processedIDs();
    }

    @Nullable
    @Override
    public State getState() {
        return null;
    }

    @Nullable
    @Override
    public ResponseStatus getResponseStatus() {
        return null;
    }

    private void processIncomingMessages(String consumingTag, MessageBatch batch) throws InterruptedException {
        try {
            LOGGER.debug("Message received batch, size {}", batch.getSerializedSize());
            for (Message message : batch.getMessagesList()) {
                if (System.currentTimeMillis() - START_MILLIS > timeout) {
                    eventSender.storeEvent(Event.start()
                            .name("Action stopped by timeout.")
                            .type("Timeout")
                            .status(Status.PASSED)
                            .toProto(parentId)
                    );
                    responseObserver.onNext(MessageResponse.newBuilder()
                            .setStatus(RequestStatus.newBuilder()
                                    .setStatus(SUCCESS)
                                    .setMessage("Request ended by timeout.")
                                    .build())
                            .build());
                    responseObserver.onCompleted();
                    close();
                } else {
                    MessageMetadata metadata = message.getMetadata();
                    String messageType = metadata.getMessageType();

                    TreeTable parametersTable = EventUtils.toTreeTable(message);
                    eventSender.storeEvent(Event.start()
                            .name(String.format("Received '%s' requested message.", messageType))
                            .type("Requested Message")
                            .status(checkRule.getResponseStatus() == ResponseStatus.PASSED ? Status.PASSED : Status.FAILED)
                            .bodyData(parametersTable)
                            .messageID(metadata.getId())
                            .toProto(parentId)
                    );
                    MessageResponse response = MessageResponse.newBuilder()
                            .setResponseMessage(message)
                            .setStatus(RequestStatus.newBuilder().setStatus(SUCCESS).build())
                            .build();
                    responseObserver.onNext(response);
                }
            }
        } catch (RuntimeException | JsonProcessingException e) {
            LOGGER.error("Could not process incoming message", e);
        }
    }

}
