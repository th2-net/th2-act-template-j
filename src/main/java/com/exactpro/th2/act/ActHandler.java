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

import com.exactpro.th2.act.grpc.ActGrpc.ActImplBase;
import com.exactpro.th2.act.grpc.PlaceMessageRequest;
import com.exactpro.th2.act.grpc.PlaceMessageRequestOrBuilder;
import com.exactpro.th2.act.grpc.PlaceMessageResponse;
import com.exactpro.th2.act.grpc.SendMessageResponse;
import com.exactpro.th2.act.impl.MessageResponseMonitor;
import com.exactpro.th2.act.receiver.AbstractMessageReceiver;
import com.exactpro.th2.act.receiver.MessageReceiver;
import com.exactpro.th2.act.rules.FieldCheckRule;
import com.exactpro.th2.act.utils.CheckMetadata;
import com.exactpro.th2.act.utils.EventUtils;
import com.exactpro.th2.act.utils.ReceiverContext;
import com.exactpro.th2.act.utils.ReceiverContext.NoResponseBodySupplier;
import com.exactpro.th2.act.utils.ReceiverContext.ReceiverSupplier;
import com.exactpro.th2.check1.grpc.Check1Service;
import com.exactpro.th2.check1.grpc.CheckpointRequest;
import com.exactpro.th2.check1.grpc.CheckpointResponse;
import com.exactpro.th2.common.event.Event;
import com.exactpro.th2.common.event.Event.Status;
import com.exactpro.th2.common.event.IBodyData;
import com.exactpro.th2.common.event.bean.TreeTable;
import com.exactpro.th2.common.event.bean.builder.MessageBuilder;
import com.exactpro.th2.common.grpc.Checkpoint;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.ListValueOrBuilder;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.MessageMetadata;
import com.exactpro.th2.common.grpc.MessageOrBuilder;
import com.exactpro.th2.common.grpc.RequestStatus;
import com.exactpro.th2.common.grpc.Value;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import static com.exactpro.th2.common.event.Event.Status.FAILED;
import static com.exactpro.th2.common.event.Event.Status.PASSED;
import static com.exactpro.th2.common.event.Event.start;
import static com.exactpro.th2.common.grpc.RequestStatus.Status.ERROR;
import static com.exactpro.th2.common.grpc.RequestStatus.Status.SUCCESS;
import static com.google.protobuf.TextFormat.shortDebugString;
import static java.lang.String.format;
import static java.time.Instant.ofEpochMilli;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toUnmodifiableMap;

public class ActHandler extends ActImplBase {
    private static final int DEFAULT_RESPONSE_TIMEOUT = 10_000;
    private static final Logger LOGGER = LoggerFactory.getLogger(ActHandler.class);

    private final Check1Service verifierConnector;
    private final MessageRouter<EventBatch> eventBatchMessageRouter;
    private final MessageRouter<MessageBatch> messageRouter;
    private final SubscriptionManager subscriptionManager;

    ActHandler(
            MessageRouter<MessageBatch> router,
            SubscriptionManager subscriptionManager,
            MessageRouter<EventBatch> eventBatchRouter,
            Check1Service verifierService
    ) {
        this.messageRouter = requireNonNull(router, "'Router' parameter");
        this.eventBatchMessageRouter = requireNonNull(eventBatchRouter, "'Event batch router' parameter");
        this.verifierConnector = requireNonNull(verifierService, "'Verifier service' parameter");
        this.subscriptionManager = requireNonNull(subscriptionManager, "'Callback list' parameter");
    }

    private static long getTimeout(Deadline deadline) {
        return deadline == null ? DEFAULT_RESPONSE_TIMEOUT : deadline.timeRemaining(MILLISECONDS);
    }

    @Override
    public void placeOrderFIX(PlaceMessageRequest request, StreamObserver<PlaceMessageResponse> responseObserver) {
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("placeOrderFIX request: " + shortDebugString(request));
            }
            placeMessageFieldRule(request, responseObserver, "NewOrderSingle", request.getMessage().getFieldsMap().get("ClOrdID").getSimpleValue(),
                    ImmutableMap.of("ExecutionReport", CheckMetadata.passOn("ClOrdID"), "BusinessMessageReject", CheckMetadata.failOn("BusinessRejectRefID")), "placeOrderFIX");
        } catch (RuntimeException | JsonProcessingException e) {
            LOGGER.error("Failed to place an order. Message = {}", request.getMessage(), e);
            sendErrorResponse(responseObserver, "Failed to place an order. Error: " + e.getMessage());
        } finally {
            LOGGER.debug("placeOrderFIX has finished");
        }
    }

    @Override
    public void sendMessage(PlaceMessageRequest request, StreamObserver<SendMessageResponse> responseObserver) {
        long startPlaceMessage = System.currentTimeMillis();
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Sending  message request: " + shortDebugString(request));
            }

            String actName = "sendMessage";
            // FIXME store parent with fail in case of children fail
            EventID parentId = createAndStoreParentEvent(request, actName, PASSED);

            Checkpoint checkpoint = registerCheckPoint(parentId);

            if (Context.current().isCancelled()) {
                LOGGER.warn("'{}' request cancelled by client", actName);
                sendMessageErrorResponse(responseObserver, "Request has been cancelled by client");
            }

            try {
                sendMessage(backwardCompatibilityConnectionId(request), parentId);
            } catch (Exception ex) {
                createAndStoreErrorEvent("sendMessage", ex.getMessage(), Instant.now(), parentId);
                throw ex;
            }

            SendMessageResponse response = SendMessageResponse.newBuilder()
                    .setStatus(RequestStatus.newBuilder().setStatus(SUCCESS).build())
                    .setCheckpointId(checkpoint)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (RuntimeException | IOException e) {
            LOGGER.error("Failed to send a message. Message = {}", request.getMessage(), e);
            sendMessageErrorResponse(responseObserver, "Send message failed. Error: " + e.getMessage());
        } finally {
            LOGGER.debug("Sending the message has been finished in {}", System.currentTimeMillis() - startPlaceMessage);
        }
    }

    @Override
    public void placeOrderMassCancelRequestFIX(PlaceMessageRequest request, StreamObserver<PlaceMessageResponse> responseObserver) {
        try {
            LOGGER.debug("placeOrderMassCancelRequestFIX request: {}", request);
            placeMessageFieldRule(request, responseObserver, "OrderMassCancelRequest", request.getMessage().getFieldsMap().get("ClOrdID").getSimpleValue(),
                    ImmutableMap.of("OrderMassCancelReport", CheckMetadata.passOn("ClOrdID")), "placeOrderMassCancelRequestFIX");
        } catch (RuntimeException | JsonProcessingException e) {
            LOGGER.error("Failed to place an OrderMassCancelRequest. Message = {}", request.getMessage(), e);
            sendErrorResponse(responseObserver, "Failed to place an OrderMassCancelRequest. Error: " + e.getMessage());
        } finally {
            LOGGER.debug("placeOrderMassCancelRequestFIX finished");
        }
    }

    @Override
    public void placeQuoteCancelFIX(PlaceMessageRequest request, StreamObserver<PlaceMessageResponse> responseObserver) {
        try {
            LOGGER.debug("placeQuoteCancelFIX request: {}", request);
            placeMessageFieldRule(request, responseObserver, "QuoteCancel", request.getMessage().getFieldsMap().get("QuoteMsgID").getSimpleValue(),
                    ImmutableMap.of("MassQuoteAcknowledgement", CheckMetadata.passOn("QuoteID")), "placeQuoteCancelFIX");
        } catch (RuntimeException | JsonProcessingException e) {
            LOGGER.error("Failed to place a QuoteCancel. Message = {}", request.getMessage(), e);
            sendErrorResponse(responseObserver, "Failed to place a QuoteCancel. Error: " + e.getMessage());
        } finally {
            LOGGER.debug("placeQuoteCancelFIX has finished");
        }
    }

    @Override
    public void placeQuoteRequestFIX(PlaceMessageRequest request, StreamObserver<PlaceMessageResponse> responseObserver) {
        try {
            LOGGER.debug("placeQuoteRequestFIX request: {}", request);
            placeMessageFieldRule(request, responseObserver, "QuoteRequest", request.getMessage().getFieldsMap().get("QuoteReqID").getSimpleValue(),
                    ImmutableMap.of("QuoteStatusReport", CheckMetadata.passOn("QuoteReqID")), "placeQuoteRequestFIX");
        } catch (RuntimeException | JsonProcessingException e) {
            LOGGER.error("Failed to place a QuoteRequest. Message = {}", request.getMessage(), e);
            sendErrorResponse(responseObserver, "Failed to place a QuoteRequest. Error: " + e.getMessage());
        } finally {
            LOGGER.debug("placeQuoteRequestFIX has finished");
        }
    }

    @Override
    public void placeQuoteResponseFIX(PlaceMessageRequest request, StreamObserver<PlaceMessageResponse> responseObserver) {
        try {
            LOGGER.debug("placeQuoteResponseFIX request: {}", request);
            placeMessageFieldRule(request, responseObserver, "QuoteResponse", request.getMessage().getFieldsMap().get("RFQID").getSimpleValue(),
                    ImmutableMap.of("ExecutionReport", CheckMetadata.passOn("RFQID"), "QuoteStatusReport", CheckMetadata.passOn("RFQID")), "placeQuoteResponseFIX");
        } catch (RuntimeException | JsonProcessingException e) {
            LOGGER.error("Failed to place a QuoteResponse. Message = {}", request.getMessage(), e);
            sendErrorResponse(responseObserver, "Failed to place a QuoteResponse. Error: " + e.getMessage());
        } finally {
            LOGGER.debug("placeQuoteResponseFIX has finished");
        }
    }

    @Override
    public void placeQuoteFIX(PlaceMessageRequest request, StreamObserver<PlaceMessageResponse> responseObserver) {
        try {
            LOGGER.debug("placeQuoteFIX request: {}", request);
            placeMessageFieldRule(request, responseObserver, "Quote", request.getMessage().getFieldsMap().get("RFQID").getSimpleValue(),
                    ImmutableMap.of("QuoteAck", CheckMetadata.passOn("RFQID")), "placeQuoteFIX");
        } catch (RuntimeException | JsonProcessingException e) {
            LOGGER.error("Failed to place a Quote. Message = {}", request.getMessage(), e);
            sendErrorResponse(responseObserver, "Failed to place a Quote. Error: " + e.getMessage());
        } finally {
            LOGGER.debug("placeQuoteFIX has finished");
        }
    }

    private void checkRequestMessageType(String expectedMessageType, MessageMetadata metadata) {
        if (!expectedMessageType.equals(metadata.getMessageType())) {
            throw new IllegalArgumentException(format("Unsupported request message type '%s', expected '%s'",
                    metadata.getMessageType(), expectedMessageType));
        }
    }

    /**
     *
     * @param expectedMessages mapping between response and the event status that should be applied in that case
     * @param noResponseBodySupplier supplier for {@link IBodyData} that will be added to the event in case there is not response received
     * @param receiver supplier for the {@link AbstractMessageReceiver} that will await for the required message
     */
    private void placeMessage(PlaceMessageRequest request, StreamObserver<PlaceMessageResponse> responseObserver,
                              String expectedRequestType, Map<String, CheckMetadata> expectedMessages, String actName,
                              NoResponseBodySupplier noResponseBodySupplier, ReceiverSupplier receiver) throws JsonProcessingException {

        long startPlaceMessage = System.currentTimeMillis();
        Message message = backwardCompatibilityConnectionId(request);
        checkRequestMessageType(expectedRequestType, message.getMetadata());
        ConnectionID requestConnId = message.getMetadata().getId().getConnectionId();

        EventID parentId = createAndStoreParentEvent(request, actName, PASSED);

        Checkpoint checkpoint = registerCheckPoint(parentId);

        MessageResponseMonitor monitor = new MessageResponseMonitor();
        try (AbstractMessageReceiver messageReceiver = receiver.create(monitor, new ReceiverContext(requestConnId, parentId))) {
            if (isSendPlaceMessage(message, responseObserver, parentId)) {
                long startAwaitSync = System.currentTimeMillis();
                long timeout = getTimeout(Context.current().getDeadline());
                monitor.awaitSync(timeout, MILLISECONDS);
                LOGGER.debug("messageReceiver.awaitSync for {} in {} ms",
                        actName, System.currentTimeMillis() - startAwaitSync);
                if (Context.current().isCancelled()) {
                    LOGGER.warn("'{}' request cancelled by client", actName);
                    sendErrorResponse(responseObserver, "The request has been cancelled by the client");
                } else {
                    processResponseMessage(actName,
                            responseObserver,
                            checkpoint,
                            parentId,
                            messageReceiver.getResponseMessage(),
                            expectedMessages,
                            timeout, messageReceiver.processedMessageIDs(), noResponseBodySupplier);
                }
            }
        } catch (RuntimeException | InterruptedException e) {
            LOGGER.error("'{}' internal error: {}", actName, e.getMessage(), e);
            createAndStoreErrorEvent(actName,
                    e.getMessage(),
                    ofEpochMilli(startPlaceMessage),
                    parentId);
            sendErrorResponse(responseObserver, "InternalError: " + e.getMessage());
        } finally {
            LOGGER.debug("placeMessage for {} in {} ms", actName, System.currentTimeMillis() - startPlaceMessage);
        }
    }

    private void placeMessageFieldRule(PlaceMessageRequest request, StreamObserver<PlaceMessageResponse> responseObserver,
                                       String expectedRequestType, String expectedFieldValue, Map<String, CheckMetadata> expectedMessages, String actName) throws JsonProcessingException {
        placeMessage(request, responseObserver, expectedRequestType, expectedMessages, actName,
                () -> Collections.singletonList(EventUtils.createNoResponseBody(expectedMessages, expectedFieldValue)), (monitor, context) -> {
                    Map<String, String> msgTypeToFieldName = expectedMessages.entrySet().stream()
                            .collect(toUnmodifiableMap(Entry::getKey, value -> value.getValue().getFieldName()));
                    CheckRule checkRule = new FieldCheckRule(expectedFieldValue, msgTypeToFieldName, context.getConnectionID());
                    return new MessageReceiver(subscriptionManager, monitor, checkRule, Direction.FIRST);
                });
    }

    private EventID createAndStoreParentEvent(PlaceMessageRequestOrBuilder request, String actName, Status status) throws JsonProcessingException {
        long startTime = System.currentTimeMillis();

        Event event = start()
                .name(actName + ' ' + request.getConnectionId().getSessionAlias())
                .description(request.getDescription())
                .type(actName)
                .status(status)
                .endTimestamp(); // FIXME set properly as is in the last child

        com.exactpro.th2.common.grpc.Event protoEvent = event.toProtoEvent(request.getParentEventId().getId());
        //FIXME process response
        try {
            eventBatchMessageRouter.send(EventBatch.newBuilder().addEvents(event.toProtoEvent(request.getParentEventId().getId())).build(), "publish", "event");
            LOGGER.debug("createAndStoreParentEvent for {} in {} ms", actName, System.currentTimeMillis() - startTime);
            return protoEvent.getId();
        } catch (IOException e) {
            throw new RuntimeException("Can not send event = " + protoEvent.getId().getId(), e);
        }
    }

    private void createAndStoreNoResponseEvent(String actName, NoResponseBodySupplier noResponseBodySupplier,
                                               Instant start,
                                               EventID parentEventId, Collection<MessageID> messageIDList) throws JsonProcessingException {

        Event errorEvent = Event.from(start)
                .endTimestamp()
                .name(format("Internal %s error", actName))
                .type("No response found by target keys.")
                .status(FAILED);
        for (IBodyData data : noResponseBodySupplier.createNoResponseBody()) {
                errorEvent.bodyData(data);
        }
        for(MessageID msgID : messageIDList){
            errorEvent.messageID(msgID);
        }
        storeEvent(errorEvent.toProtoEvent(parentEventId.getId()));
    }

    private void processResponseMessage(String actName,
                                        StreamObserver<PlaceMessageResponse> responseObserver,
                                        Checkpoint checkpoint,
                                        EventID parentEventId,
                                        Message responseMessage,
                                        Map<String, CheckMetadata> expectedMessages,
                                        long timeout,
                                        Collection<MessageID> messageIDList,
                                        NoResponseBodySupplier noResponseBodySupplier) throws JsonProcessingException {
        long startTime = System.currentTimeMillis();
        String message = format("No response message has been received in '%s' ms", timeout);
        if (responseMessage == null) {
            createAndStoreNoResponseEvent(actName, noResponseBodySupplier,
                    Instant.now(),
                    parentEventId, messageIDList);
            sendErrorResponse(responseObserver, message);
        } else {
            MessageMetadata metadata = responseMessage.getMetadata();
            String messageType = metadata.getMessageType();
            CheckMetadata checkMetadata = expectedMessages.get(messageType);
            TreeTable parametersTable = EventUtils.toTreeTable(responseMessage);
            storeEvent(Event.start()
                    .name(format("Received '%s' response message", messageType))
                    .type("message")
                    .status(checkMetadata.getEventStatus())
                    .bodyData(parametersTable)
                    .messageID(metadata.getId())
                    .toProtoEvent(parentEventId.getId())
            );
            PlaceMessageResponse response = PlaceMessageResponse.newBuilder()
                    .setResponseMessage(responseMessage)
                    .setStatus(RequestStatus.newBuilder().setStatus(checkMetadata.getRequestStatus()).build())
                    .setCheckpointId(checkpoint)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
        LOGGER.debug("processResponseMessage in {} ms", System.currentTimeMillis() - startTime);
    }

    private boolean isSendPlaceMessage(Message message, StreamObserver<PlaceMessageResponse> responseObserver,
                                       EventID parentEventId) {
        long startTime = System.currentTimeMillis();

        try {
            sendMessage(message, parentEventId);
            return true;
        } catch (IOException e) {
            LOGGER.error("Could not send message to queue", e);
            sendErrorResponse(responseObserver, "Could not send message to queue: " + e.getMessage());
            return false;
        } finally {
            LOGGER.debug("isSendPlaceMessage in {} ms", System.currentTimeMillis() - startTime);
        }
    }

    private void sendMessage(Message message, EventID parentEventId) throws IOException {
        try {
            LOGGER.debug("Sending the message started");

            //May be use in future for filtering
            //request.getConnectionId().getSessionAlias();
            messageRouter.send(MessageBatch.newBuilder()
                    .addMessages(Message.newBuilder(message)
                            .setParentEventId(parentEventId)
                            .build())
                    .build());
            //TODO remove after solving issue TH2-217
            //TODO process response
            EventBatch eventBatch = EventBatch.newBuilder()
                    .addEvents(EventUtils.createSendMessageEvent(message, parentEventId))
                    .build();
            eventBatchMessageRouter.send(eventBatch, "publish", "event");
        } finally {
            LOGGER.debug("Sending the message ended");
        }
    }

    private void createAndStoreErrorEvent(String actName, String message,Instant start,EventID parentEventId) throws JsonProcessingException {
        Event errorEvent = Event.from(start)
                .endTimestamp()
                .name(format("Internal %s error", actName))
                .type("Error")
                .status(FAILED)
                .bodyData(new MessageBuilder().text(message).build());
        storeEvent(errorEvent.toProto(parentEventId));
    }

    private void storeEvent(com.exactpro.th2.common.grpc.Event eventRequest) {
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Try to store event: {}", toDebugMessage(eventRequest));
            }
            eventBatchMessageRouter.send(EventBatch.newBuilder().addEvents(eventRequest).build(), "publish", "event");
        } catch (Exception e) {
            LOGGER.error("Could not store event", e);
        }
    }


    private Map<String, Object> convertMessage(MessageOrBuilder message) {
        Map<String, Object> fields = new HashMap<>();
        message.getFieldsMap().forEach((key, value) -> {
            Object convertedValue;
            if (value.hasMessageValue()) {
                convertedValue = convertMessage(value.getMessageValue());
            } else if (value.hasListValue()) {
                convertedValue = convertList(value.getListValue());
            } else {
                convertedValue = value.getSimpleValue();
            }
            fields.put(key, convertedValue);
        });
        return fields;
    }

    private Object convertList(ListValueOrBuilder listValue) {
        List<Value> valuesList = listValue.getValuesList();
        if (!valuesList.isEmpty()) {
            if (valuesList.get(0).hasMessageValue()) {
                return valuesList.stream().map(value -> convertMessage(value.getMessageValue())).collect(Collectors.toList());
            }
            return valuesList.stream().map(value -> valuesList.get(0).hasListValue() ? convertList(value.getListValue()) : value.getSimpleValue()).collect(Collectors.toList());
        }
        return new ArrayList<>();
    }

    private void sendErrorResponse(StreamObserver<PlaceMessageResponse> responseObserver,
                                   String message) {
        responseObserver.onNext(PlaceMessageResponse.newBuilder()
                .setStatus(RequestStatus.newBuilder()
                        .setStatus(ERROR)
                        .setMessage(message)
                        .build())
                .build());
        responseObserver.onCompleted();
        LOGGER.debug("Error response : {}", message);
    }

    private void sendMessageErrorResponse(StreamObserver<SendMessageResponse> responseObserver,
                                          String message) {
        responseObserver.onNext(SendMessageResponse.newBuilder()
                .setStatus(RequestStatus.newBuilder()
                        .setStatus(ERROR)
                        .setMessage(message)
                        .build())
                .build());
        responseObserver.onCompleted();
        LOGGER.debug("Error response : {}", message);
    }

    private Checkpoint registerCheckPoint(EventID parentEventId) {
        LOGGER.debug("Registering the checkpoint started");
        CheckpointResponse response = verifierConnector.createCheckpoint(CheckpointRequest.newBuilder()
                .setParentEventId(parentEventId)
                .build());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Registering the checkpoint ended. Response " + shortDebugString(response));
        }
        return response.getCheckpoint();
    }

    private static String toDebugMessage(com.google.protobuf.MessageOrBuilder messageOrBuilder) throws InvalidProtocolBufferException {
        return JsonFormat.printer().omittingInsignificantWhitespace().print(messageOrBuilder);
    }

    private Message backwardCompatibilityConnectionId(PlaceMessageRequest request) {
        ConnectionID connectionId = request.getMessage().getMetadata().getId().getConnectionId();
        if (!connectionId.getSessionAlias().isEmpty()) {
             return request.getMessage();
        } else {
            return Message.newBuilder(request.getMessage()).mergeMetadata(
                    MessageMetadata.newBuilder().mergeId(
                            MessageID.newBuilder().setConnectionId(request.getConnectionId()).build()
                    ).build()
            ).build();
        }
    }

}
