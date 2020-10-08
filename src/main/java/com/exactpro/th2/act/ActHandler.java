/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

import static com.datastax.driver.core.utils.UUIDs.timeBased;
import static com.exactpro.th2.common.event.Event.Status.PASSED;
import static com.exactpro.th2.infra.grpc.RequestStatus.Status.ERROR;
import static com.exactpro.th2.infra.grpc.RequestStatus.Status.SUCCESS;
import static com.google.protobuf.TextFormat.shortDebugString;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Instant.ofEpochMilli;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.act.grpc.ActGrpc.ActImplBase;
import com.exactpro.th2.act.grpc.PlaceMessageRequest;
import com.exactpro.th2.act.grpc.PlaceMessageRequestOrBuilder;
import com.exactpro.th2.act.grpc.PlaceMessageResponse;
import com.exactpro.th2.act.grpc.SendMessageResponse;
import com.exactpro.th2.common.event.Event.Status;
import com.exactpro.th2.eventstore.grpc.StoreEventRequest;
import com.exactpro.th2.infra.grpc.Checkpoint;
import com.exactpro.th2.infra.grpc.ConnectionID;
import com.exactpro.th2.infra.grpc.Event;
import com.exactpro.th2.infra.grpc.EventBatch;
import com.exactpro.th2.infra.grpc.EventID;
import com.exactpro.th2.infra.grpc.EventStatus;
import com.exactpro.th2.infra.grpc.ListValueOrBuilder;
import com.exactpro.th2.infra.grpc.Message;
import com.exactpro.th2.infra.grpc.MessageBatch;
import com.exactpro.th2.infra.grpc.MessageID;
import com.exactpro.th2.infra.grpc.MessageMetadata;
import com.exactpro.th2.infra.grpc.MessageOrBuilder;
import com.exactpro.th2.infra.grpc.RequestStatus;
import com.exactpro.th2.infra.grpc.Value;
import com.exactpro.th2.schema.factory.CommonFactory;
import com.exactpro.th2.schema.message.MessageRouter;
import com.exactpro.th2.verifier.grpc.CheckpointRequest;
import com.exactpro.th2.verifier.grpc.CheckpointResponse;
import com.exactpro.th2.verifier.grpc.VerifierService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;

import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.stub.StreamObserver;

public class ActHandler extends ActImplBase {
    private static final int DEFAULT_RESPONSE_TIMEOUT = 10_000;
    private final Logger logger = LoggerFactory.getLogger(getClass().getName() + '@' + hashCode());

    private final VerifierService verifierConnector;
    private final MessageRouter<EventBatch> eventBatchMessageRouter;
    private final MessageRouter<MessageBatch> messageRouter;

    ActHandler(CommonFactory factory, MessageRouter<MessageBatch> router) throws ClassNotFoundException {
        this.messageRouter = router;
        this.eventBatchMessageRouter = factory.getEventBatchRouter();
        this.verifierConnector = factory.getGrpcRouter().getService(VerifierService.class);
    }

    @Override
    public void placeOrderFIX(PlaceMessageRequest request, StreamObserver<PlaceMessageResponse> responseObserver) {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("placeOrderFIX request: " + shortDebugString(request));
            }
            placeMessage(request, responseObserver, "NewOrderSingle", "ClOrdID", request.getMessage().getFieldsMap().get("ClOrdID").getSimpleValue(),
                    ImmutableSet.of("ExecutionReport", "BusinessMessageReject"), "placeOrderFIX");
        } catch (RuntimeException | JsonProcessingException e) {
            logger.error("Place Order failed. Message = {}", request.getMessage(), e);
            sendErrorResponse(responseObserver, "Place Order failed. See the logs.");
        } finally {
            logger.debug("placeOrderFIX finished");
        }
    }

    @Override
    public void sendMessage(PlaceMessageRequest request, StreamObserver<SendMessageResponse> responseObserver) {
        long startPlaceMessage = System.currentTimeMillis();
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Send message request: " + shortDebugString(request));
            }

            String actName = "sendMessage";
            // FIXME store parent with fail in case of children fail
            StoreEventRequest storeEventRequest = createAndStoreParentEvent(request, actName, PASSED);
            EventID parentId = storeEventRequest.getEvent().getId();

            Checkpoint checkpoint = registerCheckPoint(parentId);

            if (Context.current().isCancelled()) {
                logger.warn("'{}' request cancelled by client", actName);
                sendMessageErrorResponse(responseObserver, "Cancelled by client");
            }

            sendMessage(request, parentId);

            SendMessageResponse response = SendMessageResponse.newBuilder()
                    .setStatus(RequestStatus.newBuilder().setStatus(SUCCESS).build())
                    .setCheckpointId(checkpoint)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (RuntimeException | IOException e) {
            logger.error("Send message failed. Message = {}", request.getMessage(), e);
            sendMessageErrorResponse(responseObserver, "Send message failed. See the logs.");
        } finally {
            logger.debug("Send message finished during {}", System.currentTimeMillis() - startPlaceMessage);
        }
    }

    @Override
    public void placeOrderMassCancelRequestFIX(PlaceMessageRequest request, StreamObserver<PlaceMessageResponse> responseObserver) {
        try {
            logger.debug("placeOrderMassCancelRequestFIX request: {}", request);
            placeMessage(request, responseObserver, "OrderMassCancelRequest", "ClOrdID", request.getMessage().getFieldsMap().get("ClOrdID").getSimpleValue(),
                    ImmutableSet.of("OrderMassCancelReport"), "placeOrderMassCancelRequestFIX");
        } catch (RuntimeException | JsonProcessingException e) {
            logger.error("Place OrderMassCancelRequest failed. Message = {}", request.getMessage(), e);
            sendErrorResponse(responseObserver, "Place OrderMassCancelRequest failed. See the logs.");
        } finally {
            logger.debug("placeOrderMassCancelRequestFIX finished");
        }
    }

    @Override
    public void placeQuoteCancelFIX(PlaceMessageRequest request, StreamObserver<PlaceMessageResponse> responseObserver) {
        try {
            logger.debug("placeQuoteCancelFIX request: {}", request);
            placeMessage(request, responseObserver, "QuoteCancel", "QuoteID", request.getMessage().getFieldsMap().get("QuoteMsgID").getSimpleValue(),
                    ImmutableSet.of("MassQuoteAcknowledgement"), "placeQuoteCancelFIX");
        } catch (RuntimeException | JsonProcessingException e) {
            logger.error("Place QuoteCancel failed. Message = {}", request.getMessage(), e);
            sendErrorResponse(responseObserver, "Place QuoteCancel failed. See the logs.");
        } finally {
            logger.debug("placeQuoteCancelFIX finished");
        }
    }

    @Override
    public void placeQuoteRequestFIX(PlaceMessageRequest request, StreamObserver<PlaceMessageResponse> responseObserver) {
        try {
            logger.debug("placeQuoteRequestFIX request: {}", request);
            placeMessage(request, responseObserver, "QuoteRequest", "QuoteReqID", request.getMessage().getFieldsMap().get("QuoteReqID").getSimpleValue(),
                    ImmutableSet.of("QuoteStatusReport"), "placeQuoteRequestFIX");
        } catch (RuntimeException | JsonProcessingException e) {
            logger.error("Place QuoteRequest failed. Message = {}", request.getMessage(), e);
            sendErrorResponse(responseObserver, "Place QuoteRequest failed. See the logs.");
        } finally {
            logger.debug("placeQuoteRequestFIX finished");
        }
    }

    @Override
    public void placeQuoteResponseFIX(PlaceMessageRequest request, StreamObserver<PlaceMessageResponse> responseObserver) {
        try {
            logger.debug("placeQuoteResponseFIX request: {}", request);
            placeMessage(request, responseObserver, "QuoteResponse", "RFQID", request.getMessage().getFieldsMap().get("RFQID").getSimpleValue(),
                    ImmutableSet.of("ExecutionReport", "QuoteStatusReport"), "placeQuoteResponseFIX");
        } catch (RuntimeException | JsonProcessingException e) {
            logger.error("Place QuoteRespons failed. Message = {}", request.getMessage(), e);
            sendErrorResponse(responseObserver, "Place QuoteRespons failed. See the logs.");
        } finally {
            logger.debug("placeQuoteResponseFIX finished");
        }
    }

    @Override
    public void placeQuoteFIX(PlaceMessageRequest request, StreamObserver<PlaceMessageResponse> responseObserver) {
        try {
            logger.debug("placeQuoteFIX request: {}", request);
            placeMessage(request, responseObserver, "Quote", "RFQID", request.getMessage().getFieldsMap().get("RFQID").getSimpleValue(),
                    ImmutableSet.of("QuoteAck"), "placeQuoteFIX");
        } catch (RuntimeException | JsonProcessingException e) {
            logger.error("Place Quote failed. Message = {}", request.getMessage(), e);
            sendErrorResponse(responseObserver, "Place Quote failed. See the logs.");
        } finally {
            logger.debug("placeQuoteFIX finished");
        }
    }

    private void checkRequestMessageType(String expectedMessageType, MessageMetadata metadata) {
        if (!expectedMessageType.equals(metadata.getMessageType())) {
            throw new IllegalArgumentException(format("Unsupported request message type '%s', expected '%s'",
                    metadata.getMessageType(), expectedMessageType));
        }
    }

    private void placeMessage(PlaceMessageRequest request, StreamObserver<PlaceMessageResponse> responseObserver,
            String expectedRequestType, String expectedFieldName, String expectedFieldValue, Set<String> expectedMessageTypes, String actName) throws JsonProcessingException {

        long startPlaceMessage = System.currentTimeMillis();
        EventID parentId = request.getParentEventId();
        ConnectionID requestConnId = request.getConnectionId();
        checkRequestMessageType(expectedRequestType, request.getMessage().getMetadata());
        var checkRule = new FixCheckRule(expectedFieldName, expectedFieldValue, expectedMessageTypes, requestConnId);

        // FIXME store parent with fail in case of children fail
        StoreEventRequest storeEventRequest = createAndStoreParentEvent(request, actName, PASSED);
        parentId = storeEventRequest.getEvent().getId();

        Checkpoint checkpoint = registerCheckPoint(parentId);

        try (MessageReceiver messageReceiver = new MessageReceiver(messageRouter, checkRule)) {
            if (isSendPlaceMessage(request, responseObserver, messageRouter, parentId)) {
                long startAwaitSync = System.currentTimeMillis();
                long timeout = getTimeout(Context.current().getDeadline());
                messageReceiver.awaitSync(timeout, MILLISECONDS);
                logger.debug("messageReceiver.awaitSync for {} in {} ms",
                        actName, System.currentTimeMillis() - startAwaitSync);
                if (Context.current().isCancelled()) {
                    logger.warn("'{}' request cancelled by client", actName);
                    sendErrorResponse(responseObserver, "Cancelled by client");
                } else {
                    processResponseMessage(actName,
                            responseObserver,
                            checkpoint,
                            parentId,
                            messageReceiver.getResponseMessage(),
                            timeout);
                }
            }
        } catch (RuntimeException | InterruptedException e) {
            logger.error("'{}' internal error: {}", actName, e.getMessage(), e);
            createAndStoreErrorEvent(actName,
                    e.getMessage(),
                    getTimestamp(ofEpochMilli(startPlaceMessage)),
                    getTimestamp(Instant.now()),
                    parentId);
            sendErrorResponse(responseObserver, "InternalError: " + e.getMessage());
        } finally {
            logger.debug("placeMessage for {} in {} ms", actName, System.currentTimeMillis() - startPlaceMessage);
        }
    }

    private static long getTimeout(Deadline deadline) {
        return deadline == null ? DEFAULT_RESPONSE_TIMEOUT : deadline.timeRemaining(MILLISECONDS);
    }

    private StoreEventRequest createAndStoreParentEvent(PlaceMessageRequestOrBuilder request, String actName, Status status) throws JsonProcessingException {
        long startTime = System.currentTimeMillis();

        com.exactpro.th2.common.event.Event event = com.exactpro.th2.common.event.Event.start()
                .name(actName + ' ' + request.getConnectionId().getSessionAlias())
                .description(request.getDescription())
                .type(actName)
                .status(status)
                .endTimestamp(); // FIXME set properly as is in the last child

        StoreEventRequest storeEventRequest = StoreEventRequest.newBuilder()
                .setEvent(event.toProtoEvent(request.getParentEventId().getId()))
                .build();
        //FIXME process response
        try {
            eventBatchMessageRouter.send(EventBatch.newBuilder().addEvents(storeEventRequest.getEvent()).build(), "publish", "event");
            logger.debug("createAndStoreParentEvent for {} in {} ms", actName, System.currentTimeMillis() - startTime);
        } catch (IOException e) {
            throw new RuntimeException("Can not send event = " + storeEventRequest.getEvent().getId().getId(), e);
        }

        return storeEventRequest;
    }

    private void processResponseMessage(String actName,
                                        StreamObserver<PlaceMessageResponse> responseObserver,
                                        Checkpoint checkpoint,
                                        EventID parentEventId,
                                        Message responseMessage,
                                        long timeout) {
        long startTime = System.currentTimeMillis();
        String message = format("No response message received during '%s' ms", timeout);
        if (responseMessage == null) {
            createAndStoreErrorEvent(actName,
                    message,
                    getTimestamp(Instant.now()),
                    getTimestamp(Instant.now()),
                    parentEventId);
            sendErrorResponse(responseObserver, message);
        } else {
            storeEvent(StoreEventRequest.newBuilder()
                    .setEvent(Event.newBuilder().setId(newEventId())
                            .setParentId(parentEventId)
                            .setName(format("Received '%s' response message", responseMessage.getMetadata().getMessageType()))
                            .setType("message")
                            .setStartTimestamp(getTimestamp(Instant.now()))
                            .setEndTimestamp(getTimestamp(Instant.now()))
                            .setStatus(EventStatus.SUCCESS)
                            .addAttachedMessageIds(responseMessage.getMetadata().getId())
                            .build())
                    .build());
            PlaceMessageResponse response = PlaceMessageResponse.newBuilder()
                    .setResponseMessage(responseMessage)
                    .setStatus(RequestStatus.newBuilder().setStatus(SUCCESS).build())
                    .setCheckpointId(checkpoint)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
        logger.debug("processResponseMessage in {} ms", System.currentTimeMillis() - startTime);
    }

    private boolean isSendPlaceMessage(PlaceMessageRequest request, StreamObserver<PlaceMessageResponse> responseObserver,
            MessageRouter<MessageBatch> router, EventID parentEventId) {
        long startTime = System.currentTimeMillis();

        try {
            sendMessage(request, parentEventId);
            return true;
        } catch (IOException e) {
            logger.error("Could not send message to queue", e);
            sendErrorResponse(responseObserver, "Could not send message to queue: " + e.getMessage());
            return false;
        } finally {
            logger.debug("isSendPlaceMessage in {} ms", System.currentTimeMillis() - startTime);
        }
    }

    private void sendMessage(PlaceMessageRequest request, EventID parentEventId) throws IOException {
        try {
            logger.debug("Send message start");
            Timestamp start = getTimestamp(Instant.now());

            //May be use in future for filtering
            //request.getConnectionId().getSessionAlias();
            Message message = backwardCompatibilityConnectionId(request);
            messageRouter.send(MessageBatch.newBuilder()
                    .addMessages(Message.newBuilder(message)
                            .setParentEventId(parentEventId)
                            .build())
                    .build());
            Timestamp end = getTimestamp(Instant.now());
            //TODO remove after solving issue TH2-217
            StoreEventRequest sendMessageEvent = createSendMessageEvent(request, start, end, parentEventId);
            //TODO process response
            eventBatchMessageRouter.send(EventBatch.newBuilder().addEvents(sendMessageEvent.getEvent()).build(), "publish", "event");
        } finally {
            logger.debug("Send message end");
        }
    }

    private Message backwardCompatibilityConnectionId(PlaceMessageRequest request) {
        ConnectionID connectionId = request.getMessage().getMetadata().getId().getConnectionId();
        if (connectionId != null && !connectionId.getSessionAlias().isEmpty()) {
            return request.getMessage();
        }
        return Message.newBuilder(request.getMessage())
                .mergeMetadata(MessageMetadata.newBuilder()
                        .mergeId(MessageID.newBuilder()
                                .setConnectionId(request.getConnectionId())
                                .build())
                        .build())
                .build();
    }

    private static Timestamp getTimestamp(Instant instant) {
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    private StoreEventRequest createSendMessageEvent(PlaceMessageRequest request,
            Timestamp start,
            Timestamp end,
            EventID parentEventId) {
        return StoreEventRequest.newBuilder()
                .setEvent(Event.newBuilder().setId(newEventId())
                        .setParentId(parentEventId)
                        .setName("Send '" + request.getMessage().getMetadata().getMessageType() + "' message")
                        .setType("sendMessage")
                        .setStartTimestamp(start)
                        .setEndTimestamp(end)
                        .setStatus(EventStatus.SUCCESS)
                        .setBody(convertMessageToEvent(request.getMessage(), request.getConnectionId().getSessionAlias()))
                        .build())
                .build();
    }

    private void createAndStoreErrorEvent(String actName, String message,
                                               Timestamp start,
                                               Timestamp end,
                                               EventID parentEventId) {

        StoreEventRequest errorEvent = StoreEventRequest.newBuilder()
                    .setEvent(Event.newBuilder().setId(newEventId())
                            .setParentId(parentEventId)
                            .setName(format("Internal %s error", actName))
                            .setType("Error")
                            .setStartTimestamp(start)
                            .setEndTimestamp(end)
                            .setStatus(EventStatus.FAILED)
                            .setBody(ByteString.copyFrom(format("{\"message\": \"%s\"}", message), UTF_8))
                            .build())
                    .build();
        storeEvent(errorEvent);
    }

    private void storeEvent(StoreEventRequest eventRequest) {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("try to store event: {}", toDebugMessage(eventRequest));
            }
            eventBatchMessageRouter.send(EventBatch.newBuilder().addEvents(eventRequest.getEvent()).build(), "publish", "event");
        } catch (Exception e) {
            logger.error("could not store event", e);
        }
    }

    private ByteString convertMessageToEvent(Message message, String connectivityId) {
        SendMessageEvent messageEvent = new SendMessageEvent();
        messageEvent.setConnectivityId(connectivityId);
        messageEvent.setMessageName(message.getMetadata().getMessageType());
        messageEvent.setFields(convertMessage(message));
        try {
            return ByteString.copyFrom(new ObjectMapper().writeValueAsBytes(messageEvent));
        } catch (JsonProcessingException e) {
            logger.error("Could not convert Message to json", e);
            return ByteString.EMPTY;
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
        logger.debug("error response : {}", message);
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
        logger.debug("error response : {}", message);
    }

    private Checkpoint registerCheckPoint(EventID parentEventId) {
        logger.debug("Register checkpoint start");
        CheckpointResponse response = verifierConnector.createCheckpoint(CheckpointRequest.newBuilder()
                .setParentEventId(parentEventId)
                .build());
        if (logger.isDebugEnabled()) {
            logger.debug("Register checkpoint end. Response " + shortDebugString(response));
        }
        return response.getCheckpoint();
    }

    private static EventID newEventId() {
        return EventID.newBuilder().setId(timeBased().toString()).build();
    }

    private static String toDebugMessage(com.google.protobuf.MessageOrBuilder messageOrBuilder) throws InvalidProtocolBufferException {
        return JsonFormat.printer().omittingInsignificantWhitespace().print(messageOrBuilder);
    }
}
