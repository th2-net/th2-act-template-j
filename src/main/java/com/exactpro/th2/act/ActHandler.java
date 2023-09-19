/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.act.grpc.SendRawMessageRequest;
import com.exactpro.th2.act.grpc.SendRawMessageResponse;
import com.exactpro.th2.act.impl.MessageResponseMonitor;
import com.exactpro.th2.act.rules.FieldCheckRule;
import com.exactpro.th2.check1.grpc.Check1Service;
import com.exactpro.th2.check1.grpc.CheckpointRequest;
import com.exactpro.th2.check1.grpc.CheckpointResponse;
import com.exactpro.th2.common.event.Event;
import com.exactpro.th2.common.event.Event.Status;
import com.exactpro.th2.common.event.EventUtils;
import com.exactpro.th2.common.event.IBodyData;
import com.exactpro.th2.common.event.bean.TreeTable;
import com.exactpro.th2.common.event.bean.builder.CollectionBuilder;
import com.exactpro.th2.common.event.bean.builder.MessageBuilder;
import com.exactpro.th2.common.event.bean.builder.RowBuilder;
import com.exactpro.th2.common.event.bean.builder.TreeTableBuilder;
import com.exactpro.th2.common.grpc.Checkpoint;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.ListValueOrBuilder;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.MessageMetadata;
import com.exactpro.th2.common.grpc.MessageOrBuilder;
import com.exactpro.th2.common.grpc.RawMessageMetadata;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage;
import com.exactpro.th2.common.grpc.RequestStatus;
import com.exactpro.th2.common.grpc.Value;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage;
import com.exactpro.th2.common.utils.event.EventUtilsKt;
import com.exactpro.th2.common.utils.message.MessageHolder;
import com.exactpro.th2.common.utils.message.MessageTableColumn;
import com.exactpro.th2.common.utils.message.MessageUtilsKt;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBufUtil;
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
import static com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction.INCOMING;
import static com.exactpro.th2.common.utils.message.MessageHolderUtilsKt.getTreeTable;
import static com.exactpro.th2.common.utils.message.MessageUtilsKt.toTransport;
import static com.exactpro.th2.common.utils.message.transport.MessageUtilsKt.toBatch;
import static com.exactpro.th2.common.utils.message.transport.MessageUtilsKt.toGroup;
import static com.exactpro.th2.common.utils.message.transport.MessageUtilsKt.toTreeTable;
import static com.google.protobuf.TextFormat.shortDebugString;
import static java.lang.String.format;
import static java.time.Instant.ofEpochMilli;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toUnmodifiableMap;

public class ActHandler extends ActImplBase {
    private static final int DEFAULT_RESPONSE_TIMEOUT = 10_000;
    private static final String SEND_RAW_QUEUE_ATTRIBUTE = "send_raw";
    private static final Logger LOGGER = LoggerFactory.getLogger(ActHandler.class);

    private final Check1Service check1Service;
    private final MessageRouter<EventBatch> eventBatchMessageRouter;
    private final MessageRouter<GroupBatch> messageRouter;
    private final SubscriptionManager subscriptionManager;

    ActHandler(
            MessageRouter<GroupBatch> router,
            SubscriptionManager subscriptionManager,
            MessageRouter<EventBatch> eventBatchRouter,
            Check1Service check1Service
    ) {
        this.messageRouter = requireNonNull(router, "'Router' parameter");
        this.eventBatchMessageRouter = requireNonNull(eventBatchRouter, "'Event batch router' parameter");
        this.check1Service = requireNonNull(check1Service, "'check1 service' parameter");
        this.subscriptionManager = requireNonNull(subscriptionManager, "'Callback list' parameter");
    }

    private static long getTimeout(Deadline deadline) {
        return deadline == null ? DEFAULT_RESPONSE_TIMEOUT : deadline.timeRemaining(MILLISECONDS);
    }

    private static String toDebugMessage(com.google.protobuf.MessageOrBuilder messageOrBuilder) throws InvalidProtocolBufferException {
        return JsonFormat.printer().omittingInsignificantWhitespace().print(messageOrBuilder);
    }

    @Override
    public void placeOrderFIX(PlaceMessageRequest request, StreamObserver<PlaceMessageResponse> responseObserver) {
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("placeOrderFIX request: " + shortDebugString(request));
            }
            placeMessageFieldRule(request, responseObserver, "NewOrderSingle", request.getMessage().getFieldsMap().get("ClOrdID").getSimpleValue(),
                    ImmutableMap.of("ExecutionReport", CheckMetadata.passOn("ClOrdID"), "BusinessMessageReject", CheckMetadata.failOn("BusinessRejectRefID")), "placeOrderFIX");
        } catch (RuntimeException | IOException e) {
            LOGGER.error("Failed to place an order. Message = {}", request.getMessage(), e);
            sendErrorResponse(responseObserver, "Failed to place an order. Error: " + e.getMessage());
        } finally {
            LOGGER.debug("placeOrderFIX has finished");
        }
    }

    @Override
    public void placeOrderCancelRequest(PlaceMessageRequest request, StreamObserver<PlaceMessageResponse> responseObserver) {
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("placeOrderCancelRequest request: " + shortDebugString(request));
            }
            placeMessageFieldRule(request, responseObserver, "OrderCancelRequest", request.getMessage().getFieldsMap().get("ClOrdID").getSimpleValue(),
                    ImmutableMap.of("ExecutionReport", CheckMetadata.passOn("ClOrdID"),
                            "OrderCancelReject", CheckMetadata.failOn("ClOrdID"),
                            "BusinessMessageReject", CheckMetadata.failOn("BusinessRejectRefID")), "placeOrderCancelRequest");
        } catch (RuntimeException | IOException e) {
            LOGGER.error("Failed to place an OrderCancelRequest. Message = {}", request.getMessage(), e);
            sendErrorResponse(responseObserver, "Failed to place an OrderCancelRequest. Error: " + e.getMessage());
        } finally {
            LOGGER.debug("placeOrderCancelRequest has finished");
        }
    }

    @Override
    public void placeOrderCancelReplaceRequest(PlaceMessageRequest request, StreamObserver<PlaceMessageResponse> responseObserver) {
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("placeOrderCancelReplaceRequest request: " + shortDebugString(request));
            }
            placeMessageFieldRule(request, responseObserver, "OrderCancelReplaceRequest", request.getMessage().getFieldsMap().get("ClOrdID").getSimpleValue(),
                    ImmutableMap.of("ExecutionReport", CheckMetadata.passOn("ClOrdID"),
                            "OrderCancelReject", CheckMetadata.failOn("ClOrdID"),
                            "BusinessMessageReject", CheckMetadata.failOn("BusinessRejectRefID")), "placeOrderCancelReplaceRequest");
        } catch (RuntimeException | IOException e) {
            LOGGER.error("Failed to place an OrderCancelReplaceRequest. Message = {}", request.getMessage(), e);
            sendErrorResponse(responseObserver, "Failed to place an OrderCancelReplaceRequest. Error: " + e.getMessage());
        } finally {
            LOGGER.debug("placeOrderCancelReplaceRequest has finished");
        }
    }

    @Override
    public void sendRawMessage(SendRawMessageRequest request, StreamObserver<SendRawMessageResponse> responseObserver) {
        long startPlaceMessage = System.currentTimeMillis();
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Sending  message request: " + shortDebugString(request));
            }

            String actName = "sendRawMessage";
            // FIXME store parent with fail in case of children fail
            EventID parentId = createAndStoreParentEvent(request, actName, PASSED);

            Checkpoint checkpoint = registerCheckPoint(parentId);

            if (Context.current().isCancelled()) {
                LOGGER.warn("'{}' request cancelled by client", actName);
                sendRawMessageErrorResponse(responseObserver, "Request has been cancelled by client");
                return;
            }

            try {
                sendRawMessage(request.getRaw(), parentId);
            } catch (Exception ex) {
                createAndStoreErrorEvent("sendMessage", ex.getMessage(), Instant.now(), parentId);
                throw ex;
            }

            SendRawMessageResponse response = SendRawMessageResponse.newBuilder()
                .setStatus(RequestStatus.newBuilder().setStatus(SUCCESS).build())
                .setCheckpointId(checkpoint)
                .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            LOGGER.error("Failed to send a message. Message = {}", request.getRaw(), e);
            sendRawMessageErrorResponse(responseObserver, "Send message failed. Error: " + e.getMessage());
        } finally {
            LOGGER.debug("Sending the message has been finished in {}", System.currentTimeMillis() - startPlaceMessage);
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
                sendMessage(request.getMessage(), parentId);
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
        } catch (RuntimeException | IOException e) {
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
        } catch (RuntimeException | IOException e) {
            LOGGER.error("Failed to place a QuoteCancel. Message = {}", request.getMessage(), e);
            sendErrorResponse(responseObserver, "Failed to place a QuoteCancel. Error: " + e.getMessage());
        } finally {
            LOGGER.debug("placeQuoteCancelFIX has finished");
        }
    }

    @Override
    public void placeSecurityStatusRequest(PlaceMessageRequest request, StreamObserver<PlaceMessageResponse> responseObserver) {
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("placeSecurityStatusRequest: " + shortDebugString(request));
            }
            placeMessageFieldRule(request, responseObserver, "SecurityStatusRequest", request.getMessage().getFieldsMap().get("SecurityID").getSimpleValue(),
                    ImmutableMap.of("SecurityStatus", CheckMetadata.passOn("SecurityID")), "placeSecurityStatusRequest");
        } catch (RuntimeException | IOException e) {
            LOGGER.error("Failed to place an order. Message = {}", request.getMessage(), e);
            sendErrorResponse(responseObserver, "Failed to place SecurityStatusRequest. Error: " + e.getMessage());
        } finally {
            LOGGER.debug("placeSecurityStatusRequest has finished");
        }
    }

    @Override
    public void placeQuoteRequestFIX(PlaceMessageRequest request, StreamObserver<PlaceMessageResponse> responseObserver) {
        try {
            LOGGER.debug("placeQuoteRequestFIX request: {}", request);
            placeMessageFieldRule(request, responseObserver, "QuoteRequest", request.getMessage().getFieldsMap().get("QuoteReqID").getSimpleValue(),
                    ImmutableMap.of("QuoteStatusReport", CheckMetadata.passOn("QuoteReqID")), "placeQuoteRequestFIX");
        } catch (RuntimeException | IOException e) {
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
        } catch (RuntimeException | IOException e) {
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
        } catch (RuntimeException | IOException e) {
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
     * @param expectedMessages       mapping between response and the event status that should be applied in that case
     * @param noResponseBodySupplier supplier for {@link IBodyData} that will be added to the event in case there is not response received
     * @param receiver               supplier for the {@link AbstractMessageReceiver} that will await for the required message
     */
    private void placeMessage(
            PlaceMessageRequest request, StreamObserver<PlaceMessageResponse> responseObserver,
            String expectedRequestType, Map<String, CheckMetadata> expectedMessages, String actName,
            NoResponseBodySupplier noResponseBodySupplier, ReceiverSupplier receiver
    ) throws IOException {

        long startPlaceMessage = System.currentTimeMillis();
        Message message = request.getMessage();
        checkRequestMessageType(expectedRequestType, message.getMetadata());
        ConnectionID requestConnId = message.getMetadata().getId().getConnectionId();

        EventID parentId = createAndStoreParentEvent(request, actName, PASSED);

        Checkpoint checkpoint = registerCheckPoint(parentId);

        MessageResponseMonitor monitor = new MessageResponseMonitor();
        try (AbstractMessageReceiver messageReceiver = receiver.create(monitor, new ReceiverContext(requestConnId))) {
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

    private void placeMessageFieldRule(
            PlaceMessageRequest request, StreamObserver<PlaceMessageResponse> responseObserver,
            String expectedRequestType, String expectedFieldValue, Map<String, CheckMetadata> expectedMessages, String actName
    ) throws IOException {
        placeMessage(request, responseObserver, expectedRequestType, expectedMessages, actName,
                () -> Collections.singletonList(createNoResponseBody(expectedMessages, expectedFieldValue)), (monitor, context) -> {
                    Map<String, String> msgTypeToFieldName = expectedMessages.entrySet().stream()
                            .collect(toUnmodifiableMap(Entry::getKey, value -> value.getValue().getFieldName()));
                    CheckRule checkRule = new FieldCheckRule(expectedFieldValue, msgTypeToFieldName, context.getConnectionID());
                    return new MessageReceiver(subscriptionManager, monitor, checkRule, INCOMING);
                });
    }

    private EventID createAndStoreParentEvent(PlaceMessageRequestOrBuilder request, String actName, Status status) throws IOException {
        long startTime = System.currentTimeMillis();

        Event event = start()
                .name(actName + ' ' + request.getMessage().getMetadata().getId().getConnectionId().getSessionAlias())
                .description(request.getDescription())
                .type(actName)
                .status(status)
                .endTimestamp(); // FIXME set properly as is in the last child

        com.exactpro.th2.common.grpc.Event protoEvent = event.toProto(request.getParentEventId());
        //FIXME process response
        try {
            eventBatchMessageRouter.send(EventBatch.newBuilder().addEvents(event.toProto(request.getParentEventId())).build(), "publish", "event");
            LOGGER.debug("createAndStoreParentEvent for {} in {} ms", actName, System.currentTimeMillis() - startTime);
            return protoEvent.getId();
        } catch (IOException e) {
            throw new RuntimeException("Can not send event = " + protoEvent.getId().getId(), e);
        }
    }

    private EventID createAndStoreParentEvent(SendRawMessageRequest request, String actName, Status status) throws IOException {
        long startTime = System.currentTimeMillis();

        Event event = start()
            .name(actName + ' ' + request.getRaw().getMetadata().getId().getConnectionId().getSessionAlias())
            .type(actName)
            .status(status)
            .endTimestamp(); // FIXME set properly as is in the last child

        com.exactpro.th2.common.grpc.Event protoEvent = event.toProto(request.getParentEventId());
        //FIXME process response
        try {
            eventBatchMessageRouter.send(EventBatch.newBuilder().addEvents(event.toProto(request.getParentEventId())).build(), "publish", "event");
            LOGGER.debug("createAndStoreParentEvent for {} in {} ms", actName, System.currentTimeMillis() - startTime);
            return protoEvent.getId();
        } catch (IOException e) {
            throw new RuntimeException("Can not send event = " + protoEvent.getId().getId(), e);
        }
    }

    private void createAndStoreNoResponseEvent(
            String actName, NoResponseBodySupplier noResponseBodySupplier,
            Instant start,
            EventID parentEventId, Collection<MessageID> messageIDList
    ) throws IOException {

        Event errorEvent = Event.from(start)
                .endTimestamp()
                .name(format("Internal %s error", actName))
                .type("No response found by target keys.")
                .status(FAILED);
        for (IBodyData data : noResponseBodySupplier.createNoResponseBody()) {
            errorEvent.bodyData(data);
        }
        for (MessageID msgID : messageIDList) {
            errorEvent.messageID(msgID);
        }
        storeEvent(errorEvent.toProto(parentEventId));
    }

    private IBodyData createNoResponseBody(Map<String, CheckMetadata> expectedMessages, String fieldValue) {
        TreeTableBuilder treeTableBuilder = new TreeTableBuilder();
        CollectionBuilder passedOn = new CollectionBuilder();
        CollectionBuilder failedOn = new CollectionBuilder();
        for (Map.Entry<String, CheckMetadata> entry : expectedMessages.entrySet()) {
            if (entry.getValue().eventStatus == PASSED) {
                passedOn.row(entry.getKey(), new CollectionBuilder().row(entry.getValue().fieldName, new RowBuilder().column(new MessageTableColumn(fieldValue)).build()).build());
            } else {
                failedOn.row(entry.getKey(), new CollectionBuilder().row(entry.getValue().fieldName, new RowBuilder().column(new MessageTableColumn(fieldValue)).build()).build());
            }
        }
        treeTableBuilder.row("PASSED on:", passedOn.build());
        treeTableBuilder.row("FAILED on:", failedOn.build());

        return treeTableBuilder.build();
    }

    private void processResponseMessage(
            String actName,
            StreamObserver<PlaceMessageResponse> responseObserver,
            Checkpoint checkpoint,
            EventID parentEventId,
            MessageHolder responseMessage,
            Map<String, CheckMetadata> expectedMessages,
            long timeout,
            Collection<MessageID> messageIDList,
            NoResponseBodySupplier noResponseBodySupplier
    ) throws IOException {
        long startTime = System.currentTimeMillis();
        String message = format("No response message has been received in '%s' ms", timeout);
        if (responseMessage == null) {
            createAndStoreNoResponseEvent(actName, noResponseBodySupplier,
                    Instant.now(),
                    parentEventId, messageIDList);
            sendErrorResponse(responseObserver, message);
        } else {
            String messageType = responseMessage.getMessageType();
            CheckMetadata checkMetadata = expectedMessages.get(messageType);
            TreeTable parametersTable = getTreeTable(responseMessage);
            storeEvent(Event.start()
                    .name(format("Received '%s' response message", messageType))
                    .type("message")
                    .status(checkMetadata.getEventStatus())
                    .bodyData(parametersTable)
                    .messageID(responseMessage.getId())
                    .toProto(parentEventId)
            );
            PlaceMessageResponse response = PlaceMessageResponse.newBuilder()
                    .setResponseMessage(responseMessage.getProtoMessage())
                    .setStatus(RequestStatus.newBuilder().setStatus(checkMetadata.getRequestStatus()).build())
                    .setCheckpointId(checkpoint)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
        LOGGER.debug("processResponseMessage in {} ms", System.currentTimeMillis() - startTime);
    }

    private boolean isSendPlaceMessage(
            Message message, StreamObserver<PlaceMessageResponse> responseObserver,
            EventID parentEventId
    ) {
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
            ParsedMessage parsedMessage = MessageUtilsKt.toTransportBuilder(message)
                    .setEventId(EventUtilsKt.toTransport(parentEventId))
                    .build();
            //May be use in future for filtering
            //request.getConnectionId().getSessionAlias();
            MessageID messageID = message.getMetadata().getId();
            messageRouter.send(toBatch(toGroup(parsedMessage), messageID.getBookName(), messageID.getConnectionId().getSessionGroup()));
            //TODO remove after solving issue TH2-217
            //TODO process response
            EventBatch eventBatch = EventBatch.newBuilder()
                    .addEvents(createSendMessageEvent(parsedMessage, parentEventId))
                    .build();
            eventBatchMessageRouter.send(eventBatch, "publish", "event");
        } finally {
            LOGGER.debug("Sending the message ended");
        }
    }

    private void sendRawMessage(com.exactpro.th2.common.grpc.RawMessage rawMessage, EventID parentEventId) throws IOException {
        try {
            LOGGER.debug("Sending the message started");
            RawMessageMetadata metadata = rawMessage.getMetadata();
            RawMessage transportRawMessage = RawMessage.builder()
                .setId(toTransport(metadata.getId()))
                .setProtocol(metadata.getProtocol())
                .setMetadata(metadata.getPropertiesMap())
                .setEventId(EventUtilsKt.toTransport(parentEventId))
                .setBody(rawMessage.getBody().toByteArray())
                .build();
            MessageID messageID = metadata.getId();
            messageRouter.send(toBatch(toGroup(transportRawMessage), messageID.getBookName(), messageID.getConnectionId().getSessionGroup()), SEND_RAW_QUEUE_ATTRIBUTE);

            EventBatch eventBatch = EventBatch.newBuilder()
                .addEvents(createSendRawMessageEvent(transportRawMessage, parentEventId))
                .build();
            eventBatchMessageRouter.send(eventBatch, "publish", "event");
        } finally {
            LOGGER.debug("Sending the message ended");
        }
    }

    private com.exactpro.th2.common.grpc.Event createSendMessageEvent(ParsedMessage message, EventID parentEventId) throws IOException {
        Event event = start()
                .name("Send '" + message.getType() + "' message to connectivity");
        TreeTable parametersTable = toTreeTable(message);
        event.status(Status.PASSED);
        event.bodyData(parametersTable);
        event.type("Outgoing message");
        return event.toProto(parentEventId);
    }

    private com.exactpro.th2.common.grpc.Event createSendRawMessageEvent(RawMessage message, EventID parentEventId) throws IOException {
        Event event = start()
            .name("Sent raw message to connectivity");
        com.exactpro.th2.common.event.bean.Message messageBean = EventUtils.createMessageBean(ByteBufUtil.hexDump(message.getBody()));
        event.status(Status.PASSED);
        event.bodyData(messageBean);
        event.type("Outgoing message");
        return event.toProto(parentEventId);
    }

    private void createAndStoreErrorEvent(String actName, String message, Instant start, EventID parentEventId) throws IOException {
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

    private void sendErrorResponse(
            StreamObserver<PlaceMessageResponse> responseObserver,
            String message
    ) {
        responseObserver.onNext(PlaceMessageResponse.newBuilder()
                .setStatus(RequestStatus.newBuilder()
                        .setStatus(ERROR)
                        .setMessage(message)
                        .build())
                .build());
        responseObserver.onCompleted();
        LOGGER.debug("Error response : {}", message);
    }

    private void sendMessageErrorResponse(
            StreamObserver<SendMessageResponse> responseObserver,
            String message
    ) {
        responseObserver.onNext(SendMessageResponse.newBuilder()
                .setStatus(RequestStatus.newBuilder()
                        .setStatus(ERROR)
                        .setMessage(message)
                        .build())
                .build());
        responseObserver.onCompleted();
        LOGGER.debug("Error response : {}", message);
    }

    private void sendRawMessageErrorResponse(
        StreamObserver<SendRawMessageResponse> responseObserver,
        String message
    ) {
        responseObserver.onNext(SendRawMessageResponse.newBuilder()
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
        CheckpointResponse response = check1Service.createCheckpoint(CheckpointRequest.newBuilder()
                .setParentEventId(parentEventId)
                .build());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Registering the checkpoint ended. Response " + shortDebugString(response));
        }
        return response.getCheckpoint();
    }

    private static class CheckMetadata {
        private final Status eventStatus;
        private final RequestStatus.Status requestStatus;
        private final String fieldName;

        private CheckMetadata(String fieldName, Status eventStatus) {
            this.eventStatus = requireNonNull(eventStatus, "Event status can't be null");
            this.fieldName = requireNonNull(fieldName, "Field name can't be null");

            switch (eventStatus) {
                case PASSED:
                    requestStatus = SUCCESS;
                    break;
                case FAILED:
                    requestStatus = ERROR;
                    break;
                default:
                    throw new IllegalArgumentException("Event status '" + eventStatus + "' can't be convert to request status");
            }
        }

        public static CheckMetadata passOn(String fieldName) {
            return new CheckMetadata(fieldName, PASSED);
        }

        public static CheckMetadata failOn(String fieldName) {
            return new CheckMetadata(fieldName, FAILED);
        }

        public Status getEventStatus() {
            return eventStatus;
        }

        public RequestStatus.Status getRequestStatus() {
            return requestStatus;
        }

        public String getFieldName() {
            return fieldName;
        }
    }

    @FunctionalInterface
    private interface ReceiverSupplier {
        AbstractMessageReceiver create(ResponseMonitor monitor, ReceiverContext context);
    }

    @FunctionalInterface
    private interface NoResponseBodySupplier {
        Collection<IBodyData> createNoResponseBody();
    }

    private static class ReceiverContext {
        private final ConnectionID connectionID;

        public ReceiverContext(ConnectionID connectionID) {
            this.connectionID = requireNonNull(connectionID, "'Connection id' parameter");
        }

        public ConnectionID getConnectionID() {
            return connectionID;
        }
    }
}
