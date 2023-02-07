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

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.act.EventUtils.MessageTableColumn;
import com.exactpro.th2.act.grpc.ActGrpc.ActImplBase;
import com.exactpro.th2.act.grpc.MultiSendRequest;
import com.exactpro.th2.act.grpc.PlaceMessageRequest;
import com.exactpro.th2.act.grpc.PlaceMessageRequestOrBuilder;
import com.exactpro.th2.act.grpc.PlaceMessageResponse;
import com.exactpro.th2.act.grpc.SendMessageResponse;
import com.exactpro.th2.act.impl.MessageResponseMonitor;
import com.exactpro.th2.act.rules.FieldCheckRule;
import com.exactpro.th2.check1.grpc.Check1Service;
import com.exactpro.th2.check1.grpc.CheckpointRequest;
import com.exactpro.th2.check1.grpc.CheckpointResponse;
import com.exactpro.th2.common.event.Event;
import com.exactpro.th2.common.event.Event.Status;
import com.exactpro.th2.common.event.IBodyData;
import com.exactpro.th2.common.event.bean.TreeTable;
import com.exactpro.th2.common.event.bean.builder.CollectionBuilder;
import com.exactpro.th2.common.event.bean.builder.MessageBuilder;
import com.exactpro.th2.common.event.bean.builder.RowBuilder;
import com.exactpro.th2.common.event.bean.builder.TreeTableBuilder;
import com.exactpro.th2.common.grpc.AnyMessage;
import com.exactpro.th2.common.grpc.Checkpoint;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.EventStatus;
import com.exactpro.th2.common.grpc.ListValueOrBuilder;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.grpc.MessageGroup;
import com.exactpro.th2.common.grpc.MessageGroupBatch;
import com.exactpro.th2.common.grpc.MessageGroupBatch.Builder;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.MessageMetadata;
import com.exactpro.th2.common.grpc.MessageOrBuilder;
import com.exactpro.th2.common.grpc.RequestStatus;
import com.exactpro.th2.common.grpc.Value;
import com.exactpro.th2.common.schema.message.MessageListener;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.utils.event.SingleEventBatcher;
import com.exactpro.th2.common.utils.message.MessageBatcher;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;

import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.stub.StreamObserver;

public class ActHandler extends ActImplBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ActHandler.class);
    private static final int CORE_POOL_SIZE = 10;
    private static final String MATCH_FIELD_NAME = "match";

    private final Check1Service check1Service;
    private final MessageRouter<EventBatch> eventBatchMessageRouter;
    private final MessageRouter<MessageBatch> messageRouter;
    private final MessageRouter<MessageGroupBatch> groupBatchMessageRouter;
    private final SubscriptionManager subscriptionManager;
    private final Configuration config;
    private final SingleEventBatcher eventBatcher;
    private final MessageBatcher messageBatcher;

    ActHandler(
            MessageRouter<MessageBatch> router,
            MessageRouter<MessageGroupBatch> groupBatchMessageRouter,
            SubscriptionManager subscriptionManager,
            MessageRouter<EventBatch> eventBatchRouter,
            Check1Service check1Service,
            Configuration configuration
    ) {
        this.messageRouter = requireNonNull(router, "'Router' parameter");
        this.eventBatchMessageRouter = requireNonNull(eventBatchRouter, "'Event batch router' parameter");
        this.check1Service = requireNonNull(check1Service, "'check1 service' parameter");
        this.subscriptionManager = requireNonNull(subscriptionManager, "'Callback list' parameter");
        this.config = configuration;
        this.groupBatchMessageRouter = groupBatchMessageRouter;
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(CORE_POOL_SIZE);

        eventBatcher = new SingleEventBatcher(config.getMaxBatchSizeCount(), config.getMaxFlushTime(), executor, eventBatch -> {
            try {
                eventBatchMessageRouter.sendAll(eventBatch, "event");
            } catch (IOException e) {
                LOGGER.error("Failed to send messages", e);
            }
        });

        messageBatcher = new MessageBatcher(config.getMaxBatchSizeCount(), config.getMaxFlushTime(),
                message -> message.getMetadata().getId().getConnectionId().getSessionAlias(), executor,
                throwable -> LOGGER.error("Failed to send messages", throwable),
                messageGroupBatch -> groupBatchMessageRouter.sendAll(messageGroupBatch, "parsed", "publish"));
    }

    private static String toDebugMessage(com.google.protobuf.MessageOrBuilder messageOrBuilder) throws InvalidProtocolBufferException {
        return JsonFormat.printer().omittingInsignificantWhitespace().print(messageOrBuilder);
    }

    private long getTimeout(Deadline deadline) {
        return deadline == null ? config.getResponseTimeout() : deadline.timeRemaining(MILLISECONDS);
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
    public void placeOrderCancelRequest(PlaceMessageRequest request, StreamObserver<PlaceMessageResponse> responseObserver) {
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("placeOrderCancelRequest request: " + shortDebugString(request));
            }
            placeMessageFieldRule(request, responseObserver, "OrderCancelRequest", request.getMessage().getFieldsMap().get("ClOrdID").getSimpleValue(),
                    ImmutableMap.of("ExecutionReport", CheckMetadata.passOn("ClOrdID"), 
                    "OrderCancelReject", CheckMetadata.failOn("ClOrdID"), 
                    "BusinessMessageReject", CheckMetadata.failOn("BusinessRejectRefID")), "placeOrderCancelRequest");
        } catch (RuntimeException | JsonProcessingException e) {
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
        } catch (RuntimeException | JsonProcessingException e) {
            LOGGER.error("Failed to place an OrderCancelReplaceRequest. Message = {}", request.getMessage(), e);
            sendErrorResponse(responseObserver, "Failed to place an OrderCancelReplaceRequest. Error: " + e.getMessage());
        } finally {
            LOGGER.debug("placeOrderCancelReplaceRequest has finished");
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

    private com.exactpro.th2.common.grpc.Event createParentEvent(MultiSendRequest request, String actName, long actionStartTimestamp, EventID id) {
        return com.exactpro.th2.common.grpc.Event.newBuilder()
                .setStartTimestamp(Timestamps.fromMillis(actionStartTimestamp))
                .setParentId(request.getParentEventId())
                .setId(id)
                .setName(format("%s for %d messages ", actName, request.getMessagesCount()))
                .setType("act_" + actName)
                .setEndTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                .build();
    }

    private void sendMessagesIgnoringOrder(MultiSendRequest request, EventID parentId) throws JsonProcessingException {
        for (Message message : request.getMessagesList()) {
            com.exactpro.th2.common.grpc.Event eventToStore = createSendMessageEvent(message, parentId);
            eventBatcher.onEvent(eventToStore);
            messageBatcher.onMessage(Message.newBuilder(message).setParentEventId(eventToStore.getId()));
        }
    }

    private void sendMessagesWithOrder(MultiSendRequest request, EventID parentId, long stopTime) throws IOException, InterruptedException {
        Collection<Message> messagesToSend = new ArrayList<>();
        String lastAlias = "";
        for (Message message : request.getMessagesList()) {
            String currentAlias = message.getMetadata().getId().getConnectionId().getSessionAlias();
            if (!lastAlias.isEmpty() && !lastAlias.equals(currentAlias)) {
                sendGroupBatcWithEcho(messagesToSend, stopTime);
                messagesToSend.clear();
            }
            if (System.currentTimeMillis() >= stopTime) {
                throw new InterruptedException("Sending interrupted by timeout at message:\n " + shortDebugString(message) + '\n'
                        + "Possible problems and solutions:\n"
                        + "1) Check links and filters on pins for this session alias.\n act->codec(encoder_in)->conn(to_send) and conn(second)->codec(decoder_in)->act(from_codec)\n"
                        + "2) Check codec logs/events. Maybe this message does not corresponds your dictionary.\n"
                        + "3) Check conn logs/events. Maybe conn can't send this message e.g. session is closed.");
            }
            com.exactpro.th2.common.grpc.Event eventToStore = createSendMessageEvent(message, parentId);
            eventBatcher.onEvent(eventToStore);
            messagesToSend.add(Message.newBuilder(message).setParentEventId(eventToStore.getId()).build());
            lastAlias = currentAlias;
        }
        sendGroupBatcWithEcho(messagesToSend, stopTime);
        messagesToSend.clear();
    }

    private void sendGroupBatcWithEcho(Iterable<? extends Message> messagesToSend, long stopTime) throws IOException {
        Lock responseLock = new ReentrantLock();
        Condition condition = responseLock.newCondition();
        Collection<String> sentIDS = new HashSet<>();
        MessageListener<MessageBatch> listener = new MessageListener<>() {
            @Override
            public void handler(String consumerTag, MessageBatch message) {
                responseLock.lock();
                try {
                    for (Message msg : message.getMessagesList()) {
                        sentIDS.remove(msg.getParentEventId().getId());
                        if (sentIDS.isEmpty()) {
                            condition.signalAll();
                        }
                    }
                } finally {
                    responseLock.unlock();
                }
            }
        };
        Builder batch = MessageGroupBatch.newBuilder();
        for (Message message : messagesToSend) {
            batch.addGroups(MessageGroup.newBuilder().addMessages(AnyMessage.newBuilder().setMessage(message).build()).build());
            sentIDS.add(message.getParentEventId().getId());
        }
        try {
            responseLock.lock();
            subscriptionManager.register(Direction.SECOND, listener);
            groupBatchMessageRouter.sendAll(batch.build(), "parsed", "publish");
            condition.await(stopTime - System.currentTimeMillis(), MILLISECONDS);
        } catch (IOException | InterruptedException e) {
            LOGGER.error("Sending failed:", e.getCause());
        } finally {
            responseLock.unlock();
            subscriptionManager.unregister(Direction.SECOND, listener);
            if (sentIDS.isEmpty()) {
                LOGGER.debug("Messages for alias {} are sent successfully.", messagesToSend.iterator().next().getMetadata().getId().getConnectionId().getSessionAlias());
            } else {
                LOGGER.error("Can't find echo for messages {} ", sentIDS);
                for (String eventID : sentIDS) {
                    eventBatcher.onEvent(createUnableToFindEcho(EventID.newBuilder().setId(eventID).build()));
                }
            }
        }
    }

    private com.exactpro.th2.common.grpc.Event createUnableToFindEcho(EventID parent) {
        return com.exactpro.th2.common.grpc.Event.newBuilder()
                .setType("actError")
                .setId(EventID.newBuilder().setId(UUID.randomUUID().toString()).build())
                .setParentId(parent)
                .setStatus(EventStatus.FAILED)
                .setStartTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                .setName("Can't find echo. This message may not have been sent.")
                .build();
    }

    private com.exactpro.th2.common.grpc.Event createUnableToSendEvent(EventID parent, String reason) {
        return com.exactpro.th2.common.grpc.Event.newBuilder()
                .setType("actError")
                .setId(EventID.newBuilder().setId(UUID.randomUUID().toString()).build())
                .setParentId(parent)
                .setStatus(EventStatus.FAILED)
                .setStartTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                .setName("Unable to send messages")
                .setBody(ByteString.copyFromUtf8(String.format("[{\"data\":\"%s\", \"type\":\"message\" } ]", reason.replace("\n", "\\n").replace('"', '\''))))
                .build();
    }

    @Override
    public void multiSendMessage(MultiSendRequest request, StreamObserver<SendMessageResponse> responseObserver) {
        long actionStartTimestamp = System.currentTimeMillis();
        long stopTime = actionStartTimestamp + getTimeout(Context.current().getDeadline());
        EventID parentId = EventUtils.generateID();
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Sending  messages request: " + shortDebugString(request));
            }
            Checkpoint checkpoint = registerCheckPoint(parentId);
            String actName = "multiSendMessage";
            eventBatcher.onEvent(createParentEvent(request, actName, actionStartTimestamp, parentId));
            if (request.getSkipOrder()) {
                sendMessagesIgnoringOrder(request, parentId);
            } else {
                sendMessagesWithOrder(request, parentId, stopTime);
            }

            if (Context.current().isCancelled()) {
                LOGGER.warn("'{}' request cancelled by client", actName);
                sendMessageErrorResponse(responseObserver, "Request has been cancelled by client");
            }

            SendMessageResponse response = SendMessageResponse.newBuilder()
                    .setStatus(RequestStatus.newBuilder().setStatus(SUCCESS).build())
                    .setCheckpointId(checkpoint)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (RuntimeException | IOException | InterruptedException e) {
            LOGGER.error("Failed to send {} messages.", request.getMessagesCount(), e.getMessage());
            eventBatcher.onEvent(createUnableToSendEvent(parentId, e.getMessage()));
            sendMessageErrorResponse(responseObserver, "Send messages failed. Error: " + e.getMessage());
        } finally {
            LOGGER.info("Sending {} messages has been finished in {}ms.", request.getMessagesCount(), System.currentTimeMillis() - actionStartTimestamp);
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
    public void placeSecurityStatusRequest(PlaceMessageRequest request, StreamObserver<PlaceMessageResponse> responseObserver) {
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("placeSecurityStatusRequest: " + shortDebugString(request));
            }
            placeMessageFieldRule(request, responseObserver, "SecurityStatusRequest", request.getMessage().getFieldsMap().get("SecurityID").getSimpleValue(),
                    ImmutableMap.of("SecurityStatus", CheckMetadata.passOn("SecurityID")), "placeSecurityStatusRequest");
        } catch (RuntimeException | JsonProcessingException e) {
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
                () -> Collections.singletonList(createNoResponseBody(expectedMessages, expectedFieldValue)), (monitor, context) -> {
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

        com.exactpro.th2.common.grpc.Event protoEvent = event.toProto(request.getParentEventId());
        //FIXME process response
        //eventBatchMessageRouter.send(EventBatch.newBuilder().addEvents(event.toProto(request.getParentEventId())).build(), "publish", "event");
        eventBatcher.onEvent(protoEvent);
        LOGGER.debug("createAndStoreParentEvent for {} in {} ms", actName, System.currentTimeMillis() - startTime);
        return protoEvent.getId();
    }

    private void createAndStoreNoResponseEvent(String actName, NoResponseBodySupplier noResponseBodySupplier,
            Instant start,
            EventID parentEventId, Collection<MessageID> messageIDList) throws JsonProcessingException {

        Event errorEvent = Event.from(start)
                .endTimestamp()
                .name("No response found")
                .type(actName + " error")
                .status(FAILED);
        com.exactpro.th2.common.event.bean.Message tableExpl = new com.exactpro.th2.common.event.bean.Message();
        tableExpl.setType("message");
        tableExpl.setData("Unable to find response messages using this filters:");
        errorEvent.bodyData(tableExpl);
        for (IBodyData data : noResponseBodySupplier.createNoResponseBody()) {
            errorEvent.bodyData(data);
        }
        for (MessageID msgID : messageIDList) {
            errorEvent.messageID(msgID);
        }
        com.exactpro.th2.common.event.bean.Message attachedExpl = new com.exactpro.th2.common.event.bean.Message();
        attachedExpl.setType("message");
        attachedExpl.setData("All messages processed by the act are attached to this event.");
        errorEvent.bodyData(attachedExpl);
        storeEvent(errorEvent.toProtoEvent(parentEventId.getId()));
    }

    private IBodyData createNoResponseBody(Map<String, CheckMetadata> expectedMessages, String fieldValue) {
        TreeTableBuilder treeTableBuilder = new TreeTableBuilder("act expected responses");
        CollectionBuilder passedOn = new CollectionBuilder();
        CollectionBuilder failedOn = new CollectionBuilder();
        for (Entry<String, CheckMetadata> entry : expectedMessages.entrySet()) {
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
            storeEvent(start()
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

            messageRouter.send(MessageBatch.newBuilder()
                    .addMessages(Message.newBuilder(message)
                            .setParentEventId(parentEventId)
                            .build())
                    .build());
            //messageBatcher.onMessage(Message.newBuilder(message).setParentEventId(parentEventId));

            //            EventBatch eventBatch = EventBatch.newBuilder()
            //                    .addEvents(createSendMessageEvent(message, parentEventId))
            //                    .build();
            //            eventBatchMessageRouter.send(eventBatch, "publish", "event");
            eventBatcher.onEvent(createSendMessageEvent(message, parentEventId));
        } finally {
            LOGGER.debug("Sending the message ended");
        }
    }

    private Message backwardCompatibilityConnectionId(PlaceMessageRequest request) {
        ConnectionID connectionId = request.getMessage().getMetadata().getId().getConnectionId();
        if (!connectionId.getSessionAlias().isEmpty()) {
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

    private com.exactpro.th2.common.grpc.Event createSendMessageEvent(Message message, EventID parentEventId) throws JsonProcessingException {
        Event event = start()
                .name("Send '" + message.getMetadata().getMessageType() + "' message to codec encoder(alias:" + message.getMetadata().getId().getConnectionId().getSessionAlias() + ')');
        TreeTable parametersTable = EventUtils.toTreeTable(message);
        event.status(PASSED);
        event.bodyData(parametersTable);
        event.type("Outgoing message");
        return event.toProto(parentEventId);
    }

    private void createAndStoreErrorEvent(String actName, String message, Instant start, EventID parentEventId) throws JsonProcessingException {
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
            //eventBatchMessageRouter.send(EventBatch.newBuilder().addEvents(eventRequest).build(), "publish", "event");
            eventBatcher.onEvent(eventRequest);
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
        CheckpointResponse response = check1Service.createCheckpoint(CheckpointRequest.newBuilder()
                .setParentEventId(parentEventId)
                .build());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Registering the checkpoint ended. Response " + shortDebugString(response));
        }
        return response.getCheckpoint();
    }

    @FunctionalInterface
    private interface ReceiverSupplier {
        AbstractMessageReceiver create(ResponseMonitor monitor, ReceiverContext context);
    }

    @FunctionalInterface
    private interface NoResponseBodySupplier {
        Collection<IBodyData> createNoResponseBody();
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

    private static class ReceiverContext {
        private final ConnectionID connectionID;
        private final EventID parentId;

        public ReceiverContext(ConnectionID connectionID, EventID parentId) {
            this.connectionID = requireNonNull(connectionID, "'Connection id' parameter");
            this.parentId = requireNonNull(parentId, "'Parent id' parameter");
        }

        public ConnectionID getConnectionID() {
            return connectionID;
        }

        public EventID getParentId() {
            return parentId;
        }
    }
}
