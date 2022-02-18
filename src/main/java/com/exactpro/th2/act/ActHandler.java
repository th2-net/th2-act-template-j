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

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.act.AbstractMessageReceiver.State;
import com.exactpro.th2.act.ResponseMapper.FieldPath;
import com.exactpro.th2.act.ResponseMapper.ResponseStatus;
import com.exactpro.th2.act.ResponseMapper.ValueMatcher;
import com.exactpro.th2.act.grpc.ActGrpc.ActImplBase;
import com.exactpro.th2.act.grpc.PlaceMessageRequest;
import com.exactpro.th2.act.grpc.PlaceMessageRequestOrBuilder;
import com.exactpro.th2.act.grpc.PlaceMessageResponse;
import com.exactpro.th2.act.grpc.SendMessageResponse;
import com.exactpro.th2.act.impl.MessageResponseMonitor;
import com.exactpro.th2.act.rules.EventIDCheckRule;
import com.exactpro.th2.act.rules.FieldsCheckRule;
import com.exactpro.th2.check1.grpc.Check1Service;
import com.exactpro.th2.check1.grpc.CheckpointRequest;
import com.exactpro.th2.check1.grpc.CheckpointResponse;
import com.exactpro.th2.common.event.IBodyData;
import com.exactpro.th2.common.event.bean.TreeTable;
import com.exactpro.th2.common.grpc.Checkpoint;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.MessageMetadata;
import com.exactpro.th2.common.grpc.MessageMetadataOrBuilder;
import com.exactpro.th2.common.grpc.RequestStatus;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.fasterxml.jackson.core.JsonProcessingException;

import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.stub.StreamObserver;

public class ActHandler extends ActImplBase {
    private static final int DEFAULT_RESPONSE_TIMEOUT = 10_000;
    private static final Logger LOGGER = LoggerFactory.getLogger(ActHandler.class);
    private final Check1Service verifierConnector;
    private final MessageRouter<MessageBatch> messageRouter;
    private final SubscriptionManager subscriptionManager;
    private final EventSender eventSender;

    ActHandler(
            MessageRouter<MessageBatch> router,
            SubscriptionManager subscriptionManager,
            MessageRouter<EventBatch> eventBatchRouter,
            Check1Service verifierService
    ) {
        this.messageRouter = requireNonNull(router, "'Router' parameter");
        this.eventSender = new EventSender(requireNonNull(eventBatchRouter, "'Event batch router' parameter"));
        this.verifierConnector = requireNonNull(verifierService, "'Verifier service' parameter");
        this.subscriptionManager = requireNonNull(subscriptionManager, "'Callback list' parameter");
    }

    private static long getTimeout(Deadline deadline) {
        return deadline == null ? DEFAULT_RESPONSE_TIMEOUT : deadline.timeRemaining(MILLISECONDS);
    }

    private static void sendErrorResponse(StreamObserver<PlaceMessageResponse> responseObserver,
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

    private static void sendMessageErrorResponse(StreamObserver<SendMessageResponse> responseObserver,
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

    @Override
    public void placeOrderFIX(PlaceMessageRequest request, StreamObserver<PlaceMessageResponse> responseObserver) {
        String requestMessageType = "NewOrderSingle"; //Set message type can be placed by this method.
        List<String> matchingFieldPath = Arrays.asList("ClOrdID"); //Describe path to field. Number of element in list also should be described.
        String actName = "placeOrderFIX"; //Set name prefix for act root event.
        try {
            String matchingValue = ActUtils.getMatchingValue(request.getMessage(), matchingFieldPath).getSimpleValue();

            List<ResponseMapper> responseMapping = Arrays.asList(
                    new ResponseMapper(ResponseStatus.FAILED, "ExecutionReport", Map.of(
                            new FieldPath("ClOrdID"), ValueMatcher.equal(matchingValue),
                            new FieldPath("ExecType"), ValueMatcher.in("8", "C", "7", "9", "4", "6"))
                    ),
                    new ResponseMapper(ResponseStatus.PASSED, "ExecutionReport", Map.of(
                            new FieldPath("ClOrdID"), ValueMatcher.equal(matchingValue),
                            new FieldPath("ExecType"), ValueMatcher.in("0", "F", "A", "New", "Trade"))
                    ),
                    new ResponseMapper(ResponseStatus.PASSED, "BusinessMessageReject", Map.of(
                            new FieldPath("BusinessRejectRefID"), ValueMatcher.equal(matchingValue))
                    ));

            placeTemplate(request, responseObserver, requestMessageType, actName, responseMapping);
        } catch (FieldNotFoundException e) {
            reportFieldNotFound(request, responseObserver, matchingFieldPath, actName, e);
        }
    }

    private void placeTemplate(PlaceMessageRequestOrBuilder request, StreamObserver<PlaceMessageResponse> responseObserver, String requestMessageType, String actName,
            List<ResponseMapper> responseMapping) {
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("%s request: %s", actName, shortDebugString(request)));
            }

            placeMessage(request, responseObserver, requestMessageType, actName,
                    () -> Collections.singletonList(EventUtils.createNoResponseBody(responseMapping)),
                    (monitor, context) -> {
                        CheckRule echoCheckRule = new EventIDCheckRule(context.getParentId().getId(), context.getConnectionID(), Direction.SECOND);
                        CheckRule responseCheckRule = new FieldsCheckRule(responseMapping, context.getConnectionID());
                        return new EchoCheckMessageReceiver(subscriptionManager, monitor, echoCheckRule, responseCheckRule);
                    });

        } catch (RuntimeException | JsonProcessingException e) {
            LOGGER.error(format("Failed to place %s. Message = %s", requestMessageType, request.getMessage()), e);
            sendErrorResponse(responseObserver, format("Failed to place %s. Error: %s", requestMessageType, e.getMessage()));
        } finally {
            LOGGER.debug(format("%s has finished.", actName));
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
            ConnectionID requestConnId = request.getMessage().getMetadata().getId().getConnectionId(); //Get ConnectionID.
            EventID parentId = eventSender.createAndStoreParentEvent(request.getDescription(), actName, requestConnId.getSessionAlias(), PASSED, request.getParentEventId());

            Checkpoint checkpoint = registerCheckPoint(parentId);

            if (Context.current().isCancelled()) {
                LOGGER.warn("'{}' request cancelled by client", actName);
                sendMessageErrorResponse(responseObserver, "Request has been cancelled by client");
            }

            try {
                sendMessage(request.getMessage(), parentId);
            } catch (Exception ex) {
                eventSender.createAndStoreErrorEvent("sendMessage", ex.getMessage(), Instant.now(), parentId);
                throw ex;
            }

            SendMessageResponse response = SendMessageResponse.newBuilder()
                    .setStatus(RequestStatus.newBuilder().setStatus(SUCCESS).build())
                    .setCheckpointId(checkpoint)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (RuntimeException | IOException e) {
            LOGGER.error("Failed to send a message. Message = {}" + request.getMessage(), e);
            sendMessageErrorResponse(responseObserver, "Send message failed. Error: " + e.getMessage());
        } finally {
            LOGGER.debug("Sending the message has been finished in {}", System.currentTimeMillis() - startPlaceMessage);
        }
    }

    @Override
    public void placeOrderMassCancelRequestFIX(PlaceMessageRequest request, StreamObserver<PlaceMessageResponse> responseObserver) {
        String requestMessageType = "OrderMassCancelRequest"; //Set message type can be placed by this method.
        List<String> matchingFieldPath = Arrays.asList("ClOrdID"); //Describe path to field. Number of element in list also should be described.
        String actName = "placeOrderMassCancelRequestFIX"; //Set name prefix for act root event.
        try {
            String matchingValue = ActUtils.getMatchingValue(request.getMessage(), matchingFieldPath).getSimpleValue();

            List<ResponseMapper> responseMapping = Arrays.asList(
                    new ResponseMapper(ResponseStatus.PASSED, "OrderMassCancelReport", Map.of(
                            new FieldPath("ClOrdID"), ValueMatcher.equal(matchingValue),
                            new FieldPath("MassCancelResponse"), ValueMatcher.equal("7"))
                    ),
                    new ResponseMapper(ResponseStatus.FAILED, "OrderMassCancelReport", Map.of(
                            new FieldPath("ClOrdID"), ValueMatcher.equal(matchingValue),
                            new FieldPath("MassCancelResponse"), ValueMatcher.equal("0"))
                    ));

            placeTemplate(request, responseObserver, requestMessageType, actName, responseMapping);
        } catch (FieldNotFoundException e) {
            reportFieldNotFound(request, responseObserver, matchingFieldPath, actName, e);
        }
    }

    @Override
    public void placeQuoteCancelFIX(PlaceMessageRequest request, StreamObserver<PlaceMessageResponse> responseObserver) {
        String requestMessageType = "QuoteCancel"; //Set message type can be placed by this method.
        List<String> matchingFieldPath = Arrays.asList("QuoteMsgID"); //Describe path to field. Number of element in list also should be described.
        String actName = "placeQuoteCancelFIX"; //Set name prefix for act root event.
        try {
            String matchingValue = ActUtils.getMatchingValue(request.getMessage(), matchingFieldPath).getSimpleValue();

            List<ResponseMapper> responseMapping = Arrays.asList(
                    new ResponseMapper(ResponseStatus.PASSED, "MassQuoteAcknowledgement", Map.of(
                            new FieldPath("QuoteID"), ValueMatcher.equal(matchingValue),
                            new FieldPath("QuoteStatus"), ValueMatcher.equal("4"))
                    ),
                    new ResponseMapper(ResponseStatus.FAILED, "MassQuoteAcknowledgement", Map.of(
                            new FieldPath("QuoteID"), ValueMatcher.equal(matchingValue),
                            new FieldPath("QuoteStatus"), ValueMatcher.equal("5"))
                    ));

            placeTemplate(request, responseObserver, requestMessageType, actName, responseMapping);
        } catch (FieldNotFoundException e) {
            reportFieldNotFound(request, responseObserver, matchingFieldPath, actName, e);
        }
    }

    @Override
    public void placeQuoteRequestFIX(PlaceMessageRequest request, StreamObserver<PlaceMessageResponse> responseObserver) {
        String requestMessageType = "QuoteRequest"; //Set message type can be placed by this method.
        List<String> matchingFieldPath = Arrays.asList("QuoteReqID"); //Describe path to field. Number of element in list also should be described.
        String actName = "placeQuoteRequestFIX"; //Set name prefix for act root event.
        try {
            String matchingValue = ActUtils.getMatchingValue(request.getMessage(), matchingFieldPath).getSimpleValue();

            List<ResponseMapper> responseMapping = Arrays.asList(
                    new ResponseMapper(ResponseStatus.FAILED, "QuoteStatusReport", Map.of(
                            new FieldPath("QuoteReqID"), ValueMatcher.equal(matchingValue),
                            new FieldPath("QuoteStatus"), ValueMatcher.equal("5"))
                    ),
                    new ResponseMapper(ResponseStatus.PASSED, "QuoteStatusReport", Map.of(
                            new FieldPath("QuoteReqID"), ValueMatcher.equal(matchingValue))
                    ));

            placeTemplate(request, responseObserver, requestMessageType, actName, responseMapping);
        } catch (FieldNotFoundException e) {
            reportFieldNotFound(request, responseObserver, matchingFieldPath, actName, e);
        }
    }

    @Override
    public void placeQuoteResponseFIX(PlaceMessageRequest request, StreamObserver<PlaceMessageResponse> responseObserver) {
        String requestMessageType = "QuoteResponse"; //Set message type can be placed by this method.
        List<String> matchingFieldPath = Arrays.asList("RFQID"); //Describe path to field. Number of element in list also should be described.
        String actName = "placeQuoteResponseFIX"; //Set name prefix for act root event.
        try {
            String matchingValue = ActUtils.getMatchingValue(request.getMessage(), matchingFieldPath).getSimpleValue();

            List<ResponseMapper> responseMapping = Arrays.asList(
                    new ResponseMapper(ResponseStatus.FAILED, "QuoteStatusReport", Map.of(
                            new FieldPath("RFQID"), ValueMatcher.equal(matchingValue),
                            new FieldPath("QuoteStatus"), ValueMatcher.equal("5"))
                    ),
                    new ResponseMapper(ResponseStatus.PASSED, "QuoteStatusReport", Map.of(
                            new FieldPath("RFQID"), ValueMatcher.equal(matchingValue))
                    ),
                    new ResponseMapper(ResponseStatus.PASSED, "ExecutionReport", Map.of(
                            new FieldPath("RFQID"), ValueMatcher.equal(matchingValue))
                    ));

            placeTemplate(request, responseObserver, requestMessageType, actName, responseMapping);
        } catch (FieldNotFoundException e) {
            reportFieldNotFound(request, responseObserver, matchingFieldPath, actName, e);
        }
    }

    @Override
    public void placeQuoteFIX(PlaceMessageRequest request, StreamObserver<PlaceMessageResponse> responseObserver) {
        String requestMessageType = "Quote"; //Set message type can be placed by this method.
        List<String> matchingFieldPath = Arrays.asList("RFQID"); //Describe path to field. Number of element in list also should be described.
        String actName = "placeQuoteFIX"; //Set name prefix for act root event.
        try {
            String matchingValue = ActUtils.getMatchingValue(request.getMessage(), matchingFieldPath).getSimpleValue();

            List<ResponseMapper> responseMapping = Arrays.asList(
                    new ResponseMapper(ResponseStatus.FAILED, "QuoteStatusReport", Map.of(
                            new FieldPath("RFQID"), ValueMatcher.equal(matchingValue),
                            new FieldPath("QuoteStatus"), ValueMatcher.equal("5"))
                    ),
                    new ResponseMapper(ResponseStatus.PASSED, "QuoteAck", Map.of(
                            new FieldPath("RFQID"), ValueMatcher.equal(matchingValue))
                    ));

            placeTemplate(request, responseObserver, requestMessageType, actName, responseMapping);
        } catch (FieldNotFoundException e) {
            reportFieldNotFound(request, responseObserver, matchingFieldPath, actName, e);
        }
    }

    private void reportFieldNotFound(PlaceMessageRequest request, StreamObserver<PlaceMessageResponse> responseObserver, List<String> matchingFieldPath, String actName, FieldNotFoundException e) {
        LOGGER.error("Failed to find matching field: " + request, e);
        sendErrorResponse(responseObserver, format("Failed to place %s. There is no path %s in request message. Error: %s", request.getMessage().getMetadata().getMessageType(), matchingFieldPath, e.getMessage()));
        LOGGER.debug(format("%s has finished.", actName));
    }

    private void checkRequestMessageType(String expectedMessageType, MessageMetadataOrBuilder metadata) {
        if (!expectedMessageType.equals(metadata.getMessageType())) {
            throw new IllegalArgumentException(format("Unsupported request message type '%s', expected '%s'",
                    metadata.getMessageType(), expectedMessageType));
        }
    }

    /**
     *
     * @param noResponseBodySupplier supplier for {@link IBodyData} that will be added to the event in case there is not response received
     * @param receiver supplier for the {@link AbstractMessageReceiver} that will await for the required message
     */
    private void placeMessage(PlaceMessageRequestOrBuilder request, StreamObserver<PlaceMessageResponse> responseObserver,
            String expectedRequestType, String actName,
            NoResponseBodySupplier noResponseBodySupplier, ReceiverSupplier receiver) throws JsonProcessingException {

        long startPlaceMessage = System.currentTimeMillis(); //Get start time
        Message message = request.getMessage();
        checkRequestMessageType(expectedRequestType, message.getMetadata()); //Check if request message type is coresponds to method.
        ConnectionID requestConnId = message.getMetadata().getId().getConnectionId(); //Get ConnectionID.

        EventID parentId = eventSender.createAndStoreParentEvent(request.getDescription(), actName, requestConnId.getSessionAlias(), PASSED, request.getParentEventId()); //Store parent event.

        Checkpoint checkpoint = registerCheckPoint(parentId); //Request Checkpoint from check1.

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
                            messageReceiver.getState(),
                            messageReceiver.getResponseStatus(),
                            timeout, messageReceiver.processedMessageIDs(), noResponseBodySupplier);
                }
            }
        } catch (RuntimeException | InterruptedException e) {
            LOGGER.error("'{}' internal error: {}", actName, e.getMessage(), e);
            eventSender.createAndStoreErrorEvent(actName,
                    e.getMessage(),
                    ofEpochMilli(startPlaceMessage),
                    parentId);
            sendErrorResponse(responseObserver, "InternalError: " + e.getMessage());
        } finally {
            LOGGER.debug("placeMessage for {} in {} ms", actName, System.currentTimeMillis() - startPlaceMessage);
        }
    }

    private void processResponseMessage(String actName,
            StreamObserver<PlaceMessageResponse> responseObserver,
            Checkpoint checkpoint,
            EventID parentEventId,
            @Nullable Message responseMessage,
            @Nullable State state,
            @Nullable ResponseStatus responseStatus,
            long timeout,
            Iterable<MessageID> messageIDList,
            NoResponseBodySupplier noResponseBodySupplier) throws JsonProcessingException {
        long startTime = System.currentTimeMillis();
        if (responseMessage == null && state == State.START) {
            String message = "Unable to send the message to an external recipient. \n "
                    + "Possible solutions: \n "
                    + "1) Check that conn SECOND direction is connected to act.\n "
                    + "2) Check codec logs or service events(Encoding by dictionary problem).\n "
                    + "3) Check conn logs or service events(Connection problem).\n "
                    + "4) Incorrect session-alias(Check that session-alias exist and filtering is correct).";
            eventSender.createAndStoreSendingFailedEvent(actName, message,
                    Instant.now(),
                    parentEventId);
            sendErrorResponse(responseObserver, message);
        } else if (responseMessage == null) {
            eventSender.createAndStoreNoResponseEvent(actName, noResponseBodySupplier,
                    Instant.now(),
                    parentEventId, messageIDList);
            String message = format("No response message has been received in '%s' ms", timeout);
            sendErrorResponse(responseObserver, message);
        } else {
            MessageMetadata metadata = responseMessage.getMetadata();
            String messageType = metadata.getMessageType();

            TreeTable parametersTable = EventUtils.toTreeTable(responseMessage);
            eventSender.storeEvent(start()
                    .name(format("Received '%s' response message", messageType))
                    .type("ResponseMessage")
                    .status(responseStatus == ResponseStatus.PASSED ? PASSED : FAILED)
                    .bodyData(parametersTable)
                    .messageID(metadata.getId())
                    .toProto(parentEventId)
            );
            PlaceMessageResponse response = PlaceMessageResponse.newBuilder()
                    .setResponseMessage(responseMessage)
                    .setStatus(RequestStatus.newBuilder().setStatus(responseStatus == ResponseStatus.PASSED ? SUCCESS : ERROR).build())
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
            //TODO process response
            eventSender.storeEvent(EventUtils.createSendMessageEvent(message, parentEventId));
        } finally {
            LOGGER.debug("Sending the message ended");
        }
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

    @FunctionalInterface
    private interface ReceiverSupplier {
        AbstractMessageReceiver create(ResponseMonitor monitor, ReceiverContext context);
    }

}
