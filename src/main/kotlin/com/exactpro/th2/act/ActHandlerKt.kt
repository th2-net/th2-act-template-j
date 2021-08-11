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

package com.exactpro.th2.act

import com.exactpro.th2.act.grpc.ActGrpc.ActImplBase
import com.exactpro.th2.act.grpc.PlaceMessageRequest
import com.exactpro.th2.act.grpc.PlaceMessageRequestOrBuilder
import com.exactpro.th2.act.grpc.PlaceMessageResponse
import com.exactpro.th2.act.grpc.SendMessageResponse
import com.exactpro.th2.act.impl.MessageResponseMonitor
import com.exactpro.th2.act.receiver.MessageReceiver
import com.exactpro.th2.act.rule.FieldCheckRuleKt
import com.exactpro.th2.act.utils.CheckMetadata
import com.exactpro.th2.act.utils.EventUtils
import com.exactpro.th2.act.utils.EventUtils.createSendMessageEvent
import com.exactpro.th2.act.utils.ReceiverContext
import com.exactpro.th2.act.utils.ReceiverContext.NoResponseBodySupplier
import com.exactpro.th2.act.utils.ReceiverContext.ReceiverSupplier
import com.exactpro.th2.check1.grpc.Check1Service
import com.exactpro.th2.check1.grpc.CheckpointRequest
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.IBodyData
import com.exactpro.th2.common.grpc.*
import com.exactpro.th2.common.schema.message.MessageRouter
import com.google.common.collect.ImmutableMap
import com.google.protobuf.TextFormat
import com.google.protobuf.util.JsonFormat
import com.exactpro.th2.act.receiver.AbstractMessageReceiver
import io.grpc.Context
import io.grpc.Deadline
import io.grpc.stub.StreamObserver
import mu.KotlinLogging
import org.apache.commons.lang3.exception.ExceptionUtils
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.stream.Collectors

class ActHandlerKt(
    private val messageRouter: MessageRouter<MessageBatch>,
    private val subscriptionManager: SubscriptionManager,
    private val eventBatchMessageRouter: MessageRouter<EventBatch>,
    private val verifierConnector: Check1Service
) : ActImplBase() {

    override fun sendMessage(
        request: PlaceMessageRequest,
        responseObserver: StreamObserver<SendMessageResponse>
    ) {
        val startPlaceMessage = System.currentTimeMillis()
        runCatching {
            LOGGER.debug { "Sending  message request: $request" }

            val actName = "sendMessage"
            // FIXME store parent with fail in case of children fail
            val parentId: EventID = createAndStoreParentEvent(request, actName, Event.Status.PASSED)

            val checkpoint: Checkpoint = verifierConnector.registerCheckPoint(parentId)
            if (Context.current().isCancelled) {
                LOGGER.warn { "'$actName' request cancelled by client" }
                responseObserver.sendMessageErrorResponse("Request has been cancelled by client")
            }

            runCatching {
                sendMessage(backwardCompatibilityConnectionId(request), parentId)
            }.onFailure {
                eventBatchMessageRouter.storeErrorEvent("sendMessage", Instant.now(), parentId, it)
                throw it
            }

            val response = SendMessageResponse.newBuilder()
                    .setStatus(RequestStatus.newBuilder().setStatus(RequestStatus.Status.SUCCESS).build())
                    .setCheckpointId(checkpoint).build()

            responseObserver.onNext(response)
            responseObserver.onCompleted()
        }.onFailure {
            LOGGER.error(it) { "Failed to send a message. Message = ${request.message}" }
            responseObserver.sendMessageErrorResponse("Send message failed. Error: ${it.message}")
        }.onSuccess {
            LOGGER.debug { "Sending the message successfully finished in ${System.currentTimeMillis() - startPlaceMessage}" }
        }
    }

    override fun placeOrderFIX(
        request: PlaceMessageRequest,
        responseObserver: StreamObserver<PlaceMessageResponse>
    ) {
        runCatching {
            LOGGER.debug { "placeOrderFIX request: $request}" }

            placeMessageFieldRule(request,
                    responseObserver,
                    "NewOrderSingle",
                    request.message.fieldsMap["ClOrdID"]!!.simpleValue,
                    mapOf("ExecutionReport" to CheckMetadata.passOn("ClOrdID"),
                            "BusinessMessageReject" to CheckMetadata.failOn("BusinessRejectRefID")),
                    "placeOrderFIX")
        }.onFailure {
            LOGGER.error(it) { "Failed to place an order. Message = ${request.message}" }
            responseObserver.sendErrorResponse("Failed to place an order. Error: ${it.message}")
        }.onSuccess {
            LOGGER.debug { "placeOrderFIX successfully finished" }
        }
    }

    override fun placeOrderMassCancelRequestFIX(
        request: PlaceMessageRequest,
        responseObserver: StreamObserver<PlaceMessageResponse>
    ) {
        runCatching {
            LOGGER.debug { "placeOrderMassCancelRequestFIX request: $request" }

            placeMessageFieldRule(request,
                    responseObserver,
                    "OrderMassCancelRequest",
                    request.message.fieldsMap["ClOrdID"]!!.simpleValue,
                    mapOf("OrderMassCancelReport" to CheckMetadata.passOn("ClOrdID")),
                    "placeOrderMassCancelRequestFIX")
        }.onFailure {
            LOGGER.error(it) { "Failed to place an OrderMassCancelRequest. Message = ${request.message}" }
            responseObserver.sendErrorResponse("Failed to place an OrderMassCancelRequest. Error: ${it.message}")
        }.onSuccess {
            LOGGER.debug { "placeOrderMassCancelRequestFIX successfully finished" }
        }
    }

    override fun placeQuoteCancelFIX(
        request: PlaceMessageRequest,
        responseObserver: StreamObserver<PlaceMessageResponse>
    ) {
        runCatching {
            LOGGER.debug { "placeQuoteCancelFIX request: $request" }
            placeMessageFieldRule(request,
                    responseObserver,
                    "QuoteCancel",
                    request.message.fieldsMap["QuoteMsgID"]!!.simpleValue,
                    ImmutableMap.of("MassQuoteAcknowledgement", CheckMetadata.passOn("QuoteID")),
                    "placeQuoteCancelFIX")
        }.onFailure {
            LOGGER.error(it) { "Failed to place an QuoteCancel. Message = ${request.message}" }
            responseObserver.sendErrorResponse("Failed to place a QuoteCancel. Error: ${it.message}")
        }.onSuccess {
            LOGGER.debug { "placeQuoteCancelFIX successfully finished" }
        }
    }

    override fun placeQuoteRequestFIX(
        request: PlaceMessageRequest,
        responseObserver: StreamObserver<PlaceMessageResponse>
    ) {
        runCatching {
            LOGGER.debug { "placeQuoteRequestFIX request: $request" }
            placeMessageFieldRule(request,
                    responseObserver,
                    "QuoteRequest",
                    request.message.fieldsMap["QuoteReqID"]!!.simpleValue,
                    mapOf("QuoteStatusReport" to CheckMetadata.passOn("QuoteReqID")),
                    "placeQuoteRequestFIX")
        }.onFailure {
            LOGGER.error(it) { "Failed to place an QuoteRequest. Message = ${request.message}" }
            responseObserver.sendErrorResponse("Failed to place a QuoteRequest. Error: ${it.message}")
        }.onSuccess {
            LOGGER.debug { "placeQuoteRequestFIX successfully finished" }
        }
    }

    override fun placeQuoteResponseFIX(
        request: PlaceMessageRequest,
        responseObserver: StreamObserver<PlaceMessageResponse>
    ) {
        runCatching {
            LOGGER.debug { "placeQuoteResponseFIX request: $request" }
            placeMessageFieldRule(request,
                    responseObserver,
                    "QuoteResponse",
                    request.message.fieldsMap["RFQID"]!!.simpleValue,
                    mapOf("ExecutionReport" to CheckMetadata.passOn("RFQID"),
                            "QuoteStatusReport" to CheckMetadata.passOn("RFQID")),
                    "placeQuoteResponseFIX")
        }.onFailure {
            LOGGER.error(it) { "Failed to place an QuoteResponse. Message = ${request.message}" }
            responseObserver.sendErrorResponse("Failed to place a QuoteResponse. Error: ${it.message}")
        }.onSuccess {
            LOGGER.debug { "placeQuoteResponseFIX successfully finished" }
        }
    }

    override fun placeQuoteFIX(
        request: PlaceMessageRequest,
        responseObserver: StreamObserver<PlaceMessageResponse>
    ) {
        runCatching {
            LOGGER.debug { "placeQuoteFIX request: $request" }
            placeMessageFieldRule(request,
                    responseObserver,
                    "Quote",
                    request.message.fieldsMap["RFQID"]!!.simpleValue,
                    mapOf("QuoteAck" to CheckMetadata.passOn("RFQID")),
                    "placeQuoteFIX")
        }.onFailure {
            LOGGER.error(it) { "Failed to place an Quote. Message = ${request.message}" }
            responseObserver.sendErrorResponse("Failed to place a Quote. Error: ${it.message}")
        }.onSuccess {
            LOGGER.debug { "placeQuoteFIX successfully finished" }
        }
    }

    /**
     *
     * @param expectedMessages mapping between response and the event status that should be applied in that case
     * @param noResponseBodySupplier supplier for [IBodyData] that will be added to the event in case there is not response received
     * @param receiver supplier for the [AbstractMessageReceiver] that will await for the required message
     */
    private fun placeMessage(
        request: PlaceMessageRequest,
        responseObserver: StreamObserver<PlaceMessageResponse>,
        expectedRequestType: String,
        expectedMessages: Map<String, CheckMetadata>,
        actName: String,
        noResponseBodySupplier: NoResponseBodySupplier,
        receiver: ReceiverSupplier
    ) {

        val startPlaceMessage = System.currentTimeMillis()
        val message: Message = backwardCompatibilityConnectionId(request)
        requireMessageType(expectedRequestType, message.metadata)
        val requestConnId = message.metadata.id.connectionId
        val parentId: EventID = createAndStoreParentEvent(request, actName, Event.Status.PASSED)
        val checkpoint: Checkpoint = verifierConnector.registerCheckPoint(parentId)
        val monitor = MessageResponseMonitor()

        runCatching {
            receiver.create(monitor, ReceiverContext(requestConnId, parentId)).use { messageReceiver ->
                if (isSendPlaceMessage(message, responseObserver, parentId)) {
                    val startAwaitSync = System.currentTimeMillis()
                    val timeout = getTimeout(Context.current().deadline)
                    monitor.awaitSync(timeout, TimeUnit.MILLISECONDS)
                    LOGGER.debug { "messageReceiver.awaitSync for $actName in ${System.currentTimeMillis() - startAwaitSync} ms" }
                    if (Context.current().isCancelled) {
                        LOGGER.warn { "'$actName' request cancelled by client" }
                        responseObserver.sendErrorResponse("The request has been cancelled by the client")
                    } else {
                        processResponseMessage(actName,
                                responseObserver,
                                checkpoint,
                                parentId,
                                messageReceiver.responseMessage,
                                expectedMessages,
                                timeout,
                                messageReceiver.processedMessageIDs(),
                                noResponseBodySupplier)
                    }
                }
            }
        }.onFailure {
            LOGGER.error(it) { "'$actName' internal error: ${it.message}" }
            eventBatchMessageRouter.storeErrorEvent(actName, Instant.ofEpochMilli(startPlaceMessage), parentId, it)
            responseObserver.sendErrorResponse("InternalError: ${it.message}")
        }
        LOGGER.debug { "placeMessage for $actName in ${System.currentTimeMillis() - startPlaceMessage} ms" }
    }

    private fun placeMessageFieldRule(
        request: PlaceMessageRequest,
        responseObserver: StreamObserver<PlaceMessageResponse>,
        expectedRequestType: String,
        expectedFieldValue: String,
        expectedMessages: Map<String, CheckMetadata>,
        actName: String
    ) {
        placeMessage(request, responseObserver, expectedRequestType, expectedMessages, actName, {
            listOf(EventUtils.createNoResponseBody(expectedMessages, expectedFieldValue))
        }) { monitor: ResponseMonitor, context: ReceiverContext ->
            val msgTypeToFieldName: Map<String, String> = expectedMessages.entries.stream()
                    .collect(Collectors.toUnmodifiableMap(Map.Entry<String, CheckMetadata>::key) { value -> value.value.fieldName })
            val checkRule: CheckRule = FieldCheckRuleKt(expectedFieldValue, msgTypeToFieldName, context.connectionID)
            MessageReceiver(subscriptionManager, monitor, checkRule, Direction.FIRST)
        }
    }

    private fun createAndStoreParentEvent(
        request: PlaceMessageRequestOrBuilder,
        actName: String,
        status: Event.Status
    ): EventID {
        val startTime = System.currentTimeMillis()
        val event = Event.start().apply {
            name("$actName ${request.connectionId.sessionAlias}")
            description(request.description)
            type(actName)
            status(status)
        }.endTimestamp() // FIXME set properly as is in the last child

        val protoEvent = event.toProtoEvent(request.parentEventId.id)
        //FIXME process response
        return protoEvent.runCatching {
            eventBatchMessageRouter.send(EventBatch.newBuilder().addEvents(event.toProtoEvent(request.parentEventId.id))
                    .build(), "publish", "event")
            LOGGER.debug { "createAndStoreParentEvent for $actName in ${System.currentTimeMillis() - startTime} ms" }
            protoEvent.id
        }.onFailure {
            throw RuntimeException("Can not send event = ${protoEvent.id.id}", it)
        }.getOrThrow()
    }

    private fun createAndStoreNoResponseEvent(
        actName: String,
        noResponseBodySupplier: NoResponseBodySupplier,
        start: Instant,
        parentEventId: EventID,
        messageIDList: Collection<MessageID>
    ) {
        val errorEvent = Event.from(start).apply {
            name("Internal $actName error")
            type("No response found by target keys.")
            status(Event.Status.FAILED)
        }.endTimestamp()

        for (data in noResponseBodySupplier.createNoResponseBody()) {
            errorEvent.bodyData(data)
        }
        for (msgID in messageIDList) {
            errorEvent.messageID(msgID)
        }

        eventBatchMessageRouter.tryStoreEvent(errorEvent.toProtoEvent(parentEventId.id))
    }

    private fun processResponseMessage(
        actName: String,
        responseObserver: StreamObserver<PlaceMessageResponse>,
        checkpoint: Checkpoint,
        parentEventId: EventID,
        responseMessage: Message?,
        expectedMessages: Map<String, CheckMetadata>,
        timeout: Long,
        messageIDList: Collection<MessageID>,
        noResponseBodySupplier: NoResponseBodySupplier
    ) {
        val startTime = System.currentTimeMillis()
        val message = "No response message has been received in '$timeout' ms"

        if (responseMessage == null) {
            createAndStoreNoResponseEvent(actName, noResponseBodySupplier, Instant.now(), parentEventId, messageIDList)
            responseObserver.sendErrorResponse(message)
        } else {
            val metadata = responseMessage.metadata
            val messageType = metadata.messageType
            val checkMetadata = expectedMessages[messageType]
            val parametersTable = EventUtils.toTreeTable(responseMessage)

            requireNotNull(checkMetadata) { "CheckMetadata wasn't found in expectedMessages param by key $messageType" }

            eventBatchMessageRouter.tryStoreEvent(Event.start().apply {
                name("Received '$messageType' response message")
                type("message")
                status(checkMetadata.eventStatus)
                bodyData(parametersTable)
                messageID(metadata.id)
            }.toProtoEvent(parentEventId.id))

            val response = PlaceMessageResponse.newBuilder().apply {
                setResponseMessage(responseMessage)
                status = RequestStatus.newBuilder().setStatus(checkMetadata.requestStatus).build()
                checkpointId = checkpoint
            }.build()

            responseObserver.onNext(response)
            responseObserver.onCompleted()
        }
        LOGGER.debug { "processResponseMessage for $actName in ${System.currentTimeMillis() - startTime} ms" }
    }

    private fun isSendPlaceMessage(
        message: Message,
        responseObserver: StreamObserver<PlaceMessageResponse>,
        parentEventId: EventID
    ): Boolean {
        val startTime = System.currentTimeMillis()
        return runCatching {
            sendMessage(message, parentEventId)
            true
        }.onFailure {
            LOGGER.error(it) { "Could not send message to queue" }
            responseObserver.sendErrorResponse("Could not send message to queue: ${it.message}")
        }.onSuccess {
            LOGGER.debug { "isSendPlaceMessage in ${System.currentTimeMillis() - startTime} ms" }
        }.getOrDefault(false)
    }

    private fun sendMessage(message: Message, parentEventId: EventID) {
        runCatching {
            LOGGER.debug { "Sending the message started" }

            //May be use in future for filtering
            //request.getConnectionId().getSessionAlias();
            messageRouter.send(MessageBatch.newBuilder()
                    .addMessages(Message.newBuilder(message).setParentEventId(parentEventId).build())
                    .build())
            //TODO remove after solving issue TH2-217
            //TODO process response
            val eventBatch: EventBatch =
                    EventBatch.newBuilder().addEvents(createSendMessageEvent(message, parentEventId)).build()
            eventBatchMessageRouter.send(eventBatch, "publish", "event")
        }
        LOGGER.debug { "Sending the message ended" }
    }


    private fun convertMessage(message: MessageOrBuilder): Map<String, Any> {
        val fields: MutableMap<String, Any> = HashMap()
        message.fieldsMap.forEach { (key: String, value: Value) ->
            fields[key] = if (value.hasMessageValue()) {
                convertMessage(value.messageValue)
            } else if (value.hasListValue()) {
                convertList(value.listValue)
            } else {
                value.simpleValue
            }
        }
        return fields
    }

    private fun convertList(listValue: ListValueOrBuilder): Any {
        val valuesList = listValue.valuesList
        if (valuesList.isEmpty()) return ArrayList<Any>()

        return if (valuesList[0].hasMessageValue()) {
            valuesList.stream().map { value: Value -> convertMessage(value.messageValue) }.collect(Collectors.toList())
        } else {
            valuesList.stream().map { value: Value -> if (valuesList[0].hasListValue()) convertList(value.listValue) else value.simpleValue }.collect(Collectors.toList())
        }
    }

    private fun StreamObserver<PlaceMessageResponse>.sendErrorResponse(message: String) {
        onNext(PlaceMessageResponse.newBuilder()
                .setStatus(RequestStatus.newBuilder()
                        .setStatus(RequestStatus.Status.ERROR)
                        .setMessage(message)
                        .build())
                .build())
        onCompleted()
        LOGGER.debug { "Error response : $message" }
    }

    private fun StreamObserver<SendMessageResponse>.sendMessageErrorResponse(message: String) {
        onNext(SendMessageResponse.newBuilder()
                .setStatus(RequestStatus.newBuilder()
                        .setStatus(RequestStatus.Status.ERROR)
                        .setMessage(message)
                        .build())
                .build())
        onCompleted()
        LOGGER.debug { "Error response : $message" }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}

        private const val DEFAULT_RESPONSE_TIMEOUT = 10_000

        private fun getTimeout(deadline: Deadline?): Long {
            return deadline?.timeRemaining(TimeUnit.MILLISECONDS) ?: DEFAULT_RESPONSE_TIMEOUT.toLong()
        }

        private fun requireMessageType(
            expectedMessageType: String,
            metadata: MessageMetadata
        ) {
            require(expectedMessageType == metadata.messageType) {
                "Unsupported message type '${metadata.messageType}', expected '${expectedMessageType}'"
            }
        }

        private fun MessageRouter<EventBatch>.tryStoreEvent(eventRequest: com.exactpro.th2.common.grpc.Event) {
            eventRequest.runCatching {
                send(EventBatch.newBuilder().addEvents(eventRequest).build(), "publish", "event")
            }.onFailure {
                LOGGER.error(it) { "Could not store event" }
            }.onSuccess {
                LOGGER.debug { "Event stored: ${JsonFormat.printer().omittingInsignificantWhitespace().print(eventRequest)}" }
            }
        }

        private fun MessageRouter<EventBatch>.storeErrorEvent(
            actName: String,
            start: Instant,
            parentEventId: EventID,
            throwable: Throwable?
        ) = Event.from(start).endTimestamp().apply {
            name("Internal $actName error")
            type("Error")
            status(Event.Status.FAILED)

            var error = throwable

            while (error != null) {
                bodyData(com.exactpro.th2.common.event.EventUtils.createMessageBean(ExceptionUtils.getMessage(error)))
                error = error.cause
            }
            tryStoreEvent(this.toProtoEvent(parentEventId.id))
        }

        private fun Check1Service.registerCheckPoint(parentEventId: EventID): Checkpoint {
            LOGGER.debug { "Registering the checkpoint started" }
            val response = createCheckpoint(CheckpointRequest.newBuilder().setParentEventId(parentEventId).build())
            LOGGER.debug { "Registering the checkpoint ended. Response ${TextFormat.shortDebugString(response)}" }
            return response.checkpoint
        }

        private fun backwardCompatibilityConnectionId(request: PlaceMessageRequest): Message {
            val connectionId = request.message.metadata.id.connectionId
            return if (connectionId.sessionAlias.isNotEmpty()) {
                request.message
            } else {
                Message.newBuilder(request.message).mergeMetadata(
                        MessageMetadata.newBuilder().mergeId(
                                MessageID.newBuilder().setConnectionId(request.connectionId).build()
                        ).build()
                ).build()
            }
        }
    }

}
