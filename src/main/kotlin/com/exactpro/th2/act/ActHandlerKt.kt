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
import com.exactpro.th2.act.impl.MessageResponseMonitor
import com.exactpro.th2.act.receiver.AbstractMessageReceiver
import com.exactpro.th2.act.receiver.MessageReceiver
import com.exactpro.th2.act.rule.FieldCheckRuleKt
import com.exactpro.th2.check1.grpc.Check1Service
import com.exactpro.th2.check1.grpc.CheckpointRequest
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.event.Event.Status.FAILED
import com.exactpro.th2.common.event.Event.Status.PASSED
import com.exactpro.th2.common.event.EventUtils.createMessageBean
import com.exactpro.th2.common.grpc.Checkpoint
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.Value.KindCase.SIMPLE_VALUE
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageBatch
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.grpc.RequestStatus
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.message.getField
import com.exactpro.th2.common.message.messageType
import com.exactpro.th2.common.message.toTreeTable
import com.fasterxml.jackson.core.JsonProcessingException
import com.google.protobuf.TextFormat.shortDebugString
import com.google.protobuf.util.JsonFormat
import io.grpc.Context
import io.grpc.Deadline
import io.grpc.stub.StreamObserver
import mu.KotlinLogging
import org.apache.commons.lang3.BooleanUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import java.time.Instant
import java.util.concurrent.TimeUnit

class ActHandlerKt(
    private val messageRouter: MessageRouter<MessageBatch>,
    private val subscriptionManager: SubscriptionManager,
    private val eventRouter: MessageRouter<EventBatch>,
    private val verifierConnector: Check1Service
) : ActImplBase() {

    override fun placeOrderFIX(
        request: PlaceMessageRequest,
        responseObserver: StreamObserver<PlaceMessageResponse>
    ) {
        runCatching {
            LOGGER.debug { "placeOrderFIX request: $request}" }
            val expectedValue = requireNotNull(request.message.fieldsMap["ClOrdID"])
            val settings = CallSettings("placeOrderFIX", expectedValue.simpleValue, mapOf("ExecutionReport" to "ClOrdID", "BusinessMessageReject" to "BusinessRejectRefID"))
            placeMessage(settings, request, responseObserver)
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
            val expectedValue = requireNotNull(request.message.fieldsMap["ClOrdID"])
            val settings = CallSettings("placeOrderMassCancelRequestFIX", expectedValue.simpleValue, mapOf("OrderMassCancelReport" to "ClOrdID"))
            placeMessage(settings, request, responseObserver)
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
            val expectedValue = requireNotNull(request.message.fieldsMap["QuoteMsgID"])
            val settings = CallSettings("placeQuoteCancelFIX", expectedValue.simpleValue, mapOf("MassQuoteAcknowledgement" to "QuoteID"))
            placeMessage(settings, request, responseObserver)
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
            val expectedValue = requireNotNull(request.message.fieldsMap["QuoteReqID"])
            val settings = CallSettings("placeQuoteRequestFIX", expectedValue.simpleValue, mapOf("QuoteStatusReport" to "QuoteReqID"))
            placeMessage(settings, request, responseObserver)
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
            val expectedValue = requireNotNull(request.message.fieldsMap["RFQID"])
            val settings = CallSettings("placeQuoteResponseFIX", expectedValue.simpleValue, mapOf("ExecutionReport" to "RFQID", "QuoteStatusReport" to "RFQID"))
            placeMessage(settings, request, responseObserver)
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
            val expectedValue = requireNotNull(request.message.fieldsMap["RFQID"])
            val settings = CallSettings("placeQuoteFIX", expectedValue.simpleValue, mapOf("QuoteAck" to "RFQID"))
            placeMessage(settings, request, responseObserver)
        }.onFailure {
            LOGGER.error(it) { "Failed to place an Quote. Message = ${request.message}" }
            responseObserver.sendErrorResponse("Failed to place a Quote. Error: ${it.message}")
        }.onSuccess {
            LOGGER.debug { "placeQuoteFIX successfully finished" }
        }
    }

    private fun placeMessage(settings: CallSettings, request: PlaceMessageRequest, responseObserver: StreamObserver<PlaceMessageResponse>) {
        LOGGER.debug { "Begin place ${settings.name} ${shortDebugString(request)}" }
        runCatching {
            val startTime = Instant.now()
            val actEventId = createAndStoreParentEvent(request, settings.name, PASSED)

            runCatching {
                val message: Message = backwardCompatibilityConnectionId(request)
                val requestConnId = message.metadata.id.connectionId

                val checkpoint = registerCheckPoint(actEventId)
                val monitor = MessageResponseMonitor()

                createMessageReceiver(settings.expectedValue, settings.expectedFields, monitor, requestConnId).use { messageReceiver ->
                    sendMessages(settings.name, actEventId, startTime, message)
                    val timeout: Long = getTimeout(Context.current().deadline)
                    monitor.awaitSync(timeout, TimeUnit.MILLISECONDS)
                    messageReceiver.processResponseMessage(settings.name, checkpoint, actEventId, responseObserver, timeout)
                }
            }.onFailure {
                eventRouter.storeErrorEvent(settings.name, startTime, actEventId, it)
                throw it
            }

        }.onFailure { it ->
            LOGGER.error(it) { "Failed to place ${settings.name}" }
            responseObserver.sendErrorResponse("Failed to place ${settings.name}, error: ${it.message}")
        }
        LOGGER.debug { "End place ${settings.name}" }
    }

    private fun AbstractMessageReceiver.processResponseMessage(
        callName: String,
        checkpoint: Checkpoint,
        parentEventId: EventID,
        responseObserver: StreamObserver<PlaceMessageResponse>,
        timeout: Long,
    ) {
        responseMessage?.let { response ->
            val status: Boolean = BooleanUtils.toBoolean(
                    response.requiredField(REQUIRED_FIELD, SIMPLE_VALUE).simpleValue
            )

            eventRouter.tryStoreEvent(Event.start().apply {
                name("$callName: Received '${response.messageType}' response message")
                type("message")
                status(if (status) PASSED else FAILED)
                bodyData(response.toTreeTable())
                messageID(response.metadata.id)
            }.toProtoEvent(parentEventId.id))

            val response = PlaceMessageResponse.newBuilder().apply {
                responseMessage = responseMessage
                statusBuilder.apply {
                    this.status = if (status) RequestStatus.Status.SUCCESS else RequestStatus.Status.ERROR
                }
                checkpointId = checkpoint
            }.build()

            responseObserver.onNext(response)
            responseObserver.onCompleted()
        } ?: run {
            val message = "$callName: No response message has been received in '$timeout' ms"
            eventRouter.tryStoreEvent(Event.start().apply {
                name(message)
                type("No response found")
                status(FAILED)
                processedMessageIDs().forEach(this::messageID)
            }.toProtoEvent(parentEventId.id))
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
            eventRouter.send(EventBatch.newBuilder().addEvents(event.toProtoEvent(request.parentEventId.id)).build(), "publish", "event")
            LOGGER.debug { "createAndStoreParentEvent for $actName in ${System.currentTimeMillis() - startTime} ms" }
            protoEvent.id
        }.onFailure {
            throw RuntimeException("Can not send event = ${protoEvent.id.id}", it)
        }.getOrThrow()
    }

    private fun sendMessages(callName: String, parentEventId: EventID, instant: Instant, vararg messages: Message) {
        LOGGER.debug { "Sending the message started" }
        runCatching {
            messageRouter.send(MessageBatch.newBuilder().apply {
                messages.forEach { message ->
                    addMessages(message)
                }
            }.build())
            eventRouter.tryStoreEvent(Event.from(instant).endTimestamp().apply {
                name("$callName: Send '${messages.map(Message::messageType).joinToString(",")}' message(s) to codec pipeline")
                type("Outgoing message")
                status(PASSED)

                messages.forEach { message ->
                    bodyData(message.toTreeTable())
                }

            }.toProtoEvent(parentEventId.id))
        }
        LOGGER.debug { "Sending the message ended" }
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

    private fun createMessageReceiver(
        expectedFieldValue: String,
        expectedMessages: Map<String, String>,
        monitor: MessageResponseMonitor,
        connectionId: ConnectionID
    ): MessageReceiver {
        return MessageReceiver(
                subscriptionManager,
                monitor,
                FieldCheckRuleKt(
                        expectedFieldValue,
                        expectedMessages,
                        connectionId
                ),
                Direction.FIRST
        )
    }

    data class CallSettings(
        val name: String,
        val expectedValue: String,
        val expectedFields: Map<String, String>
    )

    private fun registerCheckPoint(parentEventId: EventID): Checkpoint {
        LOGGER.debug { "Registering the checkpoint started" }
        val response = verifierConnector.createCheckpoint(CheckpointRequest.newBuilder().setParentEventId(parentEventId).build())
        LOGGER.debug { "Registering the checkpoint ended. Response ${shortDebugString(response)}" }
        return response.checkpoint
    }

    // TODO: move to common lib
    private fun Message.requiredField(fieldName: String, kind: Value.KindCase) = requireNotNull(getField(fieldName)) {
        "Message doesn't contain the $fieldName required field, content ${shortDebugString(this)}"
    }.also {
        check(it.kindCase == kind) {
            "The $fieldName field with value ${shortDebugString(it)} has incorrect kind, expected: $kind, actual: ${it.kindCase}"
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
        status(FAILED)

        var error = throwable

        while (error != null) {
            bodyData(createMessageBean(ExceptionUtils.getMessage(error)))
            error = error.cause
        }
        tryStoreEvent(this.toProtoEvent(parentEventId.id))
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

    companion object {
        private val LOGGER = KotlinLogging.logger {}

        private const val REQUIRED_FIELD = "REQUIRED_FIELD"
        private const val DEFAULT_RESPONSE_TIMEOUT = 10_000

        private fun getTimeout(deadline: Deadline?): Long {
            return deadline?.timeRemaining(TimeUnit.MILLISECONDS) ?: DEFAULT_RESPONSE_TIMEOUT.toLong()
        }
    }

}
