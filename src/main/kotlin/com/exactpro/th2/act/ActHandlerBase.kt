/*
 * Copyright 2025 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.act.grpc.PlaceHttpRequest
import com.exactpro.th2.act.grpc.PlaceHttpResponse
import com.exactpro.th2.check1.grpc.Check1Service
import com.exactpro.th2.check1.grpc.CheckpointRequest
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.Event.Status.FAILED
import com.exactpro.th2.common.event.Event.Status.PASSED
import com.exactpro.th2.common.event.IBodyData
import com.exactpro.th2.common.grpc.Checkpoint
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RequestStatus
import com.exactpro.th2.common.grpc.RequestStatus.Status.ERROR
import com.exactpro.th2.common.grpc.RequestStatus.Status.SUCCESS
import com.exactpro.th2.common.message.messageType
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction.INCOMING
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Message
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.common.utils.event.toTransport
import com.exactpro.th2.common.utils.message.MessageHolder
import com.exactpro.th2.common.utils.message.sessionAlias
import com.exactpro.th2.common.utils.message.sessionGroup
import com.exactpro.th2.common.utils.message.toTransportBuilder
import com.exactpro.th2.common.utils.message.transport.toBatch
import com.exactpro.th2.common.utils.message.transport.toGroup
import com.exactpro.th2.common.utils.message.transport.toTreeTable
import com.google.protobuf.MessageOrBuilder
import io.github.oshai.kotlinlogging.KotlinLogging
import io.grpc.Context
import io.grpc.stub.StreamObserver
import java.io.IOException
import java.lang.System.currentTimeMillis
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import com.exactpro.th2.common.grpc.Event as ProtoEvent
import com.exactpro.th2.common.grpc.Message as ProtoMessage

@Suppress("unused")
open class ActHandlerBase(
    protected val messageRouter: MessageRouter<GroupBatch>,
    private val eventRouter: MessageRouter<EventBatch>,
    protected val subscriptionManager: SubscriptionManager,
    private val check1Service: Check1Service?,
    private val responseTimeout: Int,
) : ActImplBase() {

    override fun placeHttpRequest(
        request: PlaceHttpRequest,
        response: StreamObserver<PlaceHttpResponse>
    ) {
        handleRequest(
            "placeHttpRequest",
            request.parentEventId,
            request,
            { text -> errorHttpResponse(response, text) }
        ) { actName, start ->
            require(request.hasHttpHeader() || request.hasHttpBody()) {
                "'http header' or 'http body' must be filled in"
            }
            var sessionAlias = ""
            var sessionGroup = ""
            val messages = mutableListOf<Message.Builder<*>>()
            if (request.hasHttpHeader()) {
                val metadata = request.httpHeader.metadata
                require(metadata.messageType == MSG_TYPE_HTTP_REQUEST) {
                    "Unsupported request message type '${metadata.messageType}', expected '$MSG_TYPE_HTTP_REQUEST'"
                }
                if (request.hasHttpBody() && request.httpBody.hasMessage()) {
                    require(metadata.protocol.isNotBlank()) {
                        "Http header message protocol must not be blank"
                    }
                }
                sessionAlias = metadata.id.connectionId.sessionAlias
                sessionGroup = metadata.id.connectionId.sessionGroup
                messages.add(request.httpHeader.toTransportBuilder())
            }
            if (request.hasHttpBody()) {
                val httpBody = request.httpBody
                httpBody.sessionAlias?.let { sessionAlias = it }
                httpBody.sessionGroup?.let { sessionGroup = it }
                when {
                    httpBody.hasMessage() -> messages.add(httpBody.message.toTransportBuilder())
                    httpBody.hasRawMessage() -> messages.add(httpBody.rawMessage.toTransportBuilder())
                    else -> error("Unsupported message kind: ${httpBody.kindCase}")
                }
            }

            val rootEventId = rootEvent(start, actName, sessionAlias, request.description, request.parentEventId).id
            val checkpoint = registerCheckPoint(rootEventId)

            sendMessages(rootEventId, messages, sessionGroup)

            AwaitGroupContext(setOf(INCOMING)) { group ->
                if (group.eventId == rootEventId) {
                    result(group)
                }
            }.use { context ->
                val timeout = calculateTimeout()
                context.await(timeout)?.let { result ->
                    val results = result.asSequence().filterNotNull().toList()

                    val status = if (results.first().getSimple("statusCode") == "200") PASSED else FAILED
                    Event.start()
                        .name("Received '${results.map(MessageHolder::messageType)}' response messages")
                        .type("messages")
                        .status(status)
                        .bodyData(result.toTreeTable())
                        .messageID(result.id)
                        .toBatchProto(rootEventId)
                        .also(::sendEventBatch)
                    response.messageResponse(results, checkpoint, status.toResponseStatus())
                } ?: run {
                    val bodyData = treeTable {
                        collection("PASSED on:") {
                            rowColumn("statusCode", 200)
                        }
                    }
                    noResponseEvent(start, actName, rootEventId, bodyData, context.messageIds)
                    errorHttpResponse(response, "No response message has been received in '$timeout' ms")
                }
            }
        }
    }

    protected fun handleRequest(
        actName: String,
        eventId: EventID,
        request: MessageOrBuilder,
        onError: (message: String) -> Unit,
        block: (actName: String, start: Instant) -> Unit,
    ) {
        val start = Instant.now()
        try {
            LOGGER.debug { "$actName request: ${request.toJson()}" }
            block(actName, start)
        } catch (e: Exception) {
            val text = "Failed to prepare event for '$actName' method handling"
            LOGGER.error(e) { text }
            errorEvent(start, actName, eventId, e)
            onError("$text: ${e.message}")
        } finally {
            LOGGER.debug { "$actName has finished" }
        }
    }

    protected fun errorHttpResponse(
        observer: StreamObserver<PlaceHttpResponse>,
        message: String
    ) {
        observer.onNext(
            PlaceHttpResponse.newBuilder()
                .setStatus(
                    RequestStatus.newBuilder()
                        .setStatus(ERROR)
                        .setMessage(message)
                        .build()
                ).build()
        )
        observer.onCompleted()
    }

    protected fun StreamObserver<PlaceHttpResponse>.messageResponse(
        messages: List<MessageHolder>,
        checkpoint: Checkpoint,
        status: RequestStatus.Status
    ) {
        onNext(
            PlaceHttpResponse.newBuilder().apply {
                if (messages.isNotEmpty()) {
                    httpHeader = messages[0].protoMessage
                    if (messages.size > 1) {
                        httpBody = messages[1].protoMessage
                    }
                }
                checkpointId = checkpoint
                statusBuilder.setStatus(status)
            }.build()
        )
        onCompleted()
    }

    protected fun errorEvent(
        start: Instant,
        actName: String,
        parentEventId: EventID,
        e: Exception
    ) {
        Event.from(start)
            .endTimestamp()
            .name("Internal $actName error")
            .type("Error")
            .status(FAILED)
            .exception(e, true)
            .toBatchProto(parentEventId)
            .also(::sendEventBatch)
    }

    protected fun noResponseEvent(
        start: Instant,
        actName: String,
        parentEventId: EventID,
        body: IBodyData,
        messageIds: Collection<MessageID>,
    ) {
        Event.from(start)
            .name("Internal $actName error")
            .type("No response found by target keys.")
            .status(FAILED)
            .bodyData(body)
            .also { messageIds.forEach(it::messageID) }
            .toBatchProto(parentEventId)
            .also(::sendEventBatch)
    }

    protected fun sendMessageEvent(parentEventId: EventID, message: ParsedMessage) {
        Event.start()
            .name("Send '${message.type}' message to connectivity")
            .type("Outgoing message")
            .status(PASSED)
            .bodyData(message.toTreeTable())
            .toBatchProto(parentEventId)
            .also(::sendEventBatch)
    }

    protected fun sendMessagesEvent(eventId: EventID, messages: List<Message<*>>) {
        val messageDescription = messages.joinToString(separator = ",", prefix = "[", postfix = "]") {
            when (it) {
                is ParsedMessage -> "parsed(${it.type})"
                is RawMessage -> "raw(${it.body.readableBytes()}B)"
                else -> "${it.javaClass.simpleName}"
            }
        }

        Event.start()
            .name("Send '$messageDescription' messages to connectivity")
            .type("Outgoing message")
            .status(PASSED)
            .bodyData(messages.toTreeTable())
            .toBatchProto(eventId)
            .also(::sendEventBatch)
    }

    protected fun rootEvent(
        start: Instant,
        actName: String,
        sessionAlias: String,
        description: String,
        parentEventId: EventID,
    ): ProtoEvent {
        val eventBatch = Event.from(start)
            .name("$actName $sessionAlias")
            .description(description)
            .type(actName)
            .status(PASSED)
            .toBatchProto(parentEventId)

        sendEventBatch(eventBatch)
        LOGGER.debug { "create and sent act event for $actName in ${currentTimeMillis() - start.toEpochMilli()} ms" }
        return eventBatch.getEvents(0)
    }

    protected fun sendEventBatch(event: EventBatch) {
        catching {
            LOGGER.debug {
                if (event.eventsCount == 1) {
                    "Try to send event: ${event.getEvents(0).toJson()}"
                } else {
                    "Try to send events: ${event.toJson()}"
                }
            }
            eventRouter.send(event)
        }.getOrElse {
            throw IllegalStateException(
                "Send ${event.eventsCount} events for '${event.parentEventId.toJson()}' parent event failure",
                it
            )
        }
    }

    @Throws(IOException::class)
    protected fun sendMessage(eventId: EventID, message: ProtoMessage) {
        catching {
            message.toTransportBuilder()
                .setEventId(eventId.toTransport())
                .build().apply {
                    val id = message.metadata.id
                    messageRouter.send(
                        this.toGroup().toBatch(id.bookName, id.connectionId.sessionGroup),
                        SEND_QUEUE_ATTRIBUTE
                    )
                }
        }.getOrElse {
            throw IllegalStateException(
                "Send '${message.messageType}' message with '${eventId.toJson()}' parent event failure",
                it
            )
        }.also { sendMessageEvent(eventId, it) }
    }

    @Throws(IOException::class)
    protected fun sendMessages(eventId: EventID, messages: List<Message.Builder<*>>, sessionGroup: String) {
        catching {
            require(messages.isNotEmpty()) { "Message list can't be empty" }

            val transportEventId = eventId.toTransport()
            messages.map { it.setEventId(transportEventId).build() }.apply {
                messageRouter.send(
                    MessageGroup(this).toBatch(eventId.bookName, sessionGroup),
                    SEND_QUEUE_ATTRIBUTE
                )
            }
        }.getOrElse {
            throw IllegalStateException(
                "Send '${messages.size}' messages with '${eventId.toJson()}' parent event failure",
                it
            )
        }.also { sendMessagesEvent(eventId, it) }
    }

    protected fun registerCheckPoint(
        eventId: EventID,
    ): Checkpoint = catching {
        check1Service?.let { service ->
            LOGGER.debug { "Registering the checkpoint started" }
            service.createCheckpoint(
                CheckpointRequest.newBuilder()
                    .setParentEventId(eventId)
                    .build()
            ).also {
                LOGGER.debug { "Registering the checkpoint ended. Response ${it.toJson()}" }
            }.checkpoint
        } ?: Checkpoint.getDefaultInstance()
    }.getOrElse {
        throw IllegalStateException(
            "Register checkpoint for '${eventId.toJson()}' parent event failure",
            it
        )
    }

    inner class AwaitMessageContext(
        private val directions: Set<Direction>,
        private val rule: AwaitMessageContext.(entity: MessageHolder) -> Unit,
    ) : AutoCloseable, MessageListener {
        private val lock = ReentrantLock()
        private val condition = lock.newCondition()

        private lateinit var resultMessage: MessageHolder

        val messageIds = CopyOnWriteArrayList<MessageID>()

        init {
            require(directions.isNotEmpty()) { "'directions' can't be empty" }
            directions.forEach { subscriptionManager.register(it, this) }
        }

        override fun handle(message: MessageHolder) {
            messageIds.add(message.id)
            rule(message)
        }

        override fun close() {
            directions.forEach { subscriptionManager.unregister(it, this) }
        }

        fun result(value: MessageHolder) = lock.withLock {
            resultMessage = value
            condition.signalAll()
        }

        fun await(timeout: Long): MessageHolder? = lock.withLock {
            if (this::resultMessage.isInitialized) {
                LOGGER.debug { "Monitor has been notified before it has started to await a response" }
                return@withLock resultMessage
            }
            if (!condition.await(timeout, TimeUnit.MILLISECONDS)) {
                LOGGER.info { "Timeout ($timeout ms) elapsed before monitor was notified" }
                return@withLock null
            }
            return@withLock resultMessage
        }
    }

    inner class AwaitGroupContext(
        private val directions: Set<Direction>,
        private val rule: AwaitGroupContext.(entity: GroupHolder) -> Unit,
    ) : AutoCloseable, GroupListener {
        private val lock = ReentrantLock()
        private val condition = lock.newCondition()

        private lateinit var resultGroup: GroupHolder

        val messageIds = CopyOnWriteArrayList<MessageID>()

        init {
            require(directions.isNotEmpty()) { "'directions' can't be empty" }
            directions.forEach { subscriptionManager.register(it, this) }
        }

        override fun handle(group: GroupHolder) {
            messageIds.add(group.id)
            rule(group)
        }

        override fun close() {
            directions.forEach { subscriptionManager.unregister(it, this) }
        }

        fun result(value: GroupHolder) = lock.withLock {
            resultGroup = value
            condition.signalAll()
        }

        fun await(timeout: Long): GroupHolder? = lock.withLock {
            if (this::resultGroup.isInitialized) {
                LOGGER.debug { "Monitor has been notified before it has started to await a response" }
                return@withLock resultGroup
            }
            if (!condition.await(timeout, TimeUnit.MILLISECONDS)) {
                LOGGER.info { "Timeout ($timeout ms) elapsed before monitor was notified" }
                return@withLock null
            }
            return@withLock resultGroup
        }
    }

    protected fun calculateTimeout(): Long =
        Context.current().getDeadline()?.timeRemaining(TimeUnit.MILLISECONDS) ?: responseTimeout.toLong()

    companion object {
        protected const val SEND_RAW_QUEUE_ATTRIBUTE: String = "send_raw"
        protected const val SEND_QUEUE_ATTRIBUTE: String = "send"

        private const val MSG_TYPE_HTTP_REQUEST = "Request"

        private val LOGGER = KotlinLogging.logger { }

        protected fun Event.Status.toResponseStatus(): RequestStatus.Status = when (this) {
            PASSED -> SUCCESS
            FAILED -> ERROR
        }

        inline fun <T, R> T.catching(block: T.() -> R): Result<R> {
            return try {
                Result.success(block())
            } catch (e: Exception) {
                Result.failure(e)
            }
        }
    }
}