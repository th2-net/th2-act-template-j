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

package com.exactpro.th2.act.integration

import com.exactpro.th2.act.grpc.AsyncActService
import com.exactpro.th2.check1.grpc.Check1Grpc
import com.exactpro.th2.check1.grpc.CheckpointRequest
import com.exactpro.th2.check1.grpc.CheckpointResponse
import com.exactpro.th2.common.grpc.Checkpoint
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.Event
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Message
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.common.utils.message.transport.toBatch
import com.exactpro.th2.common.utils.message.transport.toGroup
import com.exactpro.th2.test.extension.CleanupExtension
import com.google.protobuf.util.Timestamps
import io.grpc.stub.StreamObserver
import org.junit.jupiter.api.assertAll
import java.time.Instant
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Future
import kotlin.test.assertTrue

typealias ProtoDirection = Direction
typealias TransportDirection = com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction

class TestActEnvironment(
    factory: CommonFactory,
    val sessionAlias: String,
    resourceCleaner: CleanupExtension.Registry,
    queueSize: Int = 100,
) {
    val book = factory.boxConfiguration.bookName
    val scope = factory.boxConfiguration.boxName
    val checkpoint: Checkpoint = createCheckpoint(sessionAlias)
    private val act: AsyncActService = factory.grpcRouter.getService(AsyncActService::class.java)
    val sendMessages = ArrayBlockingQueue<ParsedMessage>(queueSize)
    val sendRawMessages = ArrayBlockingQueue<RawMessage>(queueSize)
    val sendHttpMessages = ArrayBlockingQueue<Message<*>>(queueSize)
    val events = ArrayBlockingQueue<Event>(queueSize)

    private val router = factory.transportGroupBatchRouter

    init {
        factory.grpcRouter.startServer(TestCheck1Service(checkpoint)).apply {
            start()
            resourceCleaner.add(this::shutdownNow)
        }

        router.subscribe({ _, batch ->
            batch.groups.asSequence()
                .flatMap { it.messages.asSequence() }
                .forEach {
                    check(it is ParsedMessage) { "Incorrect type of message ${it::class.simpleName}" }
                    sendMessages.put(it)
                }
        }, "send").also { resourceCleaner.add(it::unsubscribe) }
        router.subscribe({ _, batch ->
            batch.groups.asSequence()
                .flatMap { it.messages.asSequence() }
                .forEach {
                    check(it is RawMessage) { "Incorrect type of message ${it::class.simpleName}" }
                    sendRawMessages.put(it)
                }
        }, "send_raw").also { resourceCleaner.add(it::unsubscribe) }
        router.subscribe({ _, batch ->
            batch.groups.asSequence()
                .flatMap { it.messages.asSequence() }
                .forEach(sendHttpMessages::put)
        }, "send_http").also { resourceCleaner.add(it::unsubscribe) }
        factory.eventBatchRouter.subscribe({ _, batch ->
            batch.eventsList.asSequence().forEach(events::put)
        }).also { resourceCleaner.add(it::unsubscribe) }
    }

    fun <T> callAct(block: AsyncActService.(StreamObserver<T>) -> Unit): Future<T>
        = StreamObserverFuture<T>().also { act.block(it) }

    fun createEvent(): EventID = EventID.newBuilder()
        .setBookName(book)
        .setScope(scope)
        .setStartTimestamp(Timestamps.now())
        .setId("test-event-id-${System.currentTimeMillis()}")
        .build()

    fun createMessageId(direction: ProtoDirection): MessageID = MessageID.newBuilder()
        .setBookName(book)
        .setDirection(direction)
        .setTimestamp(Timestamps.now())
        .setSequence(System.currentTimeMillis())
        .apply {
            connectionIdBuilder
                .setSessionAlias(sessionAlias)
                .setSessionGroup(sessionAlias)
        }.build()

    fun createMessageId(direction: TransportDirection): MessageId = MessageId(
        sessionAlias,
        direction,
        System.currentTimeMillis(),
        Instant.now(),
    )

    fun oe(message: ParsedMessage) {
        router.send(message.toGroup().toBatch(book, message.id.sessionAlias), "oe")
    }

    fun asserQueues() {
        assertAll(
            { assertTrue(sendMessages.isEmpty(), "act sent unchecked message to a pin with 'send' attribute") },
            {
                assertTrue(
                    sendRawMessages.isEmpty(),
                    "act sent unchecked message to a pin 'send_raw'  attribute"
                )
            },
            {
                assertTrue(
                    sendHttpMessages.isEmpty(),
                    "act sent unchecked message to a pin 'send_http'  attribute"
                )
            },
            { assertTrue(events.isEmpty(), "act sent unchecked event to a pin 'event' attribute") },
        )
    }

    private fun createCheckpoint(sessionAlias: String) = Checkpoint.newBuilder().apply {
        id = "test-checkpoint-id-${System.currentTimeMillis()}"
        putBookNameToSessionAliasToDirectionCheckpoint(
            book, Checkpoint.SessionAliasToDirectionCheckpoint.newBuilder().apply {
                putSessionAliasToDirectionCheckpoint(
                    sessionAlias,
                    Checkpoint.DirectionCheckpoint.newBuilder().apply {
                        putDirectionToCheckpointData(0, Checkpoint.CheckpointData.newBuilder().apply {
                            sequence = System.currentTimeMillis()
                            timestamp = Timestamps.now()
                        }.build())
                    }.build()
                )
            }.build()
        )
    }.build()

    companion object {
        private class TestCheck1Service(
            private val checkpoint: Checkpoint
        ) : Check1Grpc.Check1ImplBase() {
            override fun createCheckpoint(
                request: CheckpointRequest,
                responseObserver: StreamObserver<CheckpointResponse>,
            ) {
                with(responseObserver) {
                    onNext(CheckpointResponse.newBuilder().setCheckpoint(checkpoint).build())
                    onCompleted()
                }
            }
        }

        private class StreamObserverFuture<T>(
            private val future: CompletableFuture<T> = CompletableFuture<T>(),
        ): StreamObserver<T>, Future<T> by future {

            override fun onNext(value: T) {
                future.complete(value)
            }

            override fun onError(t: Throwable) {
                future.completeExceptionally(t)
            }

            override fun onCompleted() {
                check(future.isDone) {
                    "onNext method wasn't called"
                }
            }
        }
    }
}