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

import com.exactpro.th2.act.Configuration
import com.exactpro.th2.act.grpc.ActService
import com.exactpro.th2.act.grpc.PlaceMessageRequest
import com.exactpro.th2.act.setupApp
import com.exactpro.th2.check1.grpc.Check1Service
import com.exactpro.th2.common.annotations.IntegrationTest
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.message.addField
import com.exactpro.th2.common.message.messageType
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.EventId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.utils.shutdownGracefully
import com.exactpro.th2.common.value.toValue
import com.exactpro.th2.test.annotations.Th2IntegrationTest
import com.exactpro.th2.test.extension.CleanupExtension
import com.exactpro.th2.test.spec.CustomConfigSpec
import com.exactpro.th2.test.spec.GrpcSpec
import com.exactpro.th2.test.spec.RabbitMqSpec
import com.exactpro.th2.test.spec.client
import com.exactpro.th2.test.spec.pin
import com.exactpro.th2.test.spec.pins
import com.exactpro.th2.test.spec.publishers
import com.exactpro.th2.test.spec.server
import com.exactpro.th2.test.spec.subscribers
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import io.grpc.Context
import io.grpc.Deadline
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import strikt.api.expectThat
import strikt.assertions.isEmpty
import strikt.assertions.isEqualTo
import strikt.assertions.isNotNull
import strikt.assertions.matches
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

@IntegrationTest
@Th2IntegrationTest
abstract class ActIntegrationTest {
    @Suppress("unused")
    val mq = RabbitMqSpec.create()
        .pins {
            publishers {
                pin("to_codec") {
                    attributes("transport-group", "send")
                }
                pin("to_conn") {
                    attributes("transport-group", "send_raw")
                }
            }
            subscribers {
                pin("from_codec") {
                    attributes("transport-group", "oe")
                }
            }
        }

    @Suppress("unused")
    val grpc = GrpcSpec.create()
        .server<ActService>()
        .client<Check1Service>()

    @Suppress("unused")
    val custom = CustomConfigSpec.fromObject(
        MAPPER.readValue(
            """
                response-timeout: $OPT_RESPONSE_TIMEOUT
            """.trimIndent(), Configuration::class.java
        )
    )

    @BeforeAll
    fun `before all`() {
        EXECUTOR = Executors.newSingleThreadScheduledExecutor()
    }

    @AfterAll
    fun `after all`() {
        EXECUTOR.shutdownGracefully()
    }

    protected fun request(
        id: MessageID,
        eventId: EventID,
        description: String = "test-description",
        type: String = "NewOrderSingle",
        body: Map<String, Any> = emptyMap(),
    ): PlaceMessageRequest = PlaceMessageRequest.newBuilder()
        .setDescription(description)
        .setParentEventId(eventId)
        .apply {
            messageBuilder.apply {
                messageType = type
                metadataBuilder.id = id
                body.forEach { (key, value) ->
                    addField(key, value.toValue())
                }
            }
        }.build()

    protected fun message(
        id: MessageId,
        eventId: EventId? = null,
        type: String = "ExecutionReport",
        body: Map<String, Any> = emptyMap(),
    ): ParsedMessage = ParsedMessage.builder()
        .setId(id)
        .setType(type)
        .setBody(body)
        .apply {
            eventId?.let(this::setEventId)
        }.build()

    companion object {
        protected const val SESSION_ALIAS = "test-session-alias"
        protected const val OPT_RESPONSE_TIMEOUT = 1_000L

        private val MAPPER: ObjectMapper = ObjectMapper(YAMLFactory())
        private lateinit var EXECUTOR: ScheduledExecutorService

        @JvmStatic
        protected fun prepareEnv(
            factory: CommonFactory,
            test: CommonFactory,
            resourceCleaner: CleanupExtension.Registry
        ): TestActEnvironment {
            setupApp(factory, resourceCleaner::add)
            val env = TestActEnvironment(test, SESSION_ALIAS, resourceCleaner)

            expectThat(env.events.poll(1, TimeUnit.SECONDS)).isNotNull() and {
                get { this.id } and {
                    get { this.bookName } isEqualTo factory.boxConfiguration.bookName
                    get { this.scope } isEqualTo factory.boxConfiguration.boxName
                }
                get { this.parentId } isEqualTo EventID.getDefaultInstance()
                get { this.status } isEqualTo EventStatus.SUCCESS
                get { this.name } matches Regex("""app .* - Root event""")
                get { this.type } isEqualTo "Microservice"
                get { this.attachedMessageIdsList }.isEmpty()
                get { this.body.toStringUtf8() } isEqualTo """[{"data":"Root event","type":"message"}]"""
            }

            return env
        }

        @JvmStatic
        protected fun <T> withDeadline(duration: Long = 1, units: TimeUnit = TimeUnit.SECONDS, block: () -> T): T =
            Context.current()
                .withDeadline(Deadline.after(duration, units), EXECUTOR).call(block)
    }
}