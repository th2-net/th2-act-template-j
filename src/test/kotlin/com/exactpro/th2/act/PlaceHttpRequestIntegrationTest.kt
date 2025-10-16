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

import com.exactpro.th2.act.grpc.PlaceHttpRequest
import com.exactpro.th2.act.integration.ActIntegrationTest
import com.exactpro.th2.act.integration.ProtoDirection
import com.exactpro.th2.act.integration.TransportDirection
import com.exactpro.th2.common.annotations.IntegrationTest
import com.exactpro.th2.common.grpc.Checkpoint
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RequestStatus
import com.exactpro.th2.common.message.addField
import com.exactpro.th2.common.message.messageType
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.utils.event.toTransport
import com.exactpro.th2.common.utils.message.toTransport
import com.exactpro.th2.common.utils.message.transport.toProto
import com.exactpro.th2.common.value.toValue
import com.exactpro.th2.test.annotations.Th2AppFactory
import com.exactpro.th2.test.annotations.Th2IntegrationTest
import com.exactpro.th2.test.annotations.Th2TestFactory
import com.exactpro.th2.test.extension.CleanupExtension
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isEmpty
import strikt.assertions.isEqualTo
import strikt.assertions.isNotEqualTo
import strikt.assertions.isNotNull
import java.util.concurrent.TimeUnit.MILLISECONDS
import kotlin.test.assertNotNull

@IntegrationTest
@Th2IntegrationTest
class PlaceHttpRequestIntegrationTest : ActIntegrationTest() {

    @Test
    fun `place HTTP request no response test`(
        @Th2AppFactory factory: CommonFactory,
        @Th2TestFactory test: CommonFactory,
        resourceCleaner: CleanupExtension.Registry,
    ) {
        val env = prepareEnv(factory, test, resourceCleaner)

        val eventId = env.createEvent()
        val messageId = env.createMessageId(ProtoDirection.SECOND)
        val request = request(
            eventId = eventId,
            id = messageId,
            description = "place HTTP request valid",
            type = TYPE_REQUEST,
        )

        val response = env.callAct { placeHttpRequest(request, it) }

        val actEvent = assertNotNull(env.events.poll(OPT_RESPONSE_TIMEOUT * 3, MILLISECONDS))
        expectThat(actEvent) and {
            get { this.id } isNotEqualTo eventId and {
                get { this.bookName } isEqualTo env.book
                get { this.scope } isEqualTo env.scope
            }
            get { this.parentId } isEqualTo eventId
            get { this.status } isEqualTo EventStatus.SUCCESS
            get { this.name } isEqualTo "placeHttpRequest $SESSION_ALIAS - ${request.description}"
            get { this.type } isEqualTo "placeHttpRequest"
            get { this.attachedMessageIdsList }.isEmpty()
            get { this.body.toStringUtf8() } isEqualTo """[{"data":"place HTTP request valid","type":"message"}]"""
        }
        expectThat(env.sendMessages.poll(OPT_RESPONSE_TIMEOUT * 3, MILLISECONDS)).isNotNull() and {
            get { this.id } isEqualTo messageId.toTransport()
            get { this.eventId }.isNotNull() and {
                get { this.book } isEqualTo env.book
                get { this.scope } isEqualTo env.scope
            }
            get { this.type } isEqualTo TYPE_REQUEST
            get { this.body }.isEmpty()
        }
        expectThat(env.events.poll(OPT_RESPONSE_TIMEOUT * 3, MILLISECONDS)).isNotNull() and {
            get { this.id } isNotEqualTo eventId and {
                get { this.bookName } isEqualTo env.book
                get { this.scope } isEqualTo env.scope
            }
            get { this.parentId } isEqualTo actEvent.id
            get { this.status } isEqualTo EventStatus.SUCCESS
            get { this.name } isEqualTo "Send '[parsed($TYPE_REQUEST)]' messages to connectivity"
            get { this.type } isEqualTo "Outgoing message"
            get { this.attachedMessageIdsList }.isEmpty()
            get { this.body.toStringUtf8() } isEqualTo """[{"type":"treeTable","rows":{"0":{"type":"collection","rows":{"type":{"type":"row","columns":{"fieldValue":"$TYPE_REQUEST"}}}}}}]"""
        }
        expectThat(env.events.poll(OPT_RESPONSE_TIMEOUT * 3, MILLISECONDS)).isNotNull() and {
            get { this.id } isNotEqualTo eventId and {
                get { this.bookName } isEqualTo env.book
                get { this.scope } isEqualTo env.scope
            }
            get { this.parentId } isEqualTo actEvent.id
            get { this.status } isEqualTo EventStatus.FAILED
            get { this.name } isEqualTo "Internal placeHttpRequest error"
            get { this.type } isEqualTo "No response found by target keys."
            get { this.attachedMessageIdsList }.isEmpty()
            get { this.body.toStringUtf8() } isEqualTo """[{"type":"treeTable","rows":{"PASSED on:":{"type":"collection","rows":{"statusCode":{"type":"row","columns":{"fieldValue":"200"}}}}}}]"""
        }
        expectThat(response.get(OPT_RESPONSE_TIMEOUT * 3, MILLISECONDS)) and {
            get { this.status } and {
                get { this.status } isEqualTo RequestStatus.Status.ERROR
                get { this.message } isEqualTo "No response message has been received in '$OPT_RESPONSE_TIMEOUT' ms"
            }
            get { this.checkpointId } isEqualTo Checkpoint.getDefaultInstance()
            get { this.httpHeader } isEqualTo Message.getDefaultInstance()
            get { this.httpBody } isEqualTo Message.getDefaultInstance()
        }
        env.asserQueues()
    }

    @Test
    fun `place HTTP request with ok status code`(
        @Th2AppFactory factory: CommonFactory,
        @Th2TestFactory test: CommonFactory,
        resourceCleaner: CleanupExtension.Registry,
    ) {
        val env = prepareEnv(factory, test, resourceCleaner)

        val eventId = env.createEvent()
        val messageIdOut = env.createMessageId(ProtoDirection.SECOND)
        val request = request(
            id = messageIdOut,
            eventId = eventId,
            description = "place HTTP request valid",
            type = TYPE_REQUEST,
        )

        val response = env.callAct { placeHttpRequest(request, it) }

        val actEvent = assertNotNull(env.events.poll(OPT_RESPONSE_TIMEOUT * 3, MILLISECONDS))
        expectThat(actEvent) and {
            get { this.id } isNotEqualTo eventId and {
                get { this.bookName } isEqualTo env.book
                get { this.scope } isEqualTo env.scope
            }
            get { this.parentId } isEqualTo eventId
            get { this.status } isEqualTo EventStatus.SUCCESS
            get { this.name } isEqualTo "placeHttpRequest $SESSION_ALIAS - ${request.description}"
            get { this.type } isEqualTo "placeHttpRequest"
            get { this.attachedMessageIdsList }.isEmpty()
            get { this.body.toStringUtf8() } isEqualTo """[{"data":"place HTTP request valid","type":"message"}]"""
        }
        expectThat(env.sendMessages.poll(OPT_RESPONSE_TIMEOUT * 3, MILLISECONDS)).isNotNull() and {
            get { this.id } isEqualTo messageIdOut.toTransport()
            get { this.eventId }.isNotNull() and {
                get { this.book } isEqualTo env.book
                get { this.scope } isEqualTo env.scope
            }
            get { this.type } isEqualTo TYPE_REQUEST
            get { this.body }.isEmpty()
        }

        val erMessage = message(
            id = env.createMessageId(TransportDirection.INCOMING),
            eventId = actEvent.id.toTransport(),
            type = TYPE_RESPONSE,
            body = mapOf(FIELD_STATUS_CODE to HTTP_CODE_OK)
        ).also(env::oe)

        expectThat(env.events.poll(OPT_RESPONSE_TIMEOUT * 3, MILLISECONDS)).isNotNull() and {
            get { this.id } isNotEqualTo eventId and {
                get { this.bookName } isEqualTo env.book
                get { this.scope } isEqualTo env.scope
            }
            get { this.parentId } isEqualTo actEvent.id
            get { this.status } isEqualTo EventStatus.SUCCESS
            get { this.name } isEqualTo "Send '[parsed($TYPE_REQUEST)]' messages to connectivity"
            get { this.type } isEqualTo "Outgoing message"
            get { this.attachedMessageIdsList }.isEmpty()
            get { this.body.toStringUtf8() } isEqualTo """[{"type":"treeTable","rows":{"0":{"type":"collection","rows":{"type":{"type":"row","columns":{"fieldValue":"$TYPE_REQUEST"}}}}}}]"""
        }
        expectThat(env.events.poll(OPT_RESPONSE_TIMEOUT * 3, MILLISECONDS)).isNotNull() and {
            get { this.id } isNotEqualTo eventId and {
                get { this.bookName } isEqualTo env.book
                get { this.scope } isEqualTo env.scope
            }
            get { this.parentId } isEqualTo actEvent.id
            get { this.status } isEqualTo EventStatus.SUCCESS
            get { this.name } isEqualTo "Received '[$TYPE_RESPONSE]' response messages"
            get { this.type } isEqualTo "messages"
            get { this.attachedMessageIdsList } isEqualTo listOf(erMessage.id.toProto(env.book, SESSION_ALIAS))
            get { this.body.toStringUtf8() } isEqualTo """[{"type":"treeTable","rows":{"0":{"type":"collection","rows":{"type":{"type":"row","columns":{"fieldValue":"$TYPE_RESPONSE"}},"body":{"type":"collection","rows":{"$FIELD_STATUS_CODE":{"type":"row","columns":{"fieldValue":"$HTTP_CODE_OK"}}}}}}}}]"""
        }
        expectThat(response.get(OPT_RESPONSE_TIMEOUT * 3, MILLISECONDS)) and {
            get { this.status } and {
                get { this.status } isEqualTo RequestStatus.Status.SUCCESS
                get { this.message } isEqualTo ""
            }
            get { this.checkpointId } isEqualTo env.checkpoint
            get { this.httpHeader } and {
                get { this.metadata } and {
                    get { this.id } and {
                        get { this.bookName } isEqualTo env.book
                        get { this.connectionId } and {
                            get { this.sessionAlias } isEqualTo SESSION_ALIAS
                            get { this.sessionGroup } isEqualTo SESSION_ALIAS
                        }
                    }
                    get { this.messageType } isEqualTo TYPE_RESPONSE
                }
                get { this.fieldsMap } isEqualTo erMessage.body.mapValues { (_, value) -> value.toValue() }
            }
        }
        env.asserQueues()
    }

    @Test
    fun `place HTTP request with bad status code`(
        @Th2AppFactory factory: CommonFactory,
        @Th2TestFactory test: CommonFactory,
        resourceCleaner: CleanupExtension.Registry,
    ) {
        val env = prepareEnv(factory, test, resourceCleaner)

        val eventId = env.createEvent()
        val messageIdOut = env.createMessageId(ProtoDirection.SECOND)
        val request = request(
            id = messageIdOut,
            eventId = eventId,
            description = "place HTTP request valid",
            type = TYPE_REQUEST,
        )

        val response = env.callAct { placeHttpRequest(request, it) }

        val actEvent = assertNotNull(env.events.poll(OPT_RESPONSE_TIMEOUT * 3, MILLISECONDS))
        expectThat(actEvent) and {
            get { this.id } isNotEqualTo eventId and {
                get { this.bookName } isEqualTo env.book
                get { this.scope } isEqualTo env.scope
            }
            get { this.parentId } isEqualTo eventId
            get { this.status } isEqualTo EventStatus.SUCCESS
            get { this.name } isEqualTo "placeHttpRequest $SESSION_ALIAS - ${request.description}"
            get { this.type } isEqualTo "placeHttpRequest"
            get { this.attachedMessageIdsList }.isEmpty()
            get { this.body.toStringUtf8() } isEqualTo """[{"data":"place HTTP request valid","type":"message"}]"""
        }
        expectThat(env.sendMessages.poll(OPT_RESPONSE_TIMEOUT * 3, MILLISECONDS)).isNotNull() and {
            get { this.id } isEqualTo messageIdOut.toTransport()
            get { this.eventId }.isNotNull() and {
                get { this.book } isEqualTo env.book
                get { this.scope } isEqualTo env.scope
            }
            get { this.type } isEqualTo TYPE_REQUEST
            get { this.body }.isEmpty()
        }

        val erMessage = message(
            id = env.createMessageId(TransportDirection.INCOMING),
            eventId = actEvent.id.toTransport(),
            type = TYPE_RESPONSE,
            body = mapOf(FIELD_STATUS_CODE to HTTP_CODE_BAD),
        ).also(env::oe)

        expectThat(env.events.poll(OPT_RESPONSE_TIMEOUT * 3, MILLISECONDS)).isNotNull() and {
            get { this.id } isNotEqualTo eventId and {
                get { this.bookName } isEqualTo env.book
                get { this.scope } isEqualTo env.scope
            }
            get { this.parentId } isEqualTo actEvent.id
            get { this.status } isEqualTo EventStatus.SUCCESS
            get { this.name } isEqualTo "Send '[parsed($TYPE_REQUEST)]' messages to connectivity"
            get { this.type } isEqualTo "Outgoing message"
            get { this.attachedMessageIdsList }.isEmpty()
            get { this.body.toStringUtf8() } isEqualTo """[{"type":"treeTable","rows":{"0":{"type":"collection","rows":{"type":{"type":"row","columns":{"fieldValue":"$TYPE_REQUEST"}}}}}}]"""
        }
        expectThat(env.events.poll(OPT_RESPONSE_TIMEOUT * 3, MILLISECONDS)).isNotNull() and {
            get { this.id } isNotEqualTo eventId and {
                get { this.bookName } isEqualTo env.book
                get { this.scope } isEqualTo env.scope
            }
            get { this.parentId } isEqualTo actEvent.id
            get { this.status } isEqualTo EventStatus.FAILED
            get { this.name } isEqualTo "Received '[$TYPE_RESPONSE]' response messages"
            get { this.type } isEqualTo "messages"
            get { this.attachedMessageIdsList } isEqualTo listOf(erMessage.id.toProto(env.book, SESSION_ALIAS))
            get { this.body.toStringUtf8() } isEqualTo """[{"type":"treeTable","rows":{"0":{"type":"collection","rows":{"type":{"type":"row","columns":{"fieldValue":"$TYPE_RESPONSE"}},"body":{"type":"collection","rows":{"$FIELD_STATUS_CODE":{"type":"row","columns":{"fieldValue":"$HTTP_CODE_BAD"}}}}}}}}]"""
        }
        expectThat(response.get(OPT_RESPONSE_TIMEOUT * 3, MILLISECONDS)) and {
            get { this.status } and {
                get { this.status } isEqualTo RequestStatus.Status.ERROR
                get { this.message } isEqualTo ""
            }
            get { this.checkpointId } isEqualTo env.checkpoint
            get { this.httpHeader } and {
                get { this.metadata } and {
                    get { this.id } and {
                        get { this.bookName } isEqualTo env.book
                        get { this.connectionId } and {
                            get { this.sessionAlias } isEqualTo SESSION_ALIAS
                            get { this.sessionGroup } isEqualTo SESSION_ALIAS
                        }
                    }
                    get { this.messageType } isEqualTo TYPE_RESPONSE
                }
                get { this.fieldsMap } isEqualTo erMessage.body.mapValues { (_, value) -> value.toValue() }
            }
        }
        env.asserQueues()
    }

    companion object {
        private const val TYPE_REQUEST = "Request"
        private const val TYPE_RESPONSE = "Response"
        private const val HTTP_CODE_OK = "200"
        private const val HTTP_CODE_BAD = "404"
        private const val FIELD_STATUS_CODE = "statusCode"

        private fun request(
            id: MessageID,
            eventId: EventID,
            description: String = "test-description",
            type: String = TYPE_REQUEST,
            payloadBody: Map<String, Any> = emptyMap(),
        ): PlaceHttpRequest = PlaceHttpRequest.newBuilder()
            .setDescription(description)
            .setParentEventId(eventId)
            .apply {
                httpBodyBuilder.apply {
                    messageBuilder.apply {
                        messageType = type
                        metadataBuilder.id = id
                        payloadBody.forEach { (key, value) ->
                            addField(key, value.toValue())
                        }
                    }
                }
            }.build()
    }
}