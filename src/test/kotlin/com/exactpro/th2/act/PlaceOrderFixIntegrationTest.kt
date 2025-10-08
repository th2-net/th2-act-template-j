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

import com.exactpro.th2.act.integration.ActIntegrationTest
import com.exactpro.th2.act.integration.ProtoDirection
import com.exactpro.th2.act.integration.TransportDirection
import com.exactpro.th2.common.annotations.IntegrationTest
import com.exactpro.th2.common.grpc.Checkpoint
import com.exactpro.th2.common.grpc.EventStatus
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.RequestStatus
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.utils.message.toTransport
import com.exactpro.th2.common.utils.message.transport.toProto
import com.exactpro.th2.common.value.toValue
import com.exactpro.th2.test.annotations.Th2AppFactory
import com.exactpro.th2.test.annotations.Th2IntegrationTest
import com.exactpro.th2.test.annotations.Th2TestFactory
import com.exactpro.th2.test.extension.CleanupExtension
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.hasSize
import strikt.assertions.isEmpty
import strikt.assertions.isEqualTo
import strikt.assertions.isNotEqualTo
import strikt.assertions.isNotNull
import java.util.concurrent.TimeUnit.MILLISECONDS
import kotlin.test.assertNotNull

@IntegrationTest
@Th2IntegrationTest
class PlaceOrderFixIntegrationTest : ActIntegrationTest() {

    @Test
    fun `place Order FIX no response test`(
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
            description = "place order FIX valid",
            type = TYPE_NEW_ORDER_SINGLE,
            body = mapOf(FIELD_CL_ORD_ID to CL_ORD_ID),
        )

        val response = env.callAct { placeOrderFIX(request, it) }

        val actEvent = assertNotNull(env.events.poll(OPT_RESPONSE_TIMEOUT * 3, MILLISECONDS))
        expectThat(actEvent) and {
            get { this.id } isNotEqualTo eventId and {
                get { this.bookName } isEqualTo env.book
                get { this.scope } isEqualTo env.scope
            }
            get { this.parentId } isEqualTo eventId
            get { this.status } isEqualTo EventStatus.SUCCESS
            get { this.name } isEqualTo "placeOrderFIX $SESSION_ALIAS - ${request.description}"
            get { this.type } isEqualTo "placeOrderFIX"
            get { this.attachedMessageIdsList }.isEmpty()
            get { this.body.toStringUtf8() } isEqualTo """[{"data":"place order FIX valid","type":"message"}]"""
        }
        expectThat(env.sendMessages.poll(OPT_RESPONSE_TIMEOUT * 3, MILLISECONDS)).isNotNull() and {
            get { this.id } isEqualTo messageId.toTransport()
            get { this.eventId }.isNotNull() and {
                get { this.book } isEqualTo env.book
                get { this.scope } isEqualTo env.scope
            }
            get { this.type } isEqualTo TYPE_NEW_ORDER_SINGLE
            get { this.body } hasSize 1 and {
                get { get(FIELD_CL_ORD_ID) } isEqualTo CL_ORD_ID
            }
        }
        expectThat(env.events.poll(OPT_RESPONSE_TIMEOUT * 3, MILLISECONDS)).isNotNull() and {
            get { this.id } isNotEqualTo eventId and {
                get { this.bookName } isEqualTo env.book
                get { this.scope } isEqualTo env.scope
            }
            get { this.parentId } isEqualTo actEvent.id
            get { this.status } isEqualTo EventStatus.SUCCESS
            get { this.name } isEqualTo "Send '$TYPE_NEW_ORDER_SINGLE' message to connectivity"
            get { this.type } isEqualTo "Outgoing message"
            get { this.attachedMessageIdsList }.isEmpty()
            get { this.body.toStringUtf8() } isEqualTo """[{"type":"treeTable","rows":{"$FIELD_CL_ORD_ID":{"type":"row","columns":{"fieldValue":"$CL_ORD_ID"}}}}]"""
        }
        expectThat(env.events.poll(OPT_RESPONSE_TIMEOUT * 3, MILLISECONDS)).isNotNull() and {
            get { this.id } isNotEqualTo eventId and {
                get { this.bookName } isEqualTo env.book
                get { this.scope } isEqualTo env.scope
            }
            get { this.parentId } isEqualTo actEvent.id
            get { this.status } isEqualTo EventStatus.FAILED
            get { this.name } isEqualTo "Internal placeOrderFIX error"
            get { this.type } isEqualTo "No response found by target keys."
            get { this.attachedMessageIdsList }.isEmpty()
            get { this.body.toStringUtf8() } isEqualTo """[{"type":"treeTable","rows":{"FAILED on:":{"type":"collection","rows":{"$TYPE_BUSINESS_MESSAGE_REJECT":{"type":"collection","rows":{"BusinessRejectRefID":{"type":"row","columns":{"fieldValue":"$CL_ORD_ID"}}}}}},"PASSED on:":{"type":"collection","rows":{"$TYPE_EXECUTION_REPORT":{"type":"collection","rows":{"ClOrdID":{"type":"row","columns":{"fieldValue":"$CL_ORD_ID"}}}}}}}}]"""
        }
        expectThat(response.get(OPT_RESPONSE_TIMEOUT * 3, MILLISECONDS)) and {
            get { this.status } and {
                get { this.status } isEqualTo RequestStatus.Status.ERROR
                get { this.message } isEqualTo "No response message has been received in '$OPT_RESPONSE_TIMEOUT' ms"
            }
            get { this.checkpointId } isEqualTo Checkpoint.getDefaultInstance()
            get { this.responseMessage } isEqualTo Message.getDefaultInstance()
        }
        env.asserQueues()
    }

    @Test
    fun `place Order FIX with ER response`(
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
            description = "place order FIX valid",
            type = TYPE_NEW_ORDER_SINGLE,
            body = mapOf(FIELD_CL_ORD_ID to CL_ORD_ID),
        )

        val response = env.callAct { placeOrderFIX(request, it) }

        val actEvent = assertNotNull(env.events.poll(OPT_RESPONSE_TIMEOUT * 3, MILLISECONDS))
        expectThat(actEvent) and {
            get { this.id } isNotEqualTo eventId and {
                get { this.bookName } isEqualTo env.book
                get { this.scope } isEqualTo env.scope
            }
            get { this.parentId } isEqualTo eventId
            get { this.status } isEqualTo EventStatus.SUCCESS
            get { this.name } isEqualTo "placeOrderFIX $SESSION_ALIAS - ${request.description}"
            get { this.type } isEqualTo "placeOrderFIX"
            get { this.attachedMessageIdsList }.isEmpty()
            get { this.body.toStringUtf8() } isEqualTo """[{"data":"place order FIX valid","type":"message"}]"""
        }
        expectThat(env.sendMessages.poll(OPT_RESPONSE_TIMEOUT * 3, MILLISECONDS)).isNotNull() and {
            get { this.id } isEqualTo messageIdOut.toTransport()
            get { this.eventId }.isNotNull() and {
                get { this.book } isEqualTo env.book
                get { this.scope } isEqualTo env.scope
            }
            get { this.type } isEqualTo TYPE_NEW_ORDER_SINGLE
            get { this.body } hasSize 1 and {
                get { get(FIELD_CL_ORD_ID) } isEqualTo CL_ORD_ID
            }
        }

        val erMessage = message(
            id = env.createMessageId(TransportDirection.INCOMING),
            type = TYPE_EXECUTION_REPORT,
            body = mapOf(FIELD_CL_ORD_ID to CL_ORD_ID),
        ).also(env::oe)

        expectThat(env.events.poll(OPT_RESPONSE_TIMEOUT * 3, MILLISECONDS)).isNotNull() and {
            get { this.id } isNotEqualTo eventId and {
                get { this.bookName } isEqualTo env.book
                get { this.scope } isEqualTo env.scope
            }
            get { this.parentId } isEqualTo actEvent.id
            get { this.status } isEqualTo EventStatus.SUCCESS
            get { this.name } isEqualTo "Send '$TYPE_NEW_ORDER_SINGLE' message to connectivity"
            get { this.type } isEqualTo "Outgoing message"
            get { this.attachedMessageIdsList }.isEmpty()
            get { this.body.toStringUtf8() } isEqualTo """[{"type":"treeTable","rows":{"$FIELD_CL_ORD_ID":{"type":"row","columns":{"fieldValue":"$CL_ORD_ID"}}}}]"""
        }
        expectThat(env.events.poll(OPT_RESPONSE_TIMEOUT * 3, MILLISECONDS)).isNotNull() and {
            get { this.id } isNotEqualTo eventId and {
                get { this.bookName } isEqualTo env.book
                get { this.scope } isEqualTo env.scope
            }
            get { this.parentId } isEqualTo actEvent.id
            get { this.status } isEqualTo EventStatus.SUCCESS
            get { this.name } isEqualTo "Received '$TYPE_EXECUTION_REPORT' response message"
            get { this.type } isEqualTo "message"
            get { this.attachedMessageIdsList } isEqualTo listOf(erMessage.id.toProto(env.book, SESSION_ALIAS))
            get { this.body.toStringUtf8() } isEqualTo """[{"type":"treeTable","rows":{"$FIELD_CL_ORD_ID":{"type":"row","columns":{"fieldValue":"$CL_ORD_ID"}}}}]"""
        }
        expectThat(response.get(OPT_RESPONSE_TIMEOUT * 3, MILLISECONDS)) and {
            get { this.status } and {
                get { this.status } isEqualTo RequestStatus.Status.SUCCESS
                get { this.message } isEqualTo ""
            }
            get { this.checkpointId } isEqualTo env.checkpoint
            get { this.responseMessage } and {
                get { this.metadata } and {
                    get { this.id } and {
                        get { this.bookName } isEqualTo env.book
                        get { this.connectionId } and {
                            get { this.sessionAlias } isEqualTo SESSION_ALIAS
                            get { this.sessionGroup } isEqualTo SESSION_ALIAS
                        }
                    }
                    get { this.messageType } isEqualTo TYPE_EXECUTION_REPORT
                }
                get { this.fieldsMap } isEqualTo erMessage.body.mapValues { (_, value) -> value.toValue() }
            }
        }
        env.asserQueues()
    }

    @Test
    fun `place Order FIX with BMR response`(
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
            description = "place order FIX valid",
            type = TYPE_NEW_ORDER_SINGLE,
            body = mapOf(FIELD_CL_ORD_ID to CL_ORD_ID),
        )

        val response = env.callAct { placeOrderFIX(request, it) }

        val actEvent = assertNotNull(env.events.poll(OPT_RESPONSE_TIMEOUT * 3, MILLISECONDS))
        expectThat(actEvent) and {
            get { this.id } isNotEqualTo eventId and {
                get { this.bookName } isEqualTo env.book
                get { this.scope } isEqualTo env.scope
            }
            get { this.parentId } isEqualTo eventId
            get { this.status } isEqualTo EventStatus.SUCCESS
            get { this.name } isEqualTo "placeOrderFIX $SESSION_ALIAS - ${request.description}"
            get { this.type } isEqualTo "placeOrderFIX"
            get { this.attachedMessageIdsList }.isEmpty()
            get { this.body.toStringUtf8() } isEqualTo """[{"data":"place order FIX valid","type":"message"}]"""
        }
        expectThat(env.sendMessages.poll(OPT_RESPONSE_TIMEOUT * 3, MILLISECONDS)).isNotNull() and {
            get { this.id } isEqualTo messageIdOut.toTransport()
            get { this.eventId }.isNotNull() and {
                get { this.book } isEqualTo env.book
                get { this.scope } isEqualTo env.scope
            }
            get { this.type } isEqualTo TYPE_NEW_ORDER_SINGLE
            get { this.body } hasSize 1 and {
                get { get(FIELD_CL_ORD_ID) } isEqualTo CL_ORD_ID
            }
        }

        val erMessage = message(
            id = env.createMessageId(TransportDirection.INCOMING),
            type = TYPE_BUSINESS_MESSAGE_REJECT,
            body = mapOf(FIELD_BUSINESS_REJECT_REF_ID to CL_ORD_ID),
        ).also(env::oe)

        expectThat(env.events.poll(OPT_RESPONSE_TIMEOUT * 3, MILLISECONDS)).isNotNull() and {
            get { this.id } isNotEqualTo eventId and {
                get { this.bookName } isEqualTo env.book
                get { this.scope } isEqualTo env.scope
            }
            get { this.parentId } isEqualTo actEvent.id
            get { this.status } isEqualTo EventStatus.SUCCESS
            get { this.name } isEqualTo "Send '$TYPE_NEW_ORDER_SINGLE' message to connectivity"
            get { this.type } isEqualTo "Outgoing message"
            get { this.attachedMessageIdsList }.isEmpty()
            get { this.body.toStringUtf8() } isEqualTo """[{"type":"treeTable","rows":{"$FIELD_CL_ORD_ID":{"type":"row","columns":{"fieldValue":"$CL_ORD_ID"}}}}]"""
        }
        expectThat(env.events.poll(OPT_RESPONSE_TIMEOUT * 3, MILLISECONDS)).isNotNull() and {
            get { this.id } isNotEqualTo eventId and {
                get { this.bookName } isEqualTo env.book
                get { this.scope } isEqualTo env.scope
            }
            get { this.parentId } isEqualTo actEvent.id
            get { this.status } isEqualTo EventStatus.FAILED
            get { this.name } isEqualTo "Received '$TYPE_BUSINESS_MESSAGE_REJECT' response message"
            get { this.type } isEqualTo "message"
            get { this.attachedMessageIdsList } isEqualTo listOf(erMessage.id.toProto(env.book, SESSION_ALIAS))
            get { this.body.toStringUtf8() } isEqualTo """[{"type":"treeTable","rows":{"$FIELD_BUSINESS_REJECT_REF_ID":{"type":"row","columns":{"fieldValue":"$CL_ORD_ID"}}}}]"""
        }
        expectThat(response.get(OPT_RESPONSE_TIMEOUT * 3, MILLISECONDS)) and {
            get { this.status } and {
                get { this.status } isEqualTo RequestStatus.Status.ERROR
                get { this.message } isEqualTo ""
            }
            get { this.checkpointId } isEqualTo env.checkpoint
            get { this.responseMessage } and {
                get { this.metadata } and {
                    get { this.id } and {
                        get { this.bookName } isEqualTo env.book
                        get { this.connectionId } and {
                            get { this.sessionAlias } isEqualTo SESSION_ALIAS
                            get { this.sessionGroup } isEqualTo SESSION_ALIAS
                        }
                    }
                    get { this.messageType } isEqualTo TYPE_BUSINESS_MESSAGE_REJECT
                }
                get { this.fieldsMap } isEqualTo erMessage.body.mapValues { (_, value) -> value.toValue() }
            }
        }
        env.asserQueues()
    }

    companion object {
        private const val TYPE_NEW_ORDER_SINGLE = "NewOrderSingle"
        private const val TYPE_EXECUTION_REPORT = "ExecutionReport"
        private const val TYPE_BUSINESS_MESSAGE_REJECT = "BusinessMessageReject"
        private const val FIELD_CL_ORD_ID = "ClOrdID"
        private const val FIELD_BUSINESS_REJECT_REF_ID = "BusinessRejectRefID"

        private val CL_ORD_ID = "test-ClOrdID-${System.currentTimeMillis()}"
    }
}