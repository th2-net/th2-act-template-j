/*
 * Copyright 2021-2024 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.act.impl.SubscriptionManagerImpl
import com.exactpro.th2.act.rules.AbstractSingleConnectionRule
import com.exactpro.th2.act.util.TEST_SESSION_ALIAS
import com.exactpro.th2.act.util.createProtoMessage
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.schema.message.DeliveryMetadata
import com.exactpro.th2.common.utils.message.MessageHolder
import com.exactpro.th2.common.utils.message.toTransport
import com.exactpro.th2.common.utils.message.transport.toBatch
import com.exactpro.th2.common.utils.message.transport.toGroup
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.junit.jupiter.api.Test
import strikt.api.expect
import strikt.assertions.containsExactlyInAnyOrder
import strikt.assertions.isEqualTo
import strikt.assertions.isNotNull
import strikt.assertions.isSuccess
import java.util.Objects
import java.util.function.Function

class TestBiDirectionalMessageReceiver {
    private val connectionID = ConnectionID.newBuilder()
        .setSessionAlias(TEST_SESSION_ALIAS)
        .build()
    private val manager = SubscriptionManagerImpl()
    private val monitor: ResponseMonitor = mock { }
    private val deliveryMetadata = DeliveryMetadata("test_tag_1", false)

    private fun receiver(outgoing: CheckRule, incomingSupplier: (MessageHolder) -> CheckRule): AbstractMessageReceiver =
        BiDirectionalMessageReceiver(
            manager,
            monitor,
            outgoing,
            Function(incomingSupplier)
        )

    @Test
    fun `works in normal case`() {
        val messageA = createProtoMessage("A", Direction.SECOND).build()
        val messageB = createProtoMessage("B", Direction.FIRST).build()

        val receiver = receiver(IdentityRule(messageA, connectionID)) { IdentityRule(messageB, connectionID) }
        receiver.use {
            manager.handle(deliveryMetadata, messageA.toTransportBatch())
            manager.handle(deliveryMetadata, messageB.toTransportBatch())
        }

        expect {
            that(receiver).apply {
                get { responseMessage?.protoMessage }.isNotNull()
                    .isEqualTo(messageB)
                get { processedMessageIDs() }.containsExactlyInAnyOrder(messageA.metadata.id, messageB.metadata.id)
            }
            catching { verify(monitor).responseReceived() }.isSuccess()
        }
    }

    @Test
    fun `works if incoming is processed before outgoing`() {
        val messageA = createProtoMessage("A", Direction.SECOND).build()
        val messageB = createProtoMessage("B", Direction.FIRST).build()

        val receiver = receiver(IdentityRule(messageA, connectionID)) { IdentityRule(messageB, connectionID) }
        receiver.use {
            manager.handle(deliveryMetadata, messageB.toTransportBatch())
            manager.handle(deliveryMetadata, messageA.toTransportBatch())
        }

        expect {
            that(receiver).apply {
                get { responseMessage?.protoMessage }.isNotNull()
                    .isEqualTo(messageB)
                get { processedMessageIDs() }.containsExactlyInAnyOrder(messageA.metadata.id, messageB.metadata.id)
            }
            catching { verify(monitor).responseReceived() }.isSuccess()
        }
    }

    @Test
    fun `works if incoming received after the buffered message`() {
        val messageA = createProtoMessage("A", Direction.SECOND).build()
        val messageB = createProtoMessage("B", Direction.FIRST).build()
        val messageC = createProtoMessage("C", Direction.FIRST).build()

        val receiver = receiver(IdentityRule(messageA, connectionID)) { IdentityRule(messageC, connectionID) }
        receiver.use {
            manager.handle(deliveryMetadata, messageB.toTransportBatch())
            manager.handle(deliveryMetadata, messageA.toTransportBatch())
            manager.handle(deliveryMetadata, messageC.toTransportBatch())
        }

        expect {
            that(receiver).apply {
                get { responseMessage?.protoMessage }.isNotNull()
                    .isEqualTo(messageC)
                get { processedMessageIDs() }.containsExactlyInAnyOrder(
                    messageA.metadata.id,
                    messageB.metadata.id,
                    messageC.metadata.id
                )
            }
            catching { verify(monitor).responseReceived() }.isSuccess()
        }
    }

    private class IdentityRule(
        private val message: Message,
        connectionId: ConnectionID
    ) : AbstractSingleConnectionRule(connectionId) {
        override fun checkMessageFromConnection(message: MessageHolder): Boolean =
            Objects.equals(this.message, message.protoMessage)
    }

    companion object {
        fun Message.toTransportBatch() = metadata.id.run {
            this@toTransportBatch.toTransport().toGroup().toBatch(bookName, connectionId.sessionGroup)
        }
    }
}