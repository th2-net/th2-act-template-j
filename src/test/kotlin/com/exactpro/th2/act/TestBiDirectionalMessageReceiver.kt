/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.act.util.createDefaultMessage
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageBatch
import com.exactpro.th2.common.schema.message.DeliveryMetadata
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.verify
import org.junit.jupiter.api.Test
import strikt.api.expect
import strikt.assertions.containsExactlyInAnyOrder
import strikt.assertions.isNotNull
import strikt.assertions.isSameInstanceAs
import strikt.assertions.isSuccess
import java.util.function.Function

class TestBiDirectionalMessageReceiver {
    private val connectionID = ConnectionID.newBuilder()
            .setSessionAlias("test")
            .build()
    private val manager = SubscriptionManagerImpl()
    private val monitor: ResponseMonitor = mock { }
    private val deliveryMetadata: DeliveryMetadata = mock {  }

    private fun receiver(outgoing: CheckRule, incomingSupplier: (Message) -> CheckRule): AbstractMessageReceiver = BiDirectionalMessageReceiver(
            manager,
            monitor,
            outgoing,
            Function(incomingSupplier)
    )

    @Test
    fun `works in normal case`() {
        val messageA = createDefaultMessage("A", Direction.SECOND).build()
        val messageB = createDefaultMessage("B", Direction.FIRST).build()

        val receiver = receiver(IdentityRule(messageA, connectionID)) { IdentityRule(messageB, connectionID) }
        receiver.use {
            manager.handle(deliveryMetadata, MessageBatch.newBuilder().addMessages(messageA).build())
            manager.handle(deliveryMetadata, MessageBatch.newBuilder().addMessages(messageB).build())
        }

        expect {
            that(receiver).apply {
                get { responseMessage }.isNotNull()
                        .isSameInstanceAs(messageB)
                get { processedMessageIDs() }.containsExactlyInAnyOrder(messageA.metadata.id, messageB.metadata.id)
            }
            catching { verify(monitor).responseReceived() }.isSuccess()
        }
    }

    @Test
    fun `works if incoming is processed before outgoing`() {
        val messageA = createDefaultMessage("A", Direction.SECOND).build()
        val messageB = createDefaultMessage("B", Direction.FIRST).build()

        val receiver = receiver(IdentityRule(messageA, connectionID)) { IdentityRule(messageB, connectionID) }
        receiver.use {
            manager.handle(deliveryMetadata, MessageBatch.newBuilder().addMessages(messageB).build())
            manager.handle(deliveryMetadata, MessageBatch.newBuilder().addMessages(messageA).build())
        }

        expect {
            that(receiver).apply {
                get { responseMessage }.isNotNull()
                        .isSameInstanceAs(messageB)
                get { processedMessageIDs() }.containsExactlyInAnyOrder(messageA.metadata.id, messageB.metadata.id)
            }
            catching { verify(monitor).responseReceived() }.isSuccess()
        }
    }

    @Test
    fun `works if incoming received after the buffered message`() {
        val messageA = createDefaultMessage("A", Direction.SECOND).build()
        val messageB = createDefaultMessage("B", Direction.FIRST).build()
        val messageC = createDefaultMessage("C", Direction.FIRST).build()

        val receiver = receiver(IdentityRule(messageA, connectionID)) { IdentityRule(messageC, connectionID) }
        receiver.use {
            manager.handle(deliveryMetadata, MessageBatch.newBuilder().addMessages(messageB).build())
            manager.handle(deliveryMetadata, MessageBatch.newBuilder().addMessages(messageA).build())
            manager.handle(deliveryMetadata, MessageBatch.newBuilder().addMessages(messageC).build())
        }

        expect {
            that(receiver).apply {
                get { responseMessage }.isNotNull()
                        .isSameInstanceAs(messageC)
                get { processedMessageIDs() }.containsExactlyInAnyOrder(messageA.metadata.id, messageB.metadata.id, messageC.metadata.id)
            }
            catching { verify(monitor).responseReceived() }.isSuccess()
        }
    }

    private class IdentityRule(
            private val message: Message,
            connectionId: ConnectionID
    ) : AbstractSingleConnectionRule(connectionId) {
        override fun checkMessageFromConnection(message: Message): Boolean = this.message === message
    }
}