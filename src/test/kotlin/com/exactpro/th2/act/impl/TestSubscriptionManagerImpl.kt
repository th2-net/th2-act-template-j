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

package com.exactpro.th2.act.impl

import com.exactpro.th2.act.Listener
import com.exactpro.th2.act.util.TEST_BOOK
import com.exactpro.th2.act.util.TEST_SESSION_GROUP
import com.exactpro.th2.act.util.createTransportMessage
import com.exactpro.th2.common.schema.message.DeliveryMetadata
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.utils.message.TransportMessageHolder
import com.exactpro.th2.common.utils.message.transport.toBatch
import com.exactpro.th2.common.utils.message.transport.toGroup
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

class TestSubscriptionManagerImpl {

    private val manager = SubscriptionManagerImpl()
    private val deliveryMetadata = DeliveryMetadata("test_tag_1", false)

    @Test
    fun `correctly distributes the batches`() {
        val listeners: Map<Direction, Listener> = mapOf(
            Direction.INCOMING to mock { },
            Direction.OUTGOING to mock { }
        )
        listeners.forEach { (dir, listener) -> manager.register(dir, listener) }

        val batch = GroupBatch.builder().apply {
            setBook(TEST_BOOK)
            setSessionGroup(TEST_SESSION_GROUP)
            groupsBuilder().apply {
                addGroup(createTransportMessage(direction = Direction.INCOMING).build().toGroup())
                addGroup(createTransportMessage(direction = Direction.OUTGOING).build().toGroup())
            }
        }.build()
        manager.handle(deliveryMetadata, batch)

        Assertions.assertAll(
            {
                verify(listeners[Direction.INCOMING])?.handle(
                    eq(
                        TransportMessageHolder(
                            batch.groups[0].messages.first() as ParsedMessage,
                            TEST_BOOK,
                            TEST_SESSION_GROUP
                        )
                    )
                )
            },
            {
                verify(listeners[Direction.OUTGOING])?.handle(
                    eq(
                        TransportMessageHolder(
                            batch.groups[1].messages.first() as ParsedMessage,
                            TEST_BOOK,
                            TEST_SESSION_GROUP
                        )
                    )
                )
            },
        )
    }

    @ParameterizedTest
    @EnumSource(value = Direction::class)
    fun `removes listener`(direction: Direction) {
        val listener = mock<Listener> { }
        manager.register(direction, listener)

        manager.unregister(direction, listener)

        val batch =
            createTransportMessage(direction = direction).build().toGroup().toBatch(TEST_BOOK, TEST_SESSION_GROUP)
        manager.handle(deliveryMetadata, batch)

        verify(listener, never()).handle(any())
    }
}