/*
 * Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.act.util.createDefaultMessage
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.MessageBatch
import com.exactpro.th2.common.schema.message.DeliveryMetadata
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.never
import com.nhaarman.mockitokotlin2.same
import com.nhaarman.mockitokotlin2.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

class TestSubscriptionManagerImpl {

    private val manager = SubscriptionManagerImpl()
    private val deliveryMetadata: DeliveryMetadata = mock {  }

    @Test
    fun `correctly distributes the batches`() {
        val listeners: Map<Direction, Listener> = mapOf(
                Direction.FIRST to mock { },
                Direction.SECOND to mock { }
        )
        listeners.forEach { (dir, listener) -> manager.register(dir, listener) }

        val batch = MessageBatch.newBuilder()
                .addMessages(createDefaultMessage(direction = Direction.FIRST))
                .addMessages(createDefaultMessage(direction = Direction.SECOND))
                .build()
        manager.handle(deliveryMetadata, batch)

        Assertions.assertAll(
            { verify(listeners[Direction.FIRST])?.handle(same(batch.getMessages(0))) },
            { verify(listeners[Direction.SECOND])?.handle(same(batch.getMessages(1))) },
        )
    }

    @ParameterizedTest
    @EnumSource(value = Direction::class, mode = EnumSource.Mode.EXCLUDE, names = ["UNRECOGNIZED"])
    fun `removes listener`(direction: Direction) {
        val listener = mock<Listener> { }
        manager.register(direction, listener)


        manager.unregister(direction, listener)

        val batch = MessageBatch.newBuilder()
                .addMessages(createDefaultMessage(direction = direction))
                .build()
        manager.handle(deliveryMetadata, batch)

        verify(listener, never()).handle(any())
    }
}