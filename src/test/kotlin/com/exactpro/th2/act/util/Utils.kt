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
package com.exactpro.th2.act.util

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.value.toValue
import java.time.Instant
import com.exactpro.th2.common.grpc.Direction as ProtoDirection

const val TEST_BOOK = "test-book"
const val TEST_SCOPE = "test-scope"
const val TEST_SESSION_GROUP = "test-session-group"
const val TEST_SESSION_ALIAS = "test-session-alias"

fun createTransportMessage(
    messageType: String = "test",
    direction: Direction = Direction.INCOMING,
) = ParsedMessage.builder().apply {
    setType(messageType)
    idBuilder().apply {
        setSessionAlias(TEST_SESSION_ALIAS)
        setTimestamp(Instant.now())
        setDirection(direction)
    }
    addField("test-field", "test-value")
}

fun createProtoMessage(
    messageType: String = "test",
    direction: ProtoDirection = ProtoDirection.FIRST,
): Message.Builder = Message.newBuilder().apply {
    metadataBuilder.apply {
        this.messageType = messageType
        idBuilder.apply {
            bookName = TEST_BOOK
            connectionIdBuilder.apply {
                sessionGroup = TEST_SESSION_GROUP
                sessionAlias = TEST_SESSION_ALIAS
            }
            this.direction = direction
            timestamp = Instant.now().toTimestamp()
        }
    }
    putFields("test-field", "test-value".toValue())
}