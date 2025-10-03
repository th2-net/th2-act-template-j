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
package com.exactpro.th2.act.rules

import com.exactpro.th2.act.util.TEST_BOOK
import com.exactpro.th2.act.util.TEST_SCOPE
import com.exactpro.th2.act.util.TEST_SESSION_ALIAS
import com.exactpro.th2.act.util.TEST_SESSION_GROUP
import com.exactpro.th2.act.util.createTransportMessage
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.EventId
import com.exactpro.th2.common.utils.event.toTransport
import com.exactpro.th2.common.utils.message.TransportMessageHolder
import com.exactpro.th2.common.utils.message.toTimestamp
import org.junit.jupiter.api.Test
import strikt.api.expect
import strikt.assertions.isFalse
import strikt.assertions.isNotNull
import strikt.assertions.isNull
import strikt.assertions.isSameInstanceAs
import strikt.assertions.isTrue
import java.time.Instant
import java.util.concurrent.ThreadLocalRandom

class TestParentIdCheckRule {
    private val parentId = EventID.newBuilder().apply {
        id = "test"
        bookName = TEST_BOOK
        scope = TEST_SCOPE
        startTimestamp = Instant.now().toTimestamp()
    }.build()
    private val connectionId = ConnectionID.newBuilder()
        .setSessionAlias(TEST_SESSION_ALIAS)
        .build()
    private val rule = ParentIdCheckRule(parentId, connectionId)

    @Test
    fun `finds match`() {
        val message = TransportMessageHolder(
            createTransportMessage()
                .setEventId(parentId.toTransport())
                .build(), TEST_BOOK, TEST_SESSION_GROUP
        )

        expect {
            that(rule.onMessage(message)).isTrue()
            that(rule.response)
                .isNotNull()
                .isSameInstanceAs(message)
        }
    }

    @Test
    fun `skips messages with different parent ID`() {
        val message = TransportMessageHolder(
            createTransportMessage()
                .setEventId(
                    EventId(
                        ThreadLocalRandom.current().nextLong().toString(),
                        TEST_BOOK,
                        TEST_SCOPE,
                        Instant.now()
                    )
                )
                .build(), TEST_BOOK, TEST_SESSION_GROUP
        )

        expect {
            that(rule.onMessage(message)).isFalse()
            that(rule.response).isNull()
        }
    }
}