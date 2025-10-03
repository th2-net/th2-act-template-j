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
import com.exactpro.th2.act.util.TEST_SESSION_ALIAS
import com.exactpro.th2.act.util.TEST_SESSION_GROUP
import com.exactpro.th2.act.util.createTransportMessage
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.utils.message.TransportMessageHolder
import org.junit.jupiter.api.Test
import strikt.api.expect
import strikt.assertions.isFalse
import strikt.assertions.isNotNull
import strikt.assertions.isNull
import strikt.assertions.isSameInstanceAs
import strikt.assertions.isTrue

class TestMessagePropertiesCheckRule {
    private val connectionId = ConnectionID.newBuilder()
        .setSessionAlias(TEST_SESSION_ALIAS)
        .build()
    private val rule = MessagePropertiesCheckRule(
        connectionId, mapOf(
            "prop1" to "value1",
            "prop2" to "value2"
        )
    )

    @Test
    fun `finds match`() {
        val message = TransportMessageHolder(
            createTransportMessage()
                .addMetadataProperty("prop1", "value1")
                .addMetadataProperty("prop2", "value2")
                .addMetadataProperty("prop3", "value3")
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
    fun `skips messages if any property is not matched`() {
        val message = TransportMessageHolder(
            createTransportMessage()
                .addMetadataProperty("prop1", "value1")
                .addMetadataProperty("prop2", "value3")
                .addMetadataProperty("prop3", "value3")
                .build(), TEST_BOOK, TEST_SESSION_GROUP
        )

        expect {
            that(rule.onMessage(message)).isFalse()
            that(rule.response).isNull()
        }
    }

    @Test
    fun `skips messages if any property is missed`() {
        val message = TransportMessageHolder(
            createTransportMessage()
                .addMetadataProperty("prop1", "value1")
                .addMetadataProperty("prop3", "value3")
                .build(), TEST_BOOK, TEST_SESSION_GROUP
        )

        expect {
            that(rule.onMessage(message)).isFalse()
            that(rule.response).isNull()
        }
    }
}