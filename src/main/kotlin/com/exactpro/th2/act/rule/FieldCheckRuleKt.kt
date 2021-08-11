/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.act.rule

import com.exactpro.th2.act.rules.AbstractSingleConnectionRule
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageOrBuilder
import com.google.protobuf.TextFormat
import mu.KotlinLogging
import org.apache.commons.lang3.StringUtils

class FieldCheckRuleKt(private val expectedFieldValue: String, private val msgTypeToFieldName: Map<String, String>, requestConnId: ConnectionID) : AbstractSingleConnectionRule(requestConnId) {

    companion object {
        private val LOGGER = KotlinLogging.logger {}
    }

    init {
        msgTypeToFieldName.forEach { (msgType: String, fieldName: String) ->
            require(!StringUtils.isAnyBlank(msgType, fieldName)) { "'msgTypeToFieldName' mapping must not contain blank values. MsgType: '$msgType' FieldName: '$fieldName'" }
        }
    }

    override fun checkMessageFromConnection(message: Message): Boolean {
        val messageType = message.metadata.messageType
        val fieldName = msgTypeToFieldName[messageType]
        if (fieldName != null) {
            LOGGER.debug { "Checking the message: ${TextFormat.shortDebugString(message)}" }
            if (checkExpectedField(message, fieldName)) {
                LOGGER.debug { "FixCheckRule passed on $messageType messageType" }
                return true
            }
        }
        return false
    }

    private fun checkExpectedField(message: MessageOrBuilder, fieldName: String): Boolean {
        val value = message.fieldsMap[fieldName]
        return value != null && expectedFieldValue == value.simpleValue
    }

}