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

import com.exactpro.th2.common.event.bean.TreeTable
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Message
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.utils.event.transport.toProto
import com.exactpro.th2.common.utils.message.MessageHolder
import com.exactpro.th2.common.utils.message.ProtoMessageHolder
import com.exactpro.th2.common.utils.message.TransportMessageHolder
import com.exactpro.th2.common.utils.message.id
import com.exactpro.th2.common.utils.message.parentEventId
import com.exactpro.th2.common.utils.message.transport.toProto
import com.exactpro.th2.common.grpc.MessageGroup as ProtoGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup as TransportGroup

sealed interface GroupHolder : Iterable<MessageHolder?> {
    val id: MessageID
    val eventId: EventID?
    val size: Int

    /**
     * Returns [MessageHolder] if index < size and message is parsed otherwise return null
     */
    operator fun get(index: Int): MessageHolder?

    fun toTreeTable(): TreeTable
}

@Suppress("unused")
class ProtoGroupHolder(
    private val group: ProtoGroup
) : GroupHolder {
    override val id: MessageID
        get() = group.getMessages(0).id
    override val eventId: EventID?
        get() = group.getMessages(0).parentEventId
    override val size: Int
        get() = group.messagesCount

    override fun get(index: Int): MessageHolder? {
        if (index < 0 && size <= index) {
            return null
        }
        return toMessageHolder(group.getMessages(index))
    }

    override fun toTreeTable(): TreeTable {
        return group.messagesList.asSequence()
            .map { anyMessage -> if (anyMessage.kindCase == AnyMessage.KindCase.MESSAGE) anyMessage.message else null }
            .filterNotNull()
            .toList().toTreeTable()
    }

    override fun iterator(): Iterator<MessageHolder?> {
        return group.messagesList.asSequence().map(::toMessageHolder).iterator()
    }

    companion object {
        private fun toMessageHolder(anyMessage: AnyMessage): MessageHolder? =
            if (anyMessage.kindCase == AnyMessage.KindCase.MESSAGE) ProtoMessageHolder(anyMessage.message) else null
    }

}

class TransportGroupHolder(
    private val group: TransportGroup,
    private val book: String,
    private val sessionGroup: String
) : GroupHolder {
    override val id: MessageID
        get() = group.messages.first().id.toProto(book, sessionGroup)
    override val eventId: EventID?
        get() = group.messages.first().eventId?.toProto()
    override val size: Int
        get() = group.messages.size

    override fun get(index: Int): MessageHolder? {
        if (index < 0 && size <= index) {
            return null
        }
        return toMessageHolder(group.messages[index])
    }

    override fun toTreeTable(): TreeTable {
        return group.messages.asSequence()
            .map { message -> message as? ParsedMessage }
            .filterNotNull()
            .toList().toTreeTable()
    }

    override fun iterator(): Iterator<MessageHolder?> {
        return group.messages.asSequence().map(::toMessageHolder).iterator()
    }

    private fun toMessageHolder(message: Message<*>): TransportMessageHolder? =
        if (message is ParsedMessage) TransportMessageHolder(message, book, sessionGroup) else null
}