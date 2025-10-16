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
import com.exactpro.th2.common.event.bean.TreeTableEntry
import com.exactpro.th2.common.event.bean.builder.CollectionBuilder
import com.exactpro.th2.common.event.bean.builder.RowBuilder
import com.exactpro.th2.common.event.bean.builder.TreeTableBuilder
import com.exactpro.th2.common.grpc.Message as ProtoMessage
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Message
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.common.utils.message.MessageTableColumn
import com.exactpro.th2.common.utils.message.transport.convertToString
import io.netty.handler.codec.base64.Base64
import kotlin.collections.component1
import kotlin.collections.component2
import kotlin.collections.iterator

inline fun treeTable(block: TreeTableBuilder.() -> Unit): TreeTable = TreeTableBuilder().apply(block).build()

inline fun TreeTableBuilder.collection(rowName: String, block: CollectionBuilder.() -> Unit) {
    row(rowName, CollectionBuilder().apply(block).build())
}

fun CollectionBuilder.collection(rowName: String, block: CollectionBuilder.() -> Unit) {
    row(rowName, CollectionBuilder().apply(block).build())
}

fun CollectionBuilder.rowColumn(rowName: String, rowValue: Any?) {
    row(rowName, RowBuilder().column(MessageTableColumn(rowValue.toString())).build())
}

@JvmName("toTreeTableTransport")
fun List<ParsedMessage>.toTreeTable(): TreeTable = treeTable {
    forEachIndexed { index, message ->
        collection(index.toString()) {
            if (message.type.isNotBlank()) rowColumn("type", message.type)
            if (message.protocol.isNotBlank()) rowColumn("protocol", message.protocol)
            if (message.metadata.isNotEmpty()) {
                collection("properties") {
                    for ((key, value) in message.metadata) {
                        row(key, value.toTreeTableEntry())
                    }
                }
            }
            if (message.body.isNotEmpty()) {
                collection("body") {
                    for ((key, value) in message.body) {
                        row(key, value.toTreeTableEntry())
                    }
                }
            }
        }
    }
}

fun List<Message<*>>.toTreeTable(): TreeTable = treeTable {
    forEachIndexed { index, message ->
        collection(index.toString()) {
            if (message is ParsedMessage && message.type.isNotBlank()) rowColumn("type", message.type)
            if (message.protocol.isNotBlank()) rowColumn("protocol", message.protocol)
            if (message.metadata.isNotEmpty()) {
                collection("properties") {
                    for ((key, value) in message.metadata) {
                        row(key, value.toTreeTableEntry())
                    }
                }
            }
            if (message is ParsedMessage && message.body.isNotEmpty()) {
                collection("body") {
                    for ((key, value) in message.body) {
                        row(key, value.toTreeTableEntry())
                    }
                }
            }
            if (message is RawMessage && message.body.readableBytes() > 0) {
                rowColumn("body", Base64.encode(message.body, false).toString(Charsets.UTF_8))
            }
        }
    }
}

@JvmName("toTreeTableProtobuf")
fun List<ProtoMessage>.toTreeTable(): TreeTable = treeTable {
    forEachIndexed { index, message ->
        collection(index.toString()) {
            if (message.metadata.messageType.isNotBlank()) rowColumn("type", message.metadata.messageType)
            if (message.metadata.protocol.isNotBlank()) rowColumn("protocol", message.metadata.protocol)
            if (message.metadata.propertiesCount > 0) {
                collection("properties") {
                    for ((key, value) in message.metadata.propertiesMap) {
                        row(key, value.toTreeTableEntry())
                    }
                }
            }
            if (message.fieldsMap.isNotEmpty()) {
                collection("body") {
                    for ((key, value) in message.fieldsMap) {
                        row(key, value.toTreeTableEntry())
                    }
                }
            }
        }
    }
}

// Copied from common-utils:com.exactpro.th2.common.utils.message.transport.MessageUtils
private fun Any?.toTreeTableEntry(): TreeTableEntry {
    return when (this) {
        null -> RowBuilder()
            .column(MessageTableColumn(null))
            .build()

        is Map<*, *> -> {
            CollectionBuilder().apply {
                forEach { (key, value) ->
                    row(key.toString(), value.toTreeTableEntry())
                }
            }.build()
        }

        is List<*> -> {
            CollectionBuilder().apply {
                forEachIndexed { index, nestedValue ->
                    row(index.toString(), nestedValue.toTreeTableEntry())
                }
            }.build()
        }

        is String -> RowBuilder().column(MessageTableColumn(this)).build()
        is Number -> RowBuilder().column(MessageTableColumn(convertToString())).build()
        else -> error("Unsupported ${this::class.simpleName} number type, value $this")
    }
}

// Copied from common-utils:com.exactpro.th2.common.utils.message.MessageUtils
private fun Value.toTreeTableEntry(): TreeTableEntry = when {
    hasNullValue() -> RowBuilder()
        .column(MessageTableColumn(null))
        .build()

    hasMessageValue() -> CollectionBuilder().apply {
        for ((key, value) in messageValue.fieldsMap) {
            row(key, value.toTreeTableEntry())
        }
    }.build()

    hasListValue() -> CollectionBuilder().apply {
        listValue.valuesList.forEachIndexed { index, nestedValue ->
            val nestedName = index.toString()
            row(nestedName, nestedValue.toTreeTableEntry())
        }
    }.build()

    hasSimpleValue() -> RowBuilder()
        .column(MessageTableColumn(simpleValue))
        .build()

    else -> error("Unsupported type, value ${toJson()}")
}