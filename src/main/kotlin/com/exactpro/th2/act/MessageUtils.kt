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

import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.common.utils.event.toTransport
import com.exactpro.th2.common.utils.message.toTransport
import io.netty.buffer.Unpooled
import com.exactpro.th2.common.grpc.RawMessage as ProtoRawMessage

// TODO: move to common-utils

fun ProtoRawMessage.toTransportBuilder(): RawMessage.Builder = RawMessage.builder().apply {
    with(metadata) {
        setId(id.toTransport())
        setProtocol(protocol)
        setMetadata(propertiesMap)
    }
    setBody(Unpooled.wrappedBuffer(this@toTransportBuilder.body.asReadOnlyByteBuffer()))
    if (hasParentEventId()) {
        setEventId(parentEventId.toTransport())
    }
}