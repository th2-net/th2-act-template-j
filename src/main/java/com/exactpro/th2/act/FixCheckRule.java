/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.act;

import static com.google.protobuf.TextFormat.shortDebugString;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageOrBuilder;
import com.exactpro.th2.common.grpc.Value;

public class FixCheckRule implements CheckRule {
    private static final Logger LOGGER = LoggerFactory.getLogger(FixCheckRule.class);
    private final String expectedFieldValue;
    private final Map<String, String> msgTypeToFieldName;
    private final ConnectionID requestConnId;

    private final AtomicReference<Message> response = new AtomicReference<>();

    public FixCheckRule(String expectedFieldValue, Map<String, String> msgTypeToFieldName, ConnectionID requestConnId) {
        this.expectedFieldValue = expectedFieldValue;
        this.msgTypeToFieldName = msgTypeToFieldName;
        this.requestConnId = requestConnId;
    }

    @Override
    public boolean onMessage(Message incomingMessage) {
        String messageType = incomingMessage.getMetadata().getMessageType();
        if (checkSessionAlias(incomingMessage)) {
            String fieldName = msgTypeToFieldName.get(messageType);
            if (fieldName != null) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Checking the message: {}", shortDebugString(incomingMessage));
                }
                if (checkExpectedField(incomingMessage, fieldName)) {
                    // we need to return the first match to the filter
                    response.compareAndSet(null, incomingMessage);
                    LOGGER.debug("FixCheckRule passed on {} messageType", messageType);
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public Message getResponse() {
        return response.get();
    }

    private boolean checkExpectedField(MessageOrBuilder message, String fieldName) {
        Value value = message.getFieldsMap().get(fieldName);
        return value != null
                && Objects.equals(expectedFieldValue, value.getSimpleValue());
    }

    private boolean checkSessionAlias(Message message) {
        var actualSessionAlias = message.getMetadata().getId().getConnectionId().getSessionAlias();
        return requestConnId.getSessionAlias().equals(actualSessionAlias);
    }

}
