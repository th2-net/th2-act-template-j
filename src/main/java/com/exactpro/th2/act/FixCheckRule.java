/******************************************************************************
 * Copyright 2009-2020 Exactpro (Exactpro Systems Limited)
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
 ******************************************************************************/
package com.exactpro.th2.act;

import com.exactpro.th2.infra.grpc.Message;
import com.exactpro.th2.infra.grpc.MessageOrBuilder;
import com.exactpro.th2.infra.grpc.Value;
import com.google.protobuf.TextFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class FixCheckRule implements CheckRule {
    private final Logger logger = LoggerFactory.getLogger(getClass().getName() + '@' + hashCode());
    private final String expectedFieldName;
    private final String expectedFieldValue;
    private final Set<String> expectedMessageTypes;

    private final AtomicReference<Message> response = new AtomicReference<>();

    public FixCheckRule(String expectedFieldName, String expectedFieldValue, Set<String> expectedMessageTypes) {
        this.expectedFieldName = expectedFieldName;
        this.expectedFieldValue = expectedFieldValue;
        this.expectedMessageTypes = expectedMessageTypes;
    }

    @Override
    public boolean onMessage(Message message) {
        String messageType = message.getMetadata().getMessageType();
        if (expectedMessageTypes.contains(messageType)) {
            if(logger.isDebugEnabled()) { logger.debug("check the message: " + TextFormat.shortDebugString(message)); }
            if (checkExpectedField(message)) {
                response.set(message);
                logger.debug("FixCheckRule passed on {} messageType", messageType);
                return true;
            }
        }
        return false;
    }

    @Override
    public Message getResponse() {
        return response.get();
    }

    private boolean checkExpectedField(MessageOrBuilder message) {
        Value value = message.getFieldsMap().get(expectedFieldName);
        return value != null && expectedFieldValue.equals(value.getSimpleValue());
    }
}