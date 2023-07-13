/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.act.rules;

import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.utils.message.FieldNotFoundException;
import com.exactpro.th2.common.utils.message.MessageHolder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

public class FieldCheckRule extends AbstractSingleConnectionRule {
    private static final Logger LOGGER = LoggerFactory.getLogger(FieldCheckRule.class);
    private final String expectedFieldValue;
    private final Map<String, String> msgTypeToFieldName;

    public FieldCheckRule(String expectedFieldValue, Map<String, String> msgTypeToFieldName, ConnectionID requestConnId) {
        super(requestConnId);
        this.expectedFieldValue = expectedFieldValue;

        msgTypeToFieldName.forEach((msgType, fieldName) -> {
            if (StringUtils.isAnyBlank(msgType, fieldName)) {
                throw new IllegalArgumentException("'msgTypeToFieldName' mapping must not contain blank values. "
                        + "MsgType: '" + msgType + "'"
                        + "FieldName: '" + fieldName + "'"
                );
            }
        });
        this.msgTypeToFieldName = msgTypeToFieldName;
    }

    @Override
    protected boolean checkMessageFromConnection(MessageHolder message) {
        String messageType = message.getMessageType();
        String fieldName = msgTypeToFieldName.get(messageType);
        if (fieldName != null) {
            LOGGER.debug("Checking the message: {}", message);
            if (checkExpectedField(message, fieldName)) {
                LOGGER.debug("FixCheckRule passed on {} messageType", messageType);
                return true;
            }
        }
        return false;
    }

    private boolean checkExpectedField(MessageHolder message, String fieldName) {
        try {
            String value = message.getSimple(fieldName);
            return Objects.equals(expectedFieldValue, value);
        } catch (FieldNotFoundException e) {
            return false;
        }
    }
}
