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
package com.exactpro.th2.act.rules;

import static com.google.protobuf.TextFormat.shortDebugString;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isEmpty;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.act.CheckMetadata;
import com.exactpro.th2.act.ActUtils;
import com.exactpro.th2.act.FieldNotFoundException;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageOrBuilder;
import com.exactpro.th2.common.grpc.Value;

public class FieldCheckRule extends AbstractSingleConnectionRule {
    private static final Logger LOGGER = LoggerFactory.getLogger(FieldCheckRule.class);
    private final String expectedFieldValue;
    private final Map<String, CheckMetadata> expectedMessages;

    public FieldCheckRule(String expectedFieldValue, Map<String, CheckMetadata> expectedMessages, ConnectionID requestConnId) {
        super(requestConnId);
        this.expectedFieldValue = expectedFieldValue;
        checkIfAnyBlank(expectedMessages);
        this.expectedMessages = expectedMessages;
    }

    /**
     * @param expectedMessages Map msgType to CheckMetadata(fieldPath)
     * @throws IllegalArgumentException if any msgType or fieldPath is blank
     */
    private void checkIfAnyBlank(Map<String, CheckMetadata> expectedMessages) {
        expectedMessages.forEach((msgType, value) -> {
            String[] fieldPath = value.getFieldPath();
            if (isEmpty(msgType) || ArrayUtils.isEmpty(fieldPath)) {
                throw new IllegalArgumentException(format(
                        "'msgTypeToFieldName' mapping must not contain blank values. MsgType: '%s', FieldPath: '%s'",
                        msgType, Arrays.toString(fieldPath))
                );
            }
        });
    }

    @Override
    protected boolean checkMessageFromConnection(Message message) {
        String messageType = message.getMetadata().getMessageType();
        CheckMetadata checkMetadata = expectedMessages.get(messageType);
        if (checkMetadata != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Checking the message: {}", shortDebugString(message));
            }
            if (checkExpectedField(message, checkMetadata)) {
                LOGGER.debug("FieldCheckRule passed on {} messageType", messageType);
                return true;
            }
        }
        return false;
    }

    private boolean checkExpectedField(MessageOrBuilder message, CheckMetadata checkMetadata) {
        Value value;
        List<String> fieldPath = Arrays.asList(checkMetadata.getFieldPath());
        try {
            value = ActUtils.getMatchingValue(message, fieldPath);
        } catch (FieldNotFoundException e) {
            LOGGER.error("Failed to find matching field path: " + fieldPath, e);
            value = null;
        }
        return value != null
                && Objects.equals(expectedFieldValue, value.getSimpleValue());
    }
}
