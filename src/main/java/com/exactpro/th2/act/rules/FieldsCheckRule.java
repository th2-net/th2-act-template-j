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
package com.exactpro.th2.act.rules;

import static com.google.protobuf.TextFormat.shortDebugString;

import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.act.ActUtils;
import com.exactpro.th2.act.FieldNotFoundException;
import com.exactpro.th2.act.ResponseMapper;
import com.exactpro.th2.act.ResponseMapper.FieldPath;
import com.exactpro.th2.act.ResponseMapper.ResponseStatus;
import com.exactpro.th2.act.ResponseMapper.ValueMatcher;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageOrBuilder;
import com.exactpro.th2.common.grpc.Value;

public class FieldsCheckRule extends AbstractSingleConnectionRule {
    private static final Logger LOGGER = LoggerFactory.getLogger(FieldsCheckRule.class);
    private final List<ResponseMapper> responseMapping;
    private ResponseStatus responseStatus;

    public FieldsCheckRule(List<ResponseMapper> responseMapping, ConnectionID requestConnId) {
        super(requestConnId);
        this.responseMapping = responseMapping;
    }

    private static boolean checkExpectedFields(MessageOrBuilder message, Map<FieldPath, ValueMatcher> matchingFields) {
        for (var entry : matchingFields.entrySet()) {
            List<String> fieldPath = entry.getKey().getPath();
            Value value;
            try {
                value = ActUtils.getMatchingValue(message, fieldPath);
            } catch (FieldNotFoundException e) {
                LOGGER.debug("Failed to find matching field path: " + fieldPath, e);
                value = null;
            }
            if (value == null || !entry.getValue().isMatches(value.getSimpleValue())) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected boolean checkMessageFromConnection(Message message) {
        String messageType = message.getMetadata().getMessageType();
        for (ResponseMapper responseMapper : responseMapping) {
            if (!responseMapper.getResponseType().equals(messageType)) {
                continue;
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Checking the message: {}", shortDebugString(message));
            }

            if (checkExpectedFields(message, responseMapper.getMatchingFields())) {
                LOGGER.debug("FieldsCheckRule passed on {} messageType", messageType);
                this.responseStatus = responseMapper.getStatus();
                return true;
            }
        }

        return false;
    }

    /**
     * Matched responseStatus
     * @return the matched responseStatus or {@code null}
     */
    @Nullable
    @Override
    public ResponseStatus getResponseStatus() {
        return responseStatus;
    }
}
