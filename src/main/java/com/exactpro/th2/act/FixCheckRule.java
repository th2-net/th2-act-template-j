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

import com.exactpro.th2.act.grpc.PlaceMessageRequest;
import com.exactpro.th2.infra.grpc.Message;
import com.exactpro.th2.infra.grpc.MessageOrBuilder;
import com.exactpro.th2.infra.grpc.Value;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.protobuf.TextFormat.*;

public class FixCheckRule implements CheckRule {
    private final Logger logger = LoggerFactory.getLogger(getClass().getName() + '@' + hashCode());
    private final String expectedFieldName;
    private final String expectedFieldValue;
    private final Set<String> expectedMessageTypes;
    private final PlaceMessageRequest request;

    private final AtomicReference<Message> response = new AtomicReference<>();

    public FixCheckRule(String expectedFieldName, String expectedFieldValue, Set<String> expectedMessageTypes, PlaceMessageRequest request) {
        this.expectedFieldName = expectedFieldName;
        this.expectedFieldValue = expectedFieldValue;
        this.expectedMessageTypes = expectedMessageTypes;
        this.request = request;
    }

    @Override
    public boolean onMessage(Message message) {
        String messageType = message.getMetadata().getMessageType();
        if (checkSessionAlias(message) && expectedMessageTypes.contains(messageType)) {
            if(logger.isDebugEnabled()) { logger.debug("check the message: {}", shortDebugString(message)); }
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

    private boolean checkSessionAlias(Message message) {

        var actualSessionAlias = message.getMetadata().getId().getConnectionId().getSessionAlias();

        var requestMsgSessionAlias = request.getMessage().getMetadata().getId().getConnectionId().getSessionAlias();

        if(StringUtils.isEmpty(requestMsgSessionAlias)){

            var requestSessionAlias = request.getConnectionId().getSessionAlias();

            return requestSessionAlias.equals(actualSessionAlias);
        }

        return requestMsgSessionAlias.equals(actualSessionAlias);

    }

}
