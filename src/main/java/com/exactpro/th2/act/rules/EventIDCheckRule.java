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

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.act.ResponseMapper.ResponseStatus;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.Message;

public class EventIDCheckRule extends AbstractSingleConnectionRule {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventIDCheckRule.class);
    private final String eventID;
    private final Direction direction;

    public EventIDCheckRule(String eventID, ConnectionID requestConnId, Direction direction) {
        super(requestConnId);
        this.direction = direction;
        this.eventID = eventID;
    }

    @Override
    protected boolean checkMessageFromConnection(Message message) {
        String incEventID = message.getParentEventId().getId();
        if (incEventID.equals(eventID) && message.getMetadata().getId().getDirection() == direction) {
            LOGGER.debug("EventIDCheckRule passed on {} messageType", message.getMetadata().getMessageType());
            return true;
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
        return null;
    }
}
