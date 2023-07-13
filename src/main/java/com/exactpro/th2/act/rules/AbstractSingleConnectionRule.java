/*
 * Copyright 2021-2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.act.CheckRule;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.utils.message.MessageHolder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static com.exactpro.th2.common.utils.message.MessageHolderUtilsKt.getSessionAlias;

public abstract class AbstractSingleConnectionRule implements CheckRule {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSingleConnectionRule.class);

    private final List<MessageID> messageIDList = new ArrayList<>();
    private final AtomicReference<MessageHolder> response = new AtomicReference<>();
    private final ConnectionID requestConnId;

    public AbstractSingleConnectionRule(ConnectionID requestConnId) {
        this.requestConnId = Objects.requireNonNull(requestConnId, "'Request conn id' parameter");
        if (StringUtils.isBlank(requestConnId.getSessionAlias())) {
            throw new IllegalArgumentException("'sessionAlias' in the requestConnId must not be blank");
        }
    }

    @Override
    public boolean onMessage(MessageHolder message) {
        if (checkSessionAlias(message)) {
            messageIDList.add(message.getId());
            boolean match = checkMessageFromConnection(message);
            if (match) {
                if (response.compareAndSet(null, message)) {
                    LOGGER.debug("Message matches the rule {}. Message: {}", getClass().getSimpleName(), message);
                }
            }
            return match;
        }
        return false;
    }

    @Override
    public List<MessageID> processedIDs() {
        return messageIDList;
    }

    @Override
    @Nullable
    public MessageHolder getResponse() {
        return response.get();
    }

    protected abstract boolean checkMessageFromConnection(MessageHolder message);

    private boolean checkSessionAlias(MessageHolder message) {
        var actualSessionAlias = getSessionAlias(message);
        return requestConnId.getSessionAlias().equals(actualSessionAlias);
    }
}
