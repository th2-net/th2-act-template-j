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
package com.exactpro.th2.act;

import static java.lang.String.format;

import java.util.List;

import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageOrBuilder;
import com.exactpro.th2.common.grpc.Value;

public class ActUtils {

    public static Value getMatchingValue(MessageOrBuilder message, List<String> path) throws FieldNotFoundException {
        String curField = path.get(0);
        Value value = message.getFieldsMap().get(curField);
        if (value == null) {
            throw new FieldNotFoundException(format("Field %s is not found in message: %s", path.get(0), message.getFieldsMap().keySet()));
        }
        if (value.hasListValue()) {
            if (path.size() < 2) {
                throw new FieldNotFoundException(format("Failed to parse pointer to list item: %s is a ListValue, unexpected end of the path: %s", path.get(0), path));
            }
            String pointerString = path.get(1);
            int listPointer;
            try {
                listPointer = Integer.parseInt(pointerString);
            } catch (NumberFormatException e) {
                throw new FieldNotFoundException(format("Failed to parse pointer to list item: %s is a ListValue, number is expected as next path value, got: %s", path.get(0), path.get(1)), e);
            }
            path = path.subList(1, path.size());
            value = value.getListValue().getValues(listPointer);
        }
        if (path.size() == 1) {
            return value;
        }
        if (!value.hasMessageValue()) {
            throw new FieldNotFoundException(format("Field %s is not MessageValue", curField));
        }
        Message subMessage = value.getMessageValue();
        List<String> subPath = path.subList(1, path.size());
        return getMatchingValue(subMessage, subPath);
    }
}
