/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

import static com.exactpro.th2.common.event.Event.Status.PASSED;
import static com.exactpro.th2.common.event.Event.start;

import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;

import com.exactpro.th2.act.ResponseMapper.ResponseStatus;
import com.exactpro.th2.common.event.Event;
import com.exactpro.th2.common.event.IBodyData;
import com.exactpro.th2.common.event.bean.IColumn;
import com.exactpro.th2.common.event.bean.TreeTable;
import com.exactpro.th2.common.event.bean.TreeTableEntry;
import com.exactpro.th2.common.event.bean.builder.CollectionBuilder;
import com.exactpro.th2.common.event.bean.builder.RowBuilder;
import com.exactpro.th2.common.event.bean.builder.TreeTableBuilder;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageOrBuilder;
import com.exactpro.th2.common.grpc.Value;
import com.exactpro.th2.common.grpc.ValueOrBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;

public class EventUtils {
    public static TreeTable toTreeTable(MessageOrBuilder message) {
        TreeTableBuilder treeTableBuilder = new TreeTableBuilder();
        for (Entry<String, Value> fieldEntry : message.getFieldsMap().entrySet()) {
            treeTableBuilder.row(fieldEntry.getKey(), toTreeTableEntry(fieldEntry.getValue()));
        }
        return treeTableBuilder.build();
    }

    private static TreeTableEntry toTreeTableEntry(ValueOrBuilder fieldValue) {
        if (fieldValue.hasMessageValue()) {
            Message nestedMessageValue = fieldValue.getMessageValue();
            CollectionBuilder collectionBuilder = new CollectionBuilder();
            for (Entry<String, Value> nestedFieldEntry : nestedMessageValue.getFieldsMap().entrySet()) {
                collectionBuilder.row(nestedFieldEntry.getKey(), toTreeTableEntry(
                        nestedFieldEntry.getValue()));
            }
            return collectionBuilder.build();
        }
        if (fieldValue.hasListValue()) {
            int index = 0;
            CollectionBuilder collectionBuilder = new CollectionBuilder();
            for (Value nestedValue : fieldValue.getListValue().getValuesList()) {
                String nestedName = String.valueOf(index++);
                collectionBuilder.row(nestedName, toTreeTableEntry(nestedValue));
            }
            return collectionBuilder.build();
        }
        return new RowBuilder()
                .column(new MessageTableColumn(fieldValue.getSimpleValue()))
                .build();
    }

    public static IBodyData createNoResponseBody(Iterable<ResponseMapper> responseMapping) {
        CollectionBuilder passedOn = new CollectionBuilder();
        CollectionBuilder failedOn = new CollectionBuilder();
        responseMapping.forEach(responseMapper -> {
            CollectionBuilder responseMapperDescription = new CollectionBuilder();
            responseMapper.getMatchingFields().forEach((path, valueMatcher) -> {
                responseMapperDescription.row(StringUtils.join(path.getPath(), "/"),
                        new RowBuilder().column(new MessageTableColumn(valueMatcher.getMatcherDescription())).build());

            });
            if (responseMapper.getStatus() == ResponseStatus.PASSED) {
                passedOn.row(responseMapper.getResponseType(), responseMapperDescription.build());
            } else {
                failedOn.row(responseMapper.getResponseType(), responseMapperDescription.build());
            }
        });
        TreeTableBuilder treeTableBuilder = new TreeTableBuilder();
        treeTableBuilder.row("PASSED on:", passedOn.build());
        treeTableBuilder.row("FAILED on:", failedOn.build());

        return treeTableBuilder.build();
    }

    public static com.exactpro.th2.common.grpc.Event createSendMessageEvent(Message message, EventID parentEventId) throws JsonProcessingException {
        Event event = start()
                .name("Send '" + message.getMetadata().getMessageType() + "' message to connectivity");
        TreeTable parametersTable = toTreeTable(message);
        event.status(PASSED);
        event.bodyData(parametersTable);
        event.type("OutgoingMessage");
        return event.toProto(parentEventId);
    }

    static class MessageTableColumn implements IColumn {
        public final String fieldValue;

        public MessageTableColumn(String fieldValue) {
            this.fieldValue = fieldValue;
        }
    }
}
