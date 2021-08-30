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

package com.exactpro.th2.act.utils;

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
import com.exactpro.th2.common.grpc.Value;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.util.Map;
import java.util.Map.Entry;

import static com.exactpro.th2.common.event.Event.Status.PASSED;
import static com.exactpro.th2.common.event.Event.start;

public class EventUtils {
    public static TreeTable toTreeTable(Message message) {
        TreeTableBuilder treeTableBuilder = new TreeTableBuilder();
        for (Entry<String, Value> fieldEntry : message.getFieldsMap().entrySet()) {
            treeTableBuilder.row(fieldEntry.getKey(), toTreeTableEntry(fieldEntry.getValue()));
        }
        return treeTableBuilder.build();
    }

    private static TreeTableEntry toTreeTableEntry(Value fieldValue) {
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

    public static IBodyData createNoResponseBody(Map<String, CheckMetadata> expectedMessages, String fieldValue) {
        TreeTableBuilder treeTableBuilder = new TreeTableBuilder();
        CollectionBuilder passedOn = new CollectionBuilder();
        CollectionBuilder failedOn = new CollectionBuilder();
        for (Map.Entry<String, CheckMetadata> entry : expectedMessages.entrySet()) {
            if (entry.getValue().getEventStatus() == PASSED) {
                passedOn.row(entry.getKey(), new CollectionBuilder().row(entry.getValue().getFieldName(), new RowBuilder().column(new EventUtils.MessageTableColumn(fieldValue)).build()).build());
            } else  {
                failedOn.row(entry.getKey(), new CollectionBuilder().row(entry.getValue().getFieldName(), new RowBuilder().column(new EventUtils.MessageTableColumn(fieldValue)).build()).build());
            }
        }
        treeTableBuilder.row("PASSED on:", passedOn.build());
        treeTableBuilder.row("FAILED on:", failedOn.build());

        return treeTableBuilder.build();
    }

    public static com.exactpro.th2.common.grpc.Event createSendMessageEvent(Message message, EventID parentEventId) throws JsonProcessingException {
        Event event = start()
                .name("Send '" + message.getMetadata().getMessageType() + "' message to connectivity");
        TreeTable parametersTable = EventUtils.toTreeTable(message);
        event.status(Event.Status.PASSED);
        event.bodyData(parametersTable);
        event.type("Outgoing message");
        return event.toProto(parentEventId);
    }

    public static class MessageTableColumn implements IColumn {
        public final String fieldValue;

        public MessageTableColumn(String fieldValue) {
            this.fieldValue = fieldValue;
        }
    }
}
