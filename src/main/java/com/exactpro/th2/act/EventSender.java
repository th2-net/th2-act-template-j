package com.exactpro.th2.act;

import static com.exactpro.th2.common.event.Event.Status.FAILED;
import static com.exactpro.th2.common.event.Event.start;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.common.event.Event;
import com.exactpro.th2.common.event.Event.Status;
import com.exactpro.th2.common.event.IBodyData;
import com.exactpro.th2.common.event.bean.builder.MessageBuilder;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;

public class EventSender {
    //TODO more wildcards
    private static final String DESCRIPTIONONLY_WILDCARD = "!exact:";
    private static final Logger LOGGER = LoggerFactory.getLogger(ActHandler.class);
    private final MessageRouter<EventBatch> eventBatchMessageRouter;

    EventSender(MessageRouter<EventBatch> eventBatchRouter) {
        this.eventBatchMessageRouter = requireNonNull(eventBatchRouter, "'Event batch router' parameter");
    }

    private static String toDebugMessage(MessageOrBuilder messageOrBuilder) throws InvalidProtocolBufferException {
        return JsonFormat.printer().omittingInsignificantWhitespace().print(messageOrBuilder);
    }

    public void storeEvent(com.exactpro.th2.common.grpc.Event eventRequest) {
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Try to store event: {}", toDebugMessage(eventRequest));
            }
            eventBatchMessageRouter.send(EventBatch.newBuilder().addEvents(eventRequest).build(), "publish", "event");
        } catch (Exception e) {
            LOGGER.error("Could not store event", e);
        }
    }

    public EventID createAndStoreParentEvent(String description, String actName, String alias, Status status, EventID parentID) throws JsonProcessingException {
        long startTime = System.currentTimeMillis();
        Event event = start()
                .name(description.startsWith(DESCRIPTIONONLY_WILDCARD) ? description.replace(DESCRIPTIONONLY_WILDCARD, "") : (actName + ':' + alias))
                .description(description.startsWith(DESCRIPTIONONLY_WILDCARD) ? "" : description)
                .type(actName + ':' + alias)
                .status(status)
                .endTimestamp();
        com.exactpro.th2.common.grpc.Event protoEvent = event.toProto(parentID);
        storeEvent(protoEvent);
        LOGGER.debug("createAndStoreParentEvent for {} in {} ms", actName, System.currentTimeMillis() - startTime);
        return protoEvent.getId();
    }

    public void createAndStoreNoResponseEvent(String actName, NoResponseBodySupplier noResponseBodySupplier,
            Instant start,
            EventID parentEventId, Iterable<MessageID> messageIDList) throws JsonProcessingException {

        Event errorEvent = Event.from(start)
                .endTimestamp()
                .name("No response found by target keys.")
                .type(format("%s error", actName))
                .status(FAILED);
        for (IBodyData data : noResponseBodySupplier.createNoResponseBody()) {
            errorEvent.bodyData(data);
        }
        for (MessageID msgID : messageIDList) {
            errorEvent.messageID(msgID);
        }
        storeEvent(errorEvent.toProto(parentEventId));
    }

    public void createAndStoreErrorEvent(String actName, String message, Instant start, EventID parentEventId) throws JsonProcessingException {
        Event errorEvent = Event.from(start)
                .endTimestamp()
                .name(format("Internal %s error", actName))
                .type(format("%s error", actName))
                .status(FAILED)
                .bodyData(new MessageBuilder().text(message).build());
        storeEvent(errorEvent.toProto(parentEventId));
    }

    public void createAndStoreSendingFailedEvent(String actName, String message, Instant start, EventID parentEventId) throws JsonProcessingException {
        Event errorEvent = Event.from(start)
                .endTimestamp()
                .name("Unable to send the message.")
                .type(format("%s error", actName))
                .status(FAILED)
                .bodyData(new MessageBuilder().text(message).build());
        storeEvent(errorEvent.toProto(parentEventId));
    }
}
