package com.exactpro.th2.act;

import static com.exactpro.th2.common.event.Event.Status.FAILED;
import static com.exactpro.th2.common.event.Event.Status.PASSED;
import static com.exactpro.th2.common.grpc.RequestStatus.Status.ERROR;
import static com.exactpro.th2.common.grpc.RequestStatus.Status.SUCCESS;
import static java.util.Objects.requireNonNull;

import com.exactpro.th2.common.event.Event.Status;
import com.exactpro.th2.common.grpc.RequestStatus;

public class CheckMetadata {
    private final Status eventStatus;
    private final RequestStatus.Status requestStatus;
    private final String[] fieldPath;

    private CheckMetadata(Status eventStatus, String... fieldPath) {
        this.eventStatus = requireNonNull(eventStatus, "Event status can't be null");
        this.fieldPath = requireNonNull(fieldPath, "Field path can't be null");

        switch (eventStatus) {
        case PASSED:
            requestStatus = SUCCESS;
            break;
        case FAILED:
            requestStatus = ERROR;
            break;
        default:
            throw new IllegalArgumentException("Event status '" + eventStatus + "' can't be convert to request status");
        }
    }

    public static CheckMetadata passOn(String... fieldName) {
        return new CheckMetadata(PASSED, fieldName);
    }

    public static CheckMetadata failOn(String... fieldName) {
        return new CheckMetadata(FAILED, fieldName);
    }

    public Status getEventStatus() {
        return eventStatus;
    }

    public RequestStatus.Status getRequestStatus() {
        return requestStatus;
    }

    public String[] getFieldPath() {
        return fieldPath;
    }
}
