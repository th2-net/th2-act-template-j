package com.exactpro.th2.act.convertors;

import com.exactpro.th2.act.grpc.NewOrderSingle;
import com.exactpro.th2.act.grpc.PlaceMessageRequestTyped;
import com.exactpro.th2.act.grpc.Quote;
import com.exactpro.th2.act.grpc.SecurityListRequest;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.MessageMetadata;

import java.util.Map;

import static com.exactpro.th2.common.value.ValueUtils.toValue;

public class ConvertorsRequest {

    public Message createMessage(PlaceMessageRequestTyped requestTyped) {
        Message requestMessage;
        switch (requestTyped.getMetadata().getMessageType()) {
            case "NewOrderSingle":
                requestMessage = createNewOrderSingle(requestTyped);
                break;
            case "Quote":
                requestMessage = createQuote(requestTyped);
                break;
            case "SecurityListRequest":
                requestMessage = createSecurityListRequest(requestTyped);
                break;
            default:
                requestMessage = null;
                break;
        }
        return requestMessage;
    }

    private Message.Builder messageBuilder(PlaceMessageRequestTyped requestTyped){
        MessageMetadata metadata = requestTyped.getMetadata();

        return Message.newBuilder()
                .setParentEventId(requestTyped.getParentEventId())
                .setMetadata(MessageMetadata.newBuilder()
                        .setMessageType(metadata.getMessageType())
                        .setId(MessageID.newBuilder()
                                .setConnectionId(ConnectionID.newBuilder()
                                        .setSessionAlias(metadata.getId().getConnectionId().getSessionAlias()))));
    }

    private Message createNewOrderSingle(PlaceMessageRequestTyped requestTyped) {
        NewOrderSingle newOrderSingle = requestTyped.getMessageTyped().getNewOrderSingle();

        return messageBuilder(requestTyped).putAllFields(
                Map.of("Price", toValue(newOrderSingle.getPrice()),
                        "OrderQty", toValue(newOrderSingle.getOrderQty()),
                        "Side", toValue(newOrderSingle.getSide()),
                        "TimeInForce", toValue(newOrderSingle.getTimeInForce()),
                        "UserID", toValue(newOrderSingle.getUserId()),
                        "Instrument", toValue(newOrderSingle.getInstrument())
                )).build();
    }

    private Message createQuote(PlaceMessageRequestTyped requestTyped) {
        Quote quote = requestTyped.getMessageTyped().getQuote();

        Message.Builder noQuoteQualifiers = Message.newBuilder();
        for (Quote.QuoteQualifier quoteQualifier : quote.getNoQuoteQualifiersList()) {
            noQuoteQualifiers.putFields("QuoteQualifier", toValue(quoteQualifier.getQuoteQualifier()));
        }

        return messageBuilder(requestTyped).putAllFields(
                Map.of("NoQuoteQualifiers", toValue(noQuoteQualifiers),
                        "OfferPx", toValue(quote.getOfferPx()),
                        "OfferSize", toValue(quote.getOfferSize()),
                        "QuoteID", toValue(quote.getQuoteId()),
                        "Symbol", toValue(quote.getSymbol()),
                        "SecurityIDSource", toValue(quote.getSecurityIdSource()),
                        "BidSize", toValue(quote.getBidSize()),
                        "BidPx", toValue(quote.getBidPx()),
                        "SecurityID", toValue(quote.getSecurityId()),
                        "QuoteType", toValue(quote.getQuoteType())
                )).build();
    }

    private Message createSecurityListRequest(PlaceMessageRequestTyped requestTyped) {
        SecurityListRequest securityListRequest = requestTyped.getMessageTyped().getSecurityListRequest();

        return messageBuilder(requestTyped).putAllFields(
                Map.of("SecurityListRequestType", toValue(securityListRequest.getSecurityListRequestType()),
                        "SecurityReqID", toValue(securityListRequest.getSecurityReqId())
                )).build();
    }
}