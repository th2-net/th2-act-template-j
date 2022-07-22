package com.exactpro.th2.act.convertors;

import com.exactpro.th2.act.grpc.*;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.Value;
import com.google.protobuf.TextFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ConvertorsResponse {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConvertorsResponse.class);

    public ResponseMessageTyped createResponseMessage(Message message) {
        ResponseMessageTyped responseMessageTyped;
        switch (message.getMetadata().getMessageType()) {
            case "BusinessMessageReject":
                responseMessageTyped = createBusinessMessageReject(message);
                break;
            case "ExecutionReport":
                responseMessageTyped = createExecutionReport(message);
                break;
            case "QuoteStatusReport":
                responseMessageTyped = createQuoteStatusReport(message);
                break;
            case "Quote":
                responseMessageTyped = createQuote(message);
                break;
            case "QuoteAck":
                responseMessageTyped = createQuoteAck(message);
                break;
            case "OrderMassCancelReport":
                responseMessageTyped = createOrderMassCancelReport(message);
                break;
            case "MassQuoteAcknowledgement":
                responseMessageTyped = createMassQuoteAcknowledgement(message);
                break;
            default:
                responseMessageTyped = null;
                break;
        }
        return responseMessageTyped;
    }

    private ResponseMessageTyped createBusinessMessageReject(Message message) {
        BusinessMessageReject businessMessageReject = BusinessMessageReject.newBuilder()
                .setRefMsgType(getField(message, "RefMsgType"))
                .setBusinessRejectReason(Integer.parseInt(getField(message, "BusinessRejectReason")))
                .setBusinessRejectRefId(getField(message, "BusinessRejectRefID"))
                .setRefSeqNum(Integer.parseInt(getField(message, "RefSeqNum")))
                .setText(getField(message, "Text")).build();

        return ResponseMessageTyped.newBuilder().setBusinessMessageReject(businessMessageReject).build();
    }

    private ResponseMessageTyped createExecutionReport(Message message) {
        Message headerField = message.getFieldsMap().get("Header").getMessageValue();

        Header header = Header.newBuilder()
                .setBeginString(getField(headerField, "BeginString"))
                .setSenderCompId(getField(headerField, "SenderCompId"))
                .setSendingTime(getField(headerField, "SendingTime"))
                .setMsgSeqNum(Integer.parseInt(getField(headerField, "MsgSeqNum")))
                .setBodyLength(Integer.parseInt(getField(headerField, "BodyLength")))
                .setMsgType(getField(headerField, "MsgType"))
                .setTargetCompId(getField(headerField, "TargetCompId"))
                .build();

        ExecutionReport executionReport = ExecutionReport.newBuilder()
                .setSecurityId(getField(message, "SecurityID"))
                .setSecurityIdSource(getField(message, "SecurityIDSource"))
                .setOrdType(getField(message, "OrdType"))
                .setAccountType(Integer.parseInt(getField(message, "AccountType")))
                .setOrderCapacity(getField(message, "OrderCapacity"))
                .setClOrdId(getField(message, "ClOrdID"))
                .setOrderQty(Float.parseFloat(getField(message, "OrderQty")))
                .setLeavesQty(Float.parseFloat(getField(message, "LeavesQty")))
                .setSide(getField(message, "Side"))
                .setCumQty(Float.parseFloat(getField(message, "CumQty")))
                .setExecType(getField(message, "ExecType"))
                .setOrdStatus(getField(message, "OrdStatus"))
                .setExecId(getField(message, "ExecID"))
                .setPrice(Float.parseFloat(getField(message, "Price")))
                .setOrderId(getField(message, "OrderID"))
                .setText(getField(message, "Text"))
                .setTimeInForce(getField(message, "TimeInForce"))
                .setTransactTime(getField(message, "TransactTime"))
                .setHeader(header)
                .setLastPx(Float.parseFloat(getField(message, "LastPx")))
                .build();

        return ResponseMessageTyped.newBuilder().setExecutionReport(executionReport).build();
    }

    private ResponseMessageTyped createQuoteStatusReport(Message message) {
        QuoteStatusReport quoteStatusReport = QuoteStatusReport.newBuilder()
                .setQuoteId(getField(message, "QuoteID"))
                .setQuoteStatus(Integer.parseInt(getField(message, "QuoteStatus")))
                .setSecurityId(getField(message, "SecurityID"))
                .setSecurityIdSource(getField(message, "SecurityIDSource"))
                .setSymbol(getField(message, "Symbol"))
                .setText(getField(message, "Text")).build();

        return ResponseMessageTyped.newBuilder().setQuoteStatusReport(quoteStatusReport).build();
    }

    private ResponseMessageTyped createQuote(Message message) {
        Quote quote = Quote.newBuilder()
                .addAllNoQuoteQualifiers(createQuoteQualifier(message))
                .setOfferPx(Float.parseFloat(getField(message, "OfferPx")))
                .setOfferSize(Float.parseFloat(getField(message, "OfferSize")))
                .setQuoteId(getField(message, "QuoteID"))
                .setSymbol(getField(message, "Symbol"))
                .setSecurityIdSource(getField(message, "SecurityIDSource"))
                .setBidSize(getField(message, "BidSize"))
                .setBidPx(Float.parseFloat(getField(message, "BidPx")))
                .setSecurityId(getField(message, "SecurityID"))
                .setQuoteType(Integer.parseInt(getField(message, "QuoteType"))).build();

        return ResponseMessageTyped.newBuilder().setQuote(quote).build();
    }

    private ResponseMessageTyped createQuoteAck(Message message) {
        QuoteAck quoteAck = QuoteAck.newBuilder()
                .setRfqid(getField(message, "RFQID"))
                .setText(getField(message, "Text")).build();

        return ResponseMessageTyped.newBuilder().setQuoteAck(quoteAck).build();
    }

    private ResponseMessageTyped createOrderMassCancelReport(Message message) {
        OrderMassCancelReport orderMassCancelReport = OrderMassCancelReport.newBuilder()
                .setClOrdId(getField(message, "ClOrdID"))
                .setText(getField(message, "Text")).build();

        return ResponseMessageTyped.newBuilder().setOrderMassCancelReport(orderMassCancelReport).build();
    }

    private ResponseMessageTyped createMassQuoteAcknowledgement(Message message) {
        MassQuoteAcknowledgement massQuoteAcknowledgement = MassQuoteAcknowledgement.newBuilder()
                .setQuoteId(getField(message, "QuoteID"))
                .setText(getField(message, "Text")).build();

        return ResponseMessageTyped.newBuilder().setMassQuoteAcknowledgement(massQuoteAcknowledgement).build();
    }

    private String getField(Message message, String key) {
        try {
            return message.getFieldsMap().get(key).getSimpleValue();
        } catch (NullPointerException npe) {
            LOGGER.error("There is no {} field in the message = {}", key, TextFormat.shortDebugString(message));
            throw npe;
        }
    }

    private List<Quote.QuoteQualifier> createQuoteQualifier(Message message) {
        List<Quote.QuoteQualifier> quoteQualifiers = new ArrayList<>();
        for (Value value : message.getFieldsMap().get("NoQuoteQualifiers").getListValue().getValuesList()) {
            quoteQualifiers.add(Quote.QuoteQualifier.newBuilder()
                    .setQuoteQualifier(getField(value.getMessageValue(), "QuoteQualifier")).build());
        }
        return quoteQualifiers;
    }
}
