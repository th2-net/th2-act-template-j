package com.exactpro.th2.act.convertors

import com.exactpro.th2.act.grpc.Header
import com.exactpro.th2.act.grpc.Quote
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.message.getField
import com.exactpro.th2.common.value.toValue
import org.junit.jupiter.api.Test
import strikt.api.expect
import strikt.assertions.isEqualTo

class TestConvertorsResponse {
    private val convertorsResponse = ConvertorsResponse()

    @Test
    fun `convert BusinessMessageReject`() {
        val message = Message.newBuilder()
            .setMetadata(MessageMetadata.newBuilder().setMessageType("BusinessMessageReject"))
            .putAllFields(
                mapOf(
                    "RefMsgType" to "RefMsgType".toValue(),
                    "BusinessRejectReason" to 1.toValue(),
                    "BusinessRejectRefID" to "BusinessRejectRefID".toValue(),
                    "RefSeqNum" to 1.toValue(),
                    "Text" to "Text".toValue()
                )
            ).build()

        val responseMessageTyped = convertorsResponse.createResponseMessage(message).businessMessageReject

        expect {
            that(message.getField("RefMsgType")).isEqualTo(responseMessageTyped.refMsgType.toValue())
            that(message.getField("BusinessRejectReason")).isEqualTo(responseMessageTyped.businessRejectReason.toValue())
            that(message.getField("BusinessRejectRefID")).isEqualTo(responseMessageTyped.businessRejectRefId.toValue())
            that(message.getField("RefSeqNum")).isEqualTo(responseMessageTyped.refSeqNum.toValue())
            that(message.getField("Text")).isEqualTo(responseMessageTyped.text.toValue())
        }
    }

    @Test
    fun `convert ExecutionReport`() {
        val header = Message.newBuilder().putAllFields(
            mapOf(
                "BeginString" to "BeginString".toValue(),
                "SenderCompId" to "SenderCompId".toValue(),
                "SendingTime" to "SendingTime".toValue(),
                "MsgSeqNum" to 1.toValue(),
                "BodyLength" to 1.toValue(),
                "MsgType" to "ExecutionReport".toValue(),
                "TargetCompId" to "TargetCompId".toValue()
            )
        ).build()

        val message = Message.newBuilder()
            .setMetadata(MessageMetadata.newBuilder().setMessageType("ExecutionReport"))
            .putAllFields(
                mapOf(
                    "SecurityID" to "SecurityID".toValue(),
                    "SecurityIDSource" to "SecurityIDSource".toValue(),
                    "OrdType" to "OrdType".toValue(),
                    "AccountType" to 1.toValue(),
                    "OrderCapacity" to "OrderCapacity".toValue(),
                    "ClOrdID" to "ClOrdID".toValue(),
                    "OrderQty" to 1.0F.toValue(),
                    "LeavesQty" to 1.0F.toValue(),
                    "Side" to "Side".toValue(),
                    "CumQty" to 1.0F.toValue(),
                    "ExecType" to "ExecType".toValue(),
                    "OrdStatus" to "OrdStatus".toValue(),
                    "ExecID" to "ExecID".toValue(),
                    "Price" to 1.0F.toValue(),
                    "OrderID" to "OrderID".toValue(),
                    "Text" to "Text".toValue(),
                    "TimeInForce" to "TimeInForce".toValue(),
                    "TransactTime" to "TransactTime".toValue(),
                    "Header" to header.toValue(),
                    "LastPx" to 1.0F.toValue()
                )
            ).build()

        val responseMessageTyped = convertorsResponse.createResponseMessage(message).executionReport

        expect {
            that(message.getField("SecurityID")).isEqualTo(responseMessageTyped.securityId.toValue())
            that(message.getField("SecurityIDSource")).isEqualTo(responseMessageTyped.securityIdSource.toValue())
            that(message.getField("OrdType")).isEqualTo(responseMessageTyped.ordType.toValue())
            that(message.getField("AccountType")).isEqualTo(responseMessageTyped.accountType.toValue())
            that(message.getField("OrderCapacity")).isEqualTo(responseMessageTyped.orderCapacity.toValue())
            that(message.getField("ClOrdID")).isEqualTo(responseMessageTyped.clOrdId.toValue())
            that(message.getField("OrderQty")).isEqualTo(responseMessageTyped.orderQty.toValue())
            that(message.getField("LeavesQty")).isEqualTo(responseMessageTyped.leavesQty.toValue())
            that(message.getField("Side")).isEqualTo(responseMessageTyped.side.toValue())
            that(message.getField("CumQty")).isEqualTo(responseMessageTyped.cumQty.toValue())
            that(message.getField("ExecType")).isEqualTo(responseMessageTyped.execType.toValue())
            that(message.getField("OrdStatus")).isEqualTo(responseMessageTyped.ordStatus.toValue())
            that(message.getField("ExecID")).isEqualTo(responseMessageTyped.execId.toValue())
            that(message.getField("Price")).isEqualTo(responseMessageTyped.price.toValue())
            that(message.getField("OrderID")).isEqualTo(responseMessageTyped.orderId.toValue())
            that(message.getField("Text")).isEqualTo(responseMessageTyped.text.toValue())
            that(message.getField("TimeInForce")).isEqualTo(responseMessageTyped.timeInForce.toValue())
            that(message.getField("TransactTime")).isEqualTo(responseMessageTyped.transactTime.toValue())
            that(
                Header.newBuilder()
                    .setBeginString("BeginString")
                    .setSenderCompId("SenderCompId")
                    .setSendingTime("SendingTime")
                    .setMsgSeqNum(1)
                    .setBodyLength(1)
                    .setMsgType("ExecutionReport")
                    .setTargetCompId("TargetCompId")
                    .build()
            ).isEqualTo(responseMessageTyped.header)
            that(message.getField("LastPx")).isEqualTo(responseMessageTyped.lastPx.toValue())
        }
    }

    @Test
    fun `convert QuoteStatusReport`() {
        val message = Message.newBuilder()
            .setMetadata(MessageMetadata.newBuilder().setMessageType("QuoteStatusReport"))
            .putAllFields(
                mapOf(
                    "QuoteID" to "QuoteID".toValue(),
                    "QuoteStatus" to 1.toValue(),
                    "SecurityID" to "SecurityID".toValue(),
                    "SecurityIDSource" to "SecurityIDSource".toValue(),
                    "Symbol" to "Symbol".toValue(),
                    "Text" to "Text".toValue()
                )
            ).build()

        val responseMessageTyped = convertorsResponse.createResponseMessage(message).quoteStatusReport

        expect {
            that(message.getField("QuoteID")).isEqualTo(responseMessageTyped.quoteId.toValue())
            that(message.getField("QuoteStatus")).isEqualTo(responseMessageTyped.quoteStatus.toValue())
            that(message.getField("SecurityID")).isEqualTo(responseMessageTyped.securityId.toValue())
            that(message.getField("SecurityIDSource")).isEqualTo(responseMessageTyped.securityIdSource.toValue())
            that(message.getField("Symbol")).isEqualTo(responseMessageTyped.symbol.toValue())
            that(message.getField("Text")).isEqualTo(responseMessageTyped.text.toValue())
        }
    }

    @Test
    fun `convert Quote`() {
        val quoteQualifier = mutableListOf<Message>()
        for (partyRole in 0..2) {
            quoteQualifier.add(
                Message.newBuilder()
                    .putFields("QuoteQualifier", "QuoteQualifier".toValue()).build()
            )
        }

        val message = Message.newBuilder()
            .setMetadata(MessageMetadata.newBuilder().setMessageType("Quote"))
            .putAllFields(
                mapOf(
                    "NoQuoteQualifiers" to quoteQualifier.toValue(),
                    "OfferPx" to 1.0F.toValue(),
                    "OfferSize" to 1.0F.toValue(),
                    "QuoteID" to "QuoteID".toValue(),
                    "Symbol" to "Symbol".toValue(),
                    "SecurityIDSource" to "SecurityIDSource".toValue(),
                    "BidSize" to "BidSize".toValue(),
                    "BidPx" to 1.0F.toValue(),
                    "SecurityID" to "SecurityID".toValue(),
                    "QuoteType" to 1.toValue()
                )
            ).build()

        val responseMessageTyped = convertorsResponse.createResponseMessage(message).quote

        expect {
            that(
                Quote.QuoteQualifier.newBuilder()
                    .setQuoteQualifier("QuoteQualifier").build()
            ).isEqualTo(responseMessageTyped.noQuoteQualifiersList[0])
            that(message.getField("OfferPx")).isEqualTo(responseMessageTyped.offerPx.toValue())
            that(message.getField("OfferSize")).isEqualTo(responseMessageTyped.offerSize.toValue())
            that(message.getField("QuoteID")).isEqualTo(responseMessageTyped.quoteId.toValue())
            that(message.getField("Symbol")).isEqualTo(responseMessageTyped.symbol.toValue())
            that(message.getField("SecurityIDSource")).isEqualTo(responseMessageTyped.securityIdSource.toValue())
            that(message.getField("BidSize")).isEqualTo(responseMessageTyped.bidSize.toValue())
            that(message.getField("BidPx")).isEqualTo(responseMessageTyped.bidPx.toValue())
            that(message.getField("SecurityID")).isEqualTo(responseMessageTyped.securityId.toValue())
            that(message.getField("QuoteType")).isEqualTo(responseMessageTyped.quoteType.toValue())
        }
    }

    @Test
    fun `convert QuoteAck`() {
        val message = Message.newBuilder()
            .setMetadata(MessageMetadata.newBuilder().setMessageType("QuoteAck"))
            .putAllFields(
                mapOf(
                    "RFQID" to "RFQID".toValue(),
                    "Text" to "Text".toValue()
                )
            ).build()

        val responseMessageTyped = convertorsResponse.createResponseMessage(message).quoteAck

        expect {
            that(message.getField("RFQID")).isEqualTo(responseMessageTyped.rfqid.toValue())
            that(message.getField("Text")).isEqualTo(responseMessageTyped.text.toValue())
        }
    }

    @Test
    fun `convert OrderMassCancelReport`() {
        val message = Message.newBuilder()
            .setMetadata(MessageMetadata.newBuilder().setMessageType("OrderMassCancelReport"))
            .putAllFields(
                mapOf(
                    "ClOrdID" to "ClOrdID".toValue(),
                    "Text" to "Text".toValue()
                )
            ).build()

        val responseMessageTyped = convertorsResponse.createResponseMessage(message).orderMassCancelReport

        expect {
            that(message.getField("ClOrdID")).isEqualTo(responseMessageTyped.clOrdId.toValue())
            that(message.getField("Text")).isEqualTo(responseMessageTyped.text.toValue())
        }
    }

    @Test
    fun `convert MassQuoteAcknowledgement`() {
        val message = Message.newBuilder()
            .setMetadata(MessageMetadata.newBuilder().setMessageType("MassQuoteAcknowledgement"))
            .putAllFields(
                mapOf(
                    "QuoteID" to "QuoteID".toValue(),
                    "Text" to "Text".toValue()
                )
            ).build()

        val responseMessageTyped = convertorsResponse.createResponseMessage(message).massQuoteAcknowledgement

        expect {
            that(message.getField("QuoteID")).isEqualTo(responseMessageTyped.quoteId.toValue())
            that(message.getField("Text")).isEqualTo(responseMessageTyped.text.toValue())
        }
    }
}