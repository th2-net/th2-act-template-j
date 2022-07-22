package com.exactpro.th2.act.convertors

import com.exactpro.th2.act.grpc.*
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.message.get
import com.exactpro.th2.common.message.messageType
import com.exactpro.th2.common.value.toValue
import org.junit.jupiter.api.Test
import strikt.api.expect
import strikt.assertions.isEqualTo
import strikt.assertions.isNotEqualTo

class TestConvertorsRequest {
    private val convertorsRequest: ConvertorsRequest = ConvertorsRequest()
    private val connectionID = ConnectionID.newBuilder().setSessionAlias("sessionAlias").build()
    private val parentEventID = EventID.newBuilder().setId("parentEventId").build()

    @Test
    fun `convert NewOrderSingle`() {
        val requestTyped: PlaceMessageRequestTyped =
            createPlaceMessageRequestTyped("NewOrderSingle")
                .setMessageTyped(
                    RequestMessageTyped.newBuilder().setNewOrderSingle(
                        NewOrderSingle.newBuilder()
                            .setPrice(1F)
                            .setOrderQty(1F)
                            .setSide("Side")
                            .setTimeInForce("TimeInForce")
                    )
                ).build()

        val requestMsg = convertorsRequest.createMessage(requestTyped)
        val expendedMsg = requestTyped.messageTyped.newOrderSingle

        expect {
            that(parentEventID).isEqualTo(requestMsg.parentEventId)
            that("NewOrderSingle").isEqualTo(requestMsg.messageType)
            that(connectionID).isEqualTo(requestMsg.metadata.id.connectionId)

            that(expendedMsg.price.toString()).isEqualTo(requestMsg["Price"]!!.simpleValue).isNotEqualTo("0.0")
            that(expendedMsg.orderQty.toString()).isEqualTo(requestMsg["OrderQty"]!!.simpleValue).isNotEqualTo("0.0")
            that(expendedMsg.side).isEqualTo(requestMsg["Side"]!!.simpleValue).isNotEqualTo("")
            that(expendedMsg.timeInForce).isEqualTo(requestMsg["TimeInForce"]!!.simpleValue).isNotEqualTo("")
        }
    }

    @Test
    fun `convert Quote`() {
        val requestTyped: PlaceMessageRequestTyped =
            createPlaceMessageRequestTyped("Quote")
                .setMessageTyped(
                    RequestMessageTyped.newBuilder().setQuote(
                        Quote.newBuilder()
                            .addNoQuoteQualifiers(
                                Quote.QuoteQualifier.newBuilder().setQuoteQualifier("NoQuoteQualifiers")
                            )
                            .setOfferPx(1F)
                            .setOfferSize(1F)
                            .setQuoteId("QuoteID")
                            .setSymbol("Symbol")
                            .setSecurityIdSource("SecurityIDSource")
                            .setBidSize("BidSize")
                            .setBidPx(1F)
                            .setSecurityId("SecurityID")
                            .setQuoteType(1)
                    )
                ).build()

        val requestMsg = convertorsRequest.createMessage(requestTyped)
        val expendedMsg = requestTyped.messageTyped.quote

        expect {
            that(parentEventID).isEqualTo(requestMsg.parentEventId)
            that("Quote").isEqualTo(requestMsg.messageType)
            that(connectionID).isEqualTo(requestMsg.metadata.id.connectionId)

            that(expendedMsg.noQuoteQualifiersList[0].quoteQualifier)
                .isEqualTo(requestMsg["NoQuoteQualifiers"]!!.messageValue["QuoteQualifier"]!!.simpleValue)
            that(expendedMsg.offerPx.toValue()).isEqualTo(requestMsg["OfferPx"]).isNotEqualTo("0.0".toValue())
            that(expendedMsg.offerSize.toValue()).isEqualTo(requestMsg["OfferSize"]).isNotEqualTo("0.0".toValue())
            that(expendedMsg.quoteId).isEqualTo(requestMsg["QuoteID"]!!.simpleValue).isNotEqualTo("")
            that(expendedMsg.symbol).isEqualTo(requestMsg["Symbol"]!!.simpleValue).isNotEqualTo("")
            that(expendedMsg.securityIdSource).isEqualTo(requestMsg["SecurityIDSource"]!!.simpleValue).isNotEqualTo("")
            that(expendedMsg.bidSize).isEqualTo(requestMsg["BidSize"]!!.simpleValue).isNotEqualTo("")
            that(expendedMsg.bidPx.toValue()).isEqualTo(requestMsg["BidPx"]).isNotEqualTo("0.0".toValue())
            that(expendedMsg.securityId).isEqualTo(requestMsg["SecurityID"]!!.simpleValue).isNotEqualTo("")
            that(expendedMsg.quoteType.toValue()).isEqualTo(requestMsg["QuoteType"]).isNotEqualTo("0.0".toValue())
        }
    }

    @Test
    fun `convert SecurityListRequest`() {
        val requestTyped: PlaceMessageRequestTyped =
            createPlaceMessageRequestTyped("SecurityListRequest")
                .setMessageTyped(
                    RequestMessageTyped.newBuilder().setSecurityListRequest(
                        SecurityListRequest.newBuilder()
                            .setSecurityListRequestType(1)
                            .setSecurityReqId("SecurityReqID")
                    )
                ).build()

        val requestMsg = convertorsRequest.createMessage(requestTyped)
        val expendedMsg = requestTyped.messageTyped.securityListRequest

        expect {
            that(parentEventID).isEqualTo(requestMsg.parentEventId)
            that("SecurityListRequest").isEqualTo(requestMsg.messageType)
            that(connectionID).isEqualTo(requestMsg.metadata.id.connectionId)

            that(expendedMsg.securityListRequestType.toString())
                .isEqualTo(requestMsg["SecurityListRequestType"]!!.simpleValue).isNotEqualTo("")
            that(expendedMsg.securityReqId).isEqualTo(requestMsg["SecurityReqID"]!!.simpleValue).isNotEqualTo("")
        }
    }

    private fun createPlaceMessageRequestTyped(messageType: String): PlaceMessageRequestTyped.Builder {
        return PlaceMessageRequestTyped.newBuilder()
            .setParentEventId(parentEventID)
            .setMetadata(
                MessageMetadata.newBuilder()
                    .setMessageType(messageType)
                    .setId(MessageID.newBuilder().setConnectionId(connectionID).build())
            )
    }
}