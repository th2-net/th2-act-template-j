/*
 * Copyright 2022-2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.act.convertors

import com.exactpro.th2.act.grpc.*
import com.exactpro.th2.common.grpc.*
import com.exactpro.th2.common.message.get
import com.exactpro.th2.common.message.messageType
import com.exactpro.th2.common.value.toValue
import org.junit.jupiter.api.Test
import strikt.api.expect
import strikt.assertions.isEqualTo
import strikt.assertions.isNotEqualTo

class TestConvertorsRequest {
    val convertorsRequest: ConvertorsRequest = ConvertorsRequest()
    val connectionID = ConnectionID.newBuilder().setSessionAlias("sessionAlias").build()
    val parentEventID = EventID.newBuilder().setId("parentEventId").build()

    @Test
    fun `convert NewOrderSingle`() {
        val requestTyped: PlaceMessageRequestTyped =
            createPlaceMessageRequestTyped("NewOrderSingle")
                .setMessageTyped(
                    RequestMessageTyped.newBuilder().setNewOrderSingle(
                        NewOrderSingle.newBuilder().apply {
                            securityId = "SecurityID"
                            secondaryClOrdId = "SecondaryClOrdID"
                            ordType = "OrdType"
                            accountType = 1
                            orderCapacity = "OrderCapacity"
                            orderQty = 1.0F
                            displayQty = 1.0F
                            price = 1.0F
                            clOrdId = "ClOrdID"
                            securityIdSource = "SecurityIDSource"
                            side = "Side"
                            timeInForce = "TimeInForce"
                            transactTime = "TransactTime"
                            tradingParty = tradingParty(5)
                            symbol = "Symbol"
                        }
                    )
                ).build()

        val requestMsg = convertorsRequest.createMessage(requestTyped)
        val expendedMsg = requestTyped.messageTyped.newOrderSingle

        expect {
            that(parentEventID).isEqualTo(requestMsg.parentEventId)
            that("NewOrderSingle").isEqualTo(requestMsg.messageType)
            that(connectionID).isEqualTo(requestMsg.metadata.id.connectionId)

            that(expendedMsg.accountType.toString()).isEqualTo(requestMsg["AccountType"]!!.simpleValue)
            that(expendedMsg.orderCapacity).isEqualTo(requestMsg["OrderCapacity"]!!.simpleValue)
            that(expendedMsg.orderQty.toString()).isEqualTo(requestMsg["OrderQty"]!!.simpleValue)
            that(expendedMsg.displayQty.toString()).isEqualTo(requestMsg["DisplayQty"]!!.simpleValue)
            that(expendedMsg.price.toString()).isEqualTo(requestMsg["Price"]!!.simpleValue)
            that(expendedMsg.clOrdId).isEqualTo(requestMsg["ClOrdID"]!!.simpleValue)
            that(expendedMsg.secondaryClOrdId).isEqualTo(requestMsg["SecondaryClOrdID"]!!.simpleValue)
            that(expendedMsg.side).isEqualTo(requestMsg["Side"]!!.simpleValue)
            that(expendedMsg.timeInForce).isEqualTo(requestMsg["TimeInForce"]!!.simpleValue)
            that(expendedMsg.transactTime).isEqualTo(requestMsg["TransactTime"]!!.simpleValue)
            that(Message.newBuilder().putFields("NoPartyIDs", noPartyIDsList(5).toValue()).build().toValue())
                .isEqualTo(requestMsg["TradingParty"])
        }
    }

    @Test
    fun `convert Quote`() {
        val requestTyped: PlaceMessageRequestTyped =
            createPlaceMessageRequestTyped("Quote")
                .setMessageTyped(
                    RequestMessageTyped.newBuilder().setQuote(
                        Quote.newBuilder().apply {
                            addNoQuoteQualifiers(
                                Quote.QuoteQualifier.newBuilder().setQuoteQualifier("NoQuoteQualifiers")
                            )
                            offerPx = 1F
                            offerSize = 1F
                            quoteId = "QuoteID"
                            symbol = "Symbol"
                            securityIdSource = "SecurityIDSource"
                            bidSize = "BidSize"
                            bidPx = 1F
                            securityId = "SecurityID"
                            quoteType = 1
                        }
                    )
                ).build()

        val requestMsg = convertorsRequest.createMessage(requestTyped)
        val expendedMsg = requestTyped.messageTyped.quote
        expect {
            that(parentEventID).isEqualTo(requestMsg.parentEventId)
            that("Quote").isEqualTo(requestMsg.messageType)
            that(connectionID).isEqualTo(requestMsg.metadata.id.connectionId)

            that(expendedMsg.noQuoteQualifiersList[0].quoteQualifier)
                .isEqualTo(requestMsg["NoQuoteQualifiers"]?.listValue?.valuesList?.get(0)?.messageValue?.get("QuoteQualifier")?.simpleValue)
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
                        SecurityListRequest.newBuilder().apply {
                            securityListRequestType = 1
                            securityReqId = "SecurityReqID"
                        }
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

    fun createPlaceMessageRequestTyped(messageType: String): PlaceMessageRequestTyped.Builder {
        return PlaceMessageRequestTyped.newBuilder()
            .setParentEventId(parentEventID)
            .setMetadata(
                MessageMetadata.newBuilder()
                    .setMessageType(messageType)
                    .setId(MessageID.newBuilder().setConnectionId(connectionID).build())
            )
    }
}