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

import com.exactpro.th2.act.grpc.NoPartyIDs
import com.exactpro.th2.act.grpc.PlaceMessageRequestTyped
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.value.toValue


class ConvertorsRequest {
    fun createMessage(requestTyped: PlaceMessageRequestTyped): Message {
        val requestMessage: Message = when (val messageType = requestTyped.metadata.messageType) {
            "NewOrderSingle" -> createNewOrderSingle(requestTyped)
            "Quote" -> createQuote(requestTyped)
            "SecurityListRequest" -> createSecurityListRequest(requestTyped)
            else -> throw IllegalArgumentException("Unsupported request message type $messageType")
        }
        return requestMessage
    }

    private fun messageBuilder(requestTyped: PlaceMessageRequestTyped): Message.Builder {
        val metadata = requestTyped.metadata

        return Message.newBuilder()
            .setParentEventId(requestTyped.parentEventId)
            .setMetadata(
                MessageMetadata.newBuilder()
                    .setMessageType(metadata.messageType)
                    .setId(
                        MessageID.newBuilder()
                            .setConnectionId(
                                ConnectionID.newBuilder()
                                    .setSessionAlias(metadata.id.connectionId.sessionAlias)
                            )
                    )
            )
    }

    private fun createNewOrderSingle(requestTyped: PlaceMessageRequestTyped): Message {
        val newOrderSingle = requestTyped.messageTyped.newOrderSingle
        return messageBuilder(requestTyped).putAllFields(

            mutableMapOf(
                "SecurityID" to newOrderSingle.securityId.toValue(),
                "SecurityIDSource" to newOrderSingle.securityIdSource.toValue(),
                "OrdType" to newOrderSingle.ordType.toValue(),
                "AccountType" to newOrderSingle.accountType.toValue(),
                "OrderCapacity" to newOrderSingle.orderCapacity.toValue(),
                "OrderQty" to newOrderSingle.orderQty.toValue(),
                "DisplayQty" to newOrderSingle.displayQty.toValue(),
                "Price" to newOrderSingle.price.toValue(),
                "ClOrdID" to newOrderSingle.clOrdId.toValue(),
                "SecondaryClOrdID" to newOrderSingle.secondaryClOrdId.toValue(),
                "Side" to newOrderSingle.side.toValue(),
                "TimeInForce" to newOrderSingle.timeInForce.toValue(),
                "TransactTime" to newOrderSingle.transactTime.toValue(),
                "TradingParty" to Message.newBuilder().putFields("NoPartyIDs",
                            createNoPartyIdsFields(newOrderSingle.tradingParty.noPartyIdsList).toValue()).toValue(),
                "Symbol" to newOrderSingle.symbol.toValue()
            )
        ).build()
    }

    private fun createQuote(requestTyped: PlaceMessageRequestTyped): Message {
        val quote = requestTyped.messageTyped.quote
        val noQuoteQualifiers = Message.newBuilder()
        for (quoteQualifier in quote.noQuoteQualifiersList) {
            noQuoteQualifiers.putFields("QuoteQualifier", quoteQualifier.quoteQualifier.toValue())
        }
        return messageBuilder(requestTyped).putAllFields(
            mutableMapOf(
                "NoQuoteQualifiers" to noQuoteQualifiers.toValue(),
                "OfferPx" to quote.offerPx.toValue(),
                "OfferSize" to quote.offerSize.toValue(),
                "QuoteID" to quote.quoteId.toValue(),
                "Symbol" to quote.symbol.toValue(),
                "SecurityIDSource" to quote.securityIdSource.toValue(),
                "BidSize" to quote.bidSize.toValue(),
                "BidPx" to quote.bidPx.toValue(),
                "SecurityID" to quote.securityId.toValue(),
                "NoPartyIDs" to createNoPartyIdsFields(quote.noPartyIdsList).toValue(),
                "QuoteType" to quote.quoteType.toValue()
            )
        ).build()
    }

    private fun createSecurityListRequest(requestTyped: PlaceMessageRequestTyped): Message {
        val securityListRequest = requestTyped.messageTyped.securityListRequest
        return messageBuilder(requestTyped).putAllFields(
            mutableMapOf(
                "SecurityListRequestType" to securityListRequest.securityListRequestType.toValue(),
                "SecurityReqID" to securityListRequest.securityReqId.toValue()
            )
        ).build()
    }

    private fun createNoPartyIdsFields(listNoPartyIds: List<NoPartyIDs>): List<Message.Builder> {
        val messages: MutableList<Message.Builder> = ArrayList()
        for (noPartyIds in listNoPartyIds) {
            messages.add(
                Message.newBuilder()
                    .putFields("PartyID", noPartyIds.partyId.toValue())
                    .putFields("PartyIDSource", noPartyIds.partyIdSource.toValue())
                    .putFields("PartyRole", noPartyIds.partyRole.toValue())
            )
        }
        return messages
    }
}