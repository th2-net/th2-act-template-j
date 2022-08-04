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
import com.exactpro.th2.act.grpc.NoPartyIDs
import com.exactpro.th2.act.grpc.Quote.QuoteQualifier
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.Value
import com.google.protobuf.TextFormat
import org.slf4j.LoggerFactory


class ConvertorsResponse {
    fun createResponseMessage(message: Message): ResponseMessageTyped {
        val responseMessageTyped: ResponseMessageTyped =
            when (val messageType = message.metadata.messageType) {
                "QuoteStatusReport" -> createQuoteStatusReport(message)
                "Quote" -> createQuote(message)
                "QuoteAck" -> createQuoteAck(message)
                "OrderMassCancelReport" -> createOrderMassCancelReport(message)
                "MassQuoteAcknowledgement" -> createMassQuoteAcknowledgement(message)
                else -> throw IllegalArgumentException("Unsupported request message type $messageType")
            }
        return responseMessageTyped
    }

    /*private fun createBusinessMessageReject(message: Message): ResponseMessageTyped {
        val businessMessageReject = BusinessMessageReject.newBuilder().apply {
            refMsgType = field(message, "RefMsgType")
            businessRejectReason = field(message, "BusinessRejectReason").toInt()
            businessRejectRefId = field(message, "BusinessRejectRefID")
            refSeqNum = field(message, "RefSeqNum").toInt()
            text = field(message, "Text")
        }

        return ResponseMessageTyped.newBuilder()
            .setBusinessMessageReject(businessMessageReject).build()
    }

    private fun createExecutionReport(message: Message): ResponseMessageTyped {
        val executionReport = ExecutionReport.newBuilder().apply {
            securityId = field(message, "SecurityID")
            securityIdSource = field(message, "SecurityIDSource")
            ordType = field(message, "OrdType")
            accountType = field(message, "AccountType").toInt()
            orderCapacity = field(message, "OrderCapacity")
            clOrdId = field(message, "ClOrdID")
            orderQty = field(message, "OrderQty").toFloat()
            leavesQty = field(message, "LeavesQty").toFloat()
            side = field(message, "Side")
            cumQty = field(message, "CumQty").toFloat()
            execType = field(message, "ExecType")
            ordStatus = field(message, "OrdStatus")
            execId = field(message, "ExecID")
            price = field(message, "Price").toFloat()
            orderId = field(message, "OrderID")
            text = field(message, "Text")
            timeInForce = field(message, "TimeInForce")
            transactTime = field(message, "TransactTime")
            lastPx = field(message, "LastPx").toFloat()
        }

        val headerFields = message.fieldsMap["Header"]
        if (headerFields != null) {
            val headerField = headerFields.messageValue

            val header = Header.newBuilder().apply {
                beginString = field(headerField, "BeginString")
                senderCompId = field(headerField, "SenderCompId")
                sendingTime = field(headerField, "SendingTime")
                msgSeqNum = field(headerField, "MsgSeqNum").toInt()
                bodyLength = field(headerField, "BodyLength").toInt()
                msgType = field(headerField, "MsgType")
                targetCompId = field(headerField, "TargetCompId")
            }

            executionReport.header = header.build()
        }

        return ResponseMessageTyped.newBuilder()
            .setExecutionReport(executionReport).build()
    }*/

    private fun createQuoteStatusReport(message: Message): ResponseMessageTyped {
        val quoteStatusReport = QuoteStatusReport.newBuilder().apply {
            quoteId = field(message, "QuoteID")
            quoteStatus = field(message, "QuoteStatus").toInt()
            securityId = field(message, "SecurityID")
            securityIdSource = field(message, "SecurityIDSource")
            symbol = field(message, "Symbol")
            text = field(message, "Text")
        }

        return ResponseMessageTyped.newBuilder()
            .setQuoteStatusReport(quoteStatusReport).build()
    }

    private fun createQuote(message: Message): ResponseMessageTyped {
        val quote = Quote.newBuilder().apply {
            addAllNoQuoteQualifiers(createQuoteQualifier(message))
            offerPx = field(message, "OfferPx").toFloat()
            offerSize = field(message, "OfferSize").toFloat()
            quoteId = field(message, "QuoteID")
            symbol = field(message, "Symbol")
            securityIdSource = field(message, "SecurityIDSource")
            bidSize = field(message, "BidSize")
            bidPx = field(message, "BidPx").toFloat()
            securityId = field(message, "SecurityID")
            addAllNoPartyIds(createNoPartyIDs(message))
            quoteType = field(message, "QuoteType").toInt()
        }
        return ResponseMessageTyped.newBuilder()
            .setQuote(quote).build()
    }

    private fun createQuoteAck(message: Message): ResponseMessageTyped {
        val quoteAck = QuoteAck.newBuilder().apply {
            rfqid = field(message, "RFQID")
            text = field(message, "Text")
        }
        return ResponseMessageTyped.newBuilder()
            .setQuoteAck(quoteAck).build()
    }

    private fun createOrderMassCancelReport(message: Message): ResponseMessageTyped {
        val orderMassCancelReport = OrderMassCancelReport.newBuilder().apply {
            clOrdId = field(message, "ClOrdID")
            text = field(message, "Text")
        }
        return ResponseMessageTyped.newBuilder()
            .setOrderMassCancelReport(orderMassCancelReport).build()
    }

    private fun createMassQuoteAcknowledgement(message: Message): ResponseMessageTyped {
        val massQuoteAcknowledgement = MassQuoteAcknowledgement.newBuilder().apply {
            quoteId = field(message, "QuoteID")
            text = field(message, "Text")
        }

        return ResponseMessageTyped.newBuilder()
            .setMassQuoteAcknowledgement(massQuoteAcknowledgement).build()
    }

    private fun field(message: Message, key: String): String =
        try {
            message.fieldsMap[key]!!.simpleValue
        } catch (npe: NullPointerException) {
            LOGGER.error("There is no {} field in the message = {}", key, TextFormat.shortDebugString(message))
            throw npe
        }

    private fun createQuoteQualifier(message: Message): List<QuoteQualifier> {
        val quoteQualifiers: MutableList<QuoteQualifier> = ArrayList()
        val noQuoteQualifiers = message.fieldsMap["NoQuoteQualifiers"]
        if(noQuoteQualifiers != null) {
            for (value in noQuoteQualifiers.listValue.valuesList) {
                quoteQualifiers.add(
                    QuoteQualifier.newBuilder()
                        .setQuoteQualifier(field(value.messageValue, "QuoteQualifier")).build()
                )
            }
        }
        return quoteQualifiers
    }

    fun createNoPartyIDs(message: Message): List<NoPartyIDs>{
        val msgNoPartyIDs: List<Value> = message.fieldsMap["NoPartyIDs"]!!.listValue.valuesList
        val noPartyIDs: MutableList<NoPartyIDs> = ArrayList()
        for (value in msgNoPartyIDs) {
            val msg: Message = value.messageValue
            noPartyIDs.add(
                NoPartyIDs.newBuilder()
                    .setPartyId(field(msg, "PartyID"))
                    .setPartyIdSource(field(msg, "PartyIDSource"))
                    .setPartyRole(field(msg, "PartyRole").toInt()).build()
            )
        }
        return noPartyIDs
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(ConvertorsResponse::class.java)
    }
}