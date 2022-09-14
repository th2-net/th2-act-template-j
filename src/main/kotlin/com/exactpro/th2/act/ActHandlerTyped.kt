/*
 * Copyright 2022-2022 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you  may not use this file except in compliance with the License.
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

package com.exactpro.th2.act

import com.exactpro.th2.act.core.action.ActionFactory
import com.exactpro.th2.act.core.messages.message
import com.exactpro.th2.act.grpc.*
import com.exactpro.th2.act.grpc.ActTypedGrpc.ActTypedImplBase
import com.exactpro.th2.common.grpc.*
import com.exactpro.th2.common.message.*
import com.exactpro.th2.common.value.toValue
import com.google.protobuf.TextFormat.shortDebugString
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory
import kotlin.streams.toList

class ActHandlerTyped(
    private val actionFactory: ActionFactory
) : ActTypedImplBase() {

    override fun placeOrderFIX(
        request: PlaceMessageRequestTyped,
        responseObserver: StreamObserver<PlaceMessageResponseTyped>
    ) {
        LOGGER.debug("placeOrderFIX request: ${shortDebugString(request)}")

        actionFactory.createAction(responseObserver, "placeOrderFIX", "Place order FIX", request.parentEventId, 10_000)
            .preFilter { msg ->
                msg.messageType != "Heartbeat"
                        && msg.sessionAlias == request.metadata.id.connectionId.sessionAlias
                        && msg.direction == Direction.FIRST
            }
            .execute {
                val requestMessage = message(
                    request.metadata.messageType,
                    ConnectionID.newBuilder().setSessionAlias(request.metadata.id.connectionId.sessionAlias).build()
                ) {
                    parentEventId = request.parentEventId
                    body {
                        val newOrderSingle = request.messageTyped.newOrderSingle
                        "Instrument" to message {
                            "Symbol" to newOrderSingle.symbol.toValue()
                            "SecurityID" to newOrderSingle.securityId.toValue()
                            "SecurityIDSource" to newOrderSingle.securityIdSource.toValue()
                        }
                        "OrdType" to newOrderSingle.ordType.toValue()
                        "AccountType" to newOrderSingle.accountType.toValue()
                        "OrderCapacity" to newOrderSingle.orderCapacity.toValue()
                        "OrderQty" to newOrderSingle.orderQty.toValue()
                        "DisplayQty" to newOrderSingle.displayQty.toValue()
                        "Price" to newOrderSingle.price.toValue()
                        "ClOrdID" to newOrderSingle.clOrdId.toValue()
                        "SecondaryClOrdID" to newOrderSingle.secondaryClOrdId.toValue()
                        "Side" to newOrderSingle.side.toValue()
                        "TimeInForce" to newOrderSingle.timeInForce.toValue()
                        "TransactTime" to newOrderSingle.transactTime.toValue()
                        "TradingParty" to message {
                            "NoPartyIDs" to newOrderSingle.tradingParty.noPartyIdsList.stream().map {
                                message {
                                    "PartyID" to it.partyId.toValue()
                                    "PartyIDSource" to it.partyIdSource.toValue()
                                    "PartyRole" to it.partyRole.toValue()
                                }.build()
                            }.toList().toValue()
                        }
                    }
                }

                val checkpoint: Checkpoint = registerCheckPoint(requestMessage.parentEventId, request.description)

                val echoMessage = send(requestMessage, requestMessage.sessionAlias, 10_000, true)

                val receiveMessage = receive(10_000, requestMessage.sessionAlias, Direction.FIRST) {
                    passOn("ExecutionReport") {
                        fieldsMap["ClOrdID"] == requestMessage.fieldsMap["ClOrdID"]
                    }
                    failOn("BusinessMessageReject") {
                        fieldsMap["BusinessRejectRefID"] == requestMessage.fieldsMap["ClOrdID"]
                    }
                    failOn("Reject") { this.sequence == echoMessage.sequence }
                }

                val executionReport = ExecutionReport.newBuilder().apply {
                    accountType = field(receiveMessage, "AccountType").toInt()
                    clOrdId = field(receiveMessage, "ClOrdID")
                    orderCapacity = field(receiveMessage, "OrderCapacity")
                    leavesQty = field(receiveMessage, "LeavesQty").toFloat()
                    side = field(receiveMessage, "Side")
                    cumQty = field(receiveMessage, "CumQty").toFloat()
                    execType = field(receiveMessage, "ExecType")
                    ordStatus = field(receiveMessage, "OrdStatus")
                    execId = field(receiveMessage, "ExecID")
                    price = field(receiveMessage, "Price").toFloat()
                    orderId = field(receiveMessage, "OrderID")
                    text = field(receiveMessage, "Text")
                    timeInForce = field(receiveMessage, "TimeInForce")
                    transactTime = field(receiveMessage, "TransactTime")
                }
                val headerFields = receiveMessage.fieldsMap["header"]
                if (headerFields != null) {
                    val headerField = headerFields.messageValue
                    executionReport.header = Header.newBuilder().apply {
                        beginString = field(headerField, "BeginString")
                        senderCompId = field(headerField, "SenderCompID")
                        sendingTime = field(headerField, "SendingTime")
                        msgSeqNum = field(headerField, "MsgSeqNum").toInt()
                        bodyLength = field(headerField, "BodyLength").toInt()
                        msgType = field(headerField, "MsgType")
                        targetCompId = field(headerField, "TargetCompID")
                    }.build()
                }

                val placeMessageResponseTyped: PlaceMessageResponseTyped =
                    PlaceMessageResponseTyped.newBuilder()
                        .setResponseMessageTyped(
                            ResponseMessageTyped.newBuilder()
                                .setExecutionReport(executionReport).build()
                        )
                        .setStatus(RequestStatus.newBuilder().setStatus(RequestStatus.Status.SUCCESS))
                        .setCheckpointId(checkpoint)
                        .build()

                emitResult(placeMessageResponseTyped)
            }
        LOGGER.debug("placeOrderFIX has finished")
    }

    override fun placeQuoteFIX(
        request: PlaceMessageRequestTyped,
        responseObserver: StreamObserver<PlaceMessageMultipleResponseTyped>
    ) {
        LOGGER.debug("placeQuoteFIX request: ${shortDebugString(request)}")

        actionFactory.createAction(responseObserver, "placeQuoteFIX", "Place quote FIX", request.parentEventId, 30_000)
            .preFilter { msg ->
                msg.messageType != "Heartbeat"
                        && msg.sessionAlias == request.metadata.id.connectionId.sessionAlias
                        && msg.direction == Direction.FIRST
            }
            .execute {
                val requestMessage = message(
                    request.metadata.messageType,
                    ConnectionID.newBuilder().setSessionAlias(request.metadata.id.connectionId.sessionAlias).build()
                ) {
                    parentEventId = request.parentEventId
                    body {
                        val quote = request.messageTyped.quote
                        "NoQuoteQualifiers" to quote.noQuoteQualifiersList.stream()
                            .map { Message.newBuilder().putFields("QuoteQualifier", it.quoteQualifier.toValue()) }
                            .toList()
                        "OfferPx" to quote.offerPx.toValue()
                        "OfferSize" to quote.offerSize.toValue()
                        "QuoteID" to quote.quoteId.toValue()
                        "Symbol" to quote.symbol.toValue()
                        "SecurityIDSource" to quote.securityIdSource.toValue()
                        "BidSize" to quote.bidSize.toValue()
                        "BidPx" to quote.bidPx.toValue()
                        "SecurityID" to quote.securityId.toValue()
                        "NoPartyIDs" to quote.noPartyIdsList.stream().map {
                            message {
                                "PartyID" to it.partyId.toValue()
                                "PartyIDSource" to it.partyIdSource.toValue()
                                "PartyRole" to it.partyRole.toValue()
                            }.build()
                        }.toList()
                        "QuoteType" to quote.quoteType.toValue()
                    }
                }

                val checkpoint = registerCheckPoint(requestMessage.parentEventId, request.description)

                send(requestMessage, requestMessage.sessionAlias)

                val quoteStatusReportReceive = receive(10_000, requestMessage.sessionAlias, Direction.FIRST) {
                    passOn("QuoteStatusReport") {
                        fieldsMap["QuoteID"] == requestMessage.fieldsMap["QuoteID"]
                                && fieldsMap["QuoteStatus"] == 0.toValue()
                    }
                    failOn("QuoteStatusReport") {
                        fieldsMap["QuoteID"] == requestMessage.fieldsMap["QuoteID"]
                                && fieldsMap["QuoteStatus"] == 5.toValue()
                    }
                }

                val quoteStatusReport = QuoteStatusReport.newBuilder().apply {
                    quoteId = field(quoteStatusReportReceive, "QuoteID")
                    quoteStatus = field(quoteStatusReportReceive, "QuoteStatus").toInt()
                    securityId = field(quoteStatusReportReceive, "SecurityID")
                    securityIdSource = field(quoteStatusReportReceive, "SecurityIDSource")
                    symbol = field(quoteStatusReportReceive, "Symbol")
                    text = field(quoteStatusReportReceive, "Text")
                }

                val quote = receive(10_000, requestMessage.sessionAlias, Direction.FIRST) {
                    passOn("Quote") {
                        fieldsMap["QuoteType"] == 0.toValue()
                                && fieldsMap["NoQuoteQualifiers"]?.listValue?.valuesList?.get(0)?.messageValue?.get(
                            "QuoteQualifier"
                        )?.simpleValue == "R"
                                && fieldsMap["Symbol"] == requestMessage.fieldsMap["Symbol"]
                    }
                }

                val placeMessageResponseTyped = mutableListOf<PlaceMessageResponseTyped>()

                placeMessageResponseTyped.add(
                    PlaceMessageResponseTyped.newBuilder()
                        .setResponseMessageTyped(
                            ResponseMessageTyped.newBuilder()
                                .setQuoteStatusReport(quoteStatusReport).build()
                        )
                        .setStatus(RequestStatus.newBuilder().setStatus(RequestStatus.Status.SUCCESS))
                        .setCheckpointId(checkpoint).build()
                )


                val quoteMes = Quote.newBuilder().apply {
                    addAllNoQuoteQualifiers(
                        quote.fieldsMap["NoQuoteQualifiers"]?.listValue?.valuesList?.stream()?.map { value ->
                            Quote.QuoteQualifier.newBuilder()
                                .setQuoteQualifier(field(value.messageValue, "QuoteQualifier")).build()
                        }?.toList()
                    )
                    offerPx = field(quote, "OfferPx").toFloat()
                    offerSize = field(quote, "OfferSize").toFloat()
                    quoteId = field(quote, "QuoteID")
                    symbol = field(quote, "Symbol")
                    securityIdSource = field(quote, "SecurityIDSource")
                    bidSize = field(quote, "BidSize")
                    bidPx = field(quote, "BidPx").toFloat()
                    securityId = field(quote, "SecurityID")
                    addAllNoPartyIds(
                        quote.fieldsMap["NoPartyIDs"]?.listValue?.valuesList?.stream()
                            ?.map { value ->
                                val msg: Message = value.messageValue
                                NoPartyIDs.newBuilder()
                                    .setPartyId(field(msg, "PartyID"))
                                    .setPartyIdSource(field(msg, "PartyIDSource"))
                                    .setPartyRole(field(msg, "PartyRole").toInt()).build()
                            }?.toList()
                    )
                    quoteType = field(quote, "QuoteType").toInt()
                }

                placeMessageResponseTyped.add(
                    PlaceMessageResponseTyped.newBuilder()
                        .setResponseMessageTyped(
                            ResponseMessageTyped.newBuilder()
                                .setQuote(quoteMes).build()
                        )
                        .setStatus(RequestStatus.newBuilder().setStatus(RequestStatus.Status.SUCCESS))
                        .setCheckpointId(checkpoint).build()
                )


                val placeMessageMultipleResponseTyped = PlaceMessageMultipleResponseTyped.newBuilder()
                    .addAllPlaceMessageResponseTyped(placeMessageResponseTyped).build()

                emitResult(placeMessageMultipleResponseTyped)
            }
        LOGGER.debug("placeQuoteFIX has finished")
    }

    override fun placeSecurityListRequest(
        request: PlaceMessageRequestTyped,
        responseObserver: StreamObserver<PlaceSecurityListResponse>
    ) {
        LOGGER.debug("placeSecurityListRequest: ${shortDebugString(request)}")

        actionFactory.createAction(
            responseObserver,
            "placeSecurityListRequest",
            "Place security list request",
            request.parentEventId,
            10_000
        )
            .preFilter { msg ->
                msg.messageType == "SecurityList"
                        && msg.fieldsMap["SecurityReqID"] == request.messageTyped.securityListRequest.securityReqId.toValue()
            }
            .execute {
                val requestMessage = message(
                    request.metadata.messageType,
                    ConnectionID.newBuilder().setSessionAlias(request.metadata.id.connectionId.sessionAlias).build()
                ) {
                    parentEventId = request.parentEventId
                    body {
                        val securityListRequest = request.messageTyped.securityListRequest
                        "SecurityListRequestType" to securityListRequest.securityListRequestType.toValue()
                        "SecurityReqID" to securityListRequest.securityReqId.toValue()
                    }
                }

                val checkpoint = registerCheckPoint(requestMessage.parentEventId, request.description)

                send(requestMessage, requestMessage.sessionAlias)

                val securityListRequest = repeat {
                    receive(10_000, requestMessage.sessionAlias, Direction.FIRST) {
                        passOn("SecurityList") { true }
                    }
                } until { msg -> msg.fieldsMap["LastFragment"] == false.toValue() }

                val placeSecurityListResponse = PlaceSecurityListResponse.newBuilder()
                    .setStatus(RequestStatus.newBuilder().setStatus(RequestStatus.Status.SUCCESS))
                    .setCheckpointId(checkpoint)

                val securityListDictionary = createSecurityListDictionary(securityListRequest)
                if (securityListDictionary.isNotEmpty()) {
                    placeSecurityListResponse.putAllSecurityListDictionary(securityListDictionary)
                }

                emitResult(placeSecurityListResponse.build())
            }
        LOGGER.debug("placeSecurityStatusRequest has finished")
    }

    private fun createSecurityListDictionary(responseMessage: List<Message>): Map<Int, Symbols> {
        val securityListDictionary = mutableMapOf<Int, Symbols>()
        responseMessage.forEach { message ->
            val securityList: MutableList<String> = ArrayList()
            val noRelatedSym = message.fieldsMap["NoRelatedSym"]
            if (noRelatedSym != null) {
                noRelatedSym.listValue.valuesList.forEach {
                    val symbol = it.messageValue.fieldsMap["Symbol"]
                    if (symbol != null) {
                        securityList.add(symbol.simpleValue)
                    }
                }
                for (i in 0 until securityList.size step 100) {
                    securityListDictionary[i / 100] =
                        Symbols.newBuilder().addAllSymbol(
                            securityList.subList(i, (i + 100).coerceAtMost(securityList.size))
                        ).build()
                }
            }
        }
        return securityListDictionary
    }

    private fun field(message: Message, key: String): String =
        try {
            message.fieldsMap[key]!!.simpleValue
        } catch (npe: NullPointerException) {
            LOGGER.error("There is no {} field in the message = {}", key, shortDebugString(message))
            throw npe
        }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(ActHandlerTyped::class.java)
    }
}