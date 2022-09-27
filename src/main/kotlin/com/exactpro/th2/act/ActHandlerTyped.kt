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

import java.time.Instant
import java.time.format.DateTimeFormatter
import com.exactpro.th2.act.core.action.ActionFactory
import com.exactpro.th2.act.core.messages.message
import com.exactpro.th2.act.template.grpc.ActTypedGrpc.ActTypedImplBase
import com.exactpro.th2.act.template.grpc.PlaceOrderRequest
import com.exactpro.th2.act.template.grpc.PlaceOrderResponse
import com.exactpro.th2.act.template.grpc.Side
import com.exactpro.th2.common.grpc.Checkpoint
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.message.direction
import com.exactpro.th2.common.message.getLong
import com.exactpro.th2.common.message.getMessage
import com.exactpro.th2.common.message.messageType
import com.exactpro.th2.common.message.sessionAlias
import com.exactpro.th2.common.message.toJson
import io.grpc.stub.StreamObserver
import mu.KotlinLogging

class ActHandlerTyped(
    private val actionFactory: ActionFactory,
    private val configuration: Configuration,
) : ActTypedImplBase() {
    private val fixTimeFormat = DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss.SSSSSS")

    override fun placeOrderFix(
        request: PlaceOrderRequest,
        responseObserver: StreamObserver<PlaceOrderResponse>,
    ) {
        LOGGER.debug("placeOrderFIX request: ${request.toJson()}")

        actionFactory.createAction(responseObserver, "placeOrderFIX", "Place order FIX", request.parentEventId)
            .preFilter { msg ->
                msg.messageType != "Heartbeat"
                        && msg.sessionAlias == request.connection.sessionAlias
                        && msg.direction == Direction.FIRST
            }
            .execute {
                val requestMessage = message(
                    "NewOrderSingle",
                    request.connection
                ) {
                    parentEventId = request.parentEventId

                    body {
                        configuration.instruments[request.symbol]?.also {
                            "Instrument" to message {
                                "Symbol" to request.symbol
                                "SecurityID" to it.securityId
                                "SecurityIDSource" to it.securityIdSource
                            }
                        }
                        "OrdType" to request.ordType
                        "AccountType" to 1
                        "OrderCapacity" to "A"
                        "OrderQty" to request.orderQty
                        "Price" to request.price
                        "ClOrdID" to if (request.hasClOrdId()) request.clOrdId else generateClOrdId(10)
                        "Side" to when (request.side) {
                            Side.SIDE_BUY -> "1"
                            Side.SIDE_SELL -> "2"
                            Side.UNRECOGNIZED, Side.SIDE_UNSPECIFIED, null -> error("Unexpected side: ${request.side}")
                        }
                        "TimeInForce" to if (request.hasTimeInForce()) request.timeInForce else "0"
                        "TransactTime" to fixTimeFormat.format(Instant.now())

                        if (request.tradingPartyCount > 0) {
                            "TradingParty" to message {
                                "NoPartyIDs" buildList {
                                    request.tradingPartyList.forEach {
                                        addMessage {
                                            "PartyID" to it.id
                                            "PartyIDSource" to it.source
                                            "PartyRole" to it.role
                                        }
                                    }
                                }
                            }
                        } else {
                            configuration.parties[request.connection.sessionAlias]?.also { perties ->
                                "TradingParty" to message {
                                    "NoPartyIDs" buildList {
                                        perties.forEach {
                                            addMessage {
                                                "PartyID" to it.partyId
                                                "PartyIDSource" to it.partyIdSource
                                                "PartyRole" to it.partyRole
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                val checkpoint: Checkpoint = registerCheckPoint(requestMessage.parentEventId, "Adding new order")

                val echoMessage = send(requestMessage, timeout = 10_000, waitEcho = true)

                val receiveMessage = receive(10_000, requestMessage.sessionAlias, Direction.FIRST) {
                    passOn("ExecutionReport") {
                        fieldsMap["ClOrdID"] == requestMessage.fieldsMap["ClOrdID"]
                    }
                    failOn("BusinessMessageReject") {
                        fieldsMap["BusinessRejectRefID"] == requestMessage.fieldsMap["ClOrdID"]
                    }
                    failOn("Reject") {
                        getLong("RefSeqNum") == echoMessage
                            .getMessage("header")!!
                            .getLong("MsgSeqNum")
                    }
                }

                emitResult(
                    PlaceOrderResponse.newBuilder()
                        .setCheckpoint(checkpoint)
                        .setResponse(receiveMessage)
                        .build()
                )
            }

        LOGGER.debug("placeOrderFIX has finished")
    }

    private fun generateClOrdId(size: Int): String = System.currentTimeMillis().toString().padStart(size)

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}