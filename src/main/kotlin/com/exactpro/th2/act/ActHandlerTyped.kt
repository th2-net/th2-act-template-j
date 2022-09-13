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

import com.exactpro.th2.act.convertors.ConvertorsRequest
import com.exactpro.th2.act.convertors.ConvertorsResponse
import com.exactpro.th2.act.core.action.ActionFactory
import com.exactpro.th2.act.grpc.*
import com.exactpro.th2.act.grpc.ActTypedGrpc.ActTypedImplBase
import com.exactpro.th2.common.grpc.Checkpoint
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.RequestStatus
import com.exactpro.th2.common.message.*
import com.exactpro.th2.common.value.toValue
import com.google.protobuf.TextFormat.shortDebugString
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory

class ActHandlerTyped(
    private val actionFactory: ActionFactory
) : ActTypedImplBase() {

    private val convertorsRequest = ConvertorsRequest()
    private val convertorsResponse = ConvertorsResponse()

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
                val requestMessage = convertorsRequest.createMessage(request)

                val checkpoint: Checkpoint = registerCheckPoint(requestMessage.parentEventId, request.description)

                val rejectMessage = send(requestMessage, requestMessage.sessionAlias)

                val receiveMessage = receive(10_000, requestMessage.sessionAlias, Direction.FIRST) {
                    passOn("ExecutionReport") {
                        fieldsMap["ClOrdID"] == requestMessage.fieldsMap["ClOrdID"]
                    }
                    failOn("BusinessMessageReject") {
                        fieldsMap["BusinessRejectRefID"] == requestMessage.fieldsMap["ClOrdID"]
                    }
                    failOn(rejectMessage.messageType) { this.sequence == rejectMessage.sequence }
                }

                val placeMessageResponseTyped: PlaceMessageResponseTyped =
                    PlaceMessageResponseTyped.newBuilder()
                        .setResponseMessageTyped(convertorsResponse.createResponseMessage(receiveMessage))
                        .setStatus(RequestStatus.newBuilder().setStatus(RequestStatus.Status.SUCCESS))
                        .setCheckpointId(checkpoint)
                        .build()

                emitResult(placeMessageResponseTyped)
            }
        LOGGER.debug("placeOrderFIX has finished")
    }

    override fun sendMessage(
        request: PlaceMessageRequestTyped,
        responseObserver: StreamObserver<SendMessageResponse>
    ) {
        LOGGER.debug("Sending  message request: ${shortDebugString(request)}")

        actionFactory.createAction(responseObserver, "sendMessage", "Send message", request.parentEventId, 10_000)
            .execute {
                val requestMessage = convertorsRequest.createMessage(request)
                val checkpoint = registerCheckPoint(requestMessage.parentEventId, request.description)

                send(requestMessage, requestMessage.sessionAlias)

                val response = SendMessageResponse.newBuilder()
                    .setStatus(RequestStatus.newBuilder().setStatus(RequestStatus.Status.SUCCESS))
                    .setCheckpointId(checkpoint)
                    .build()

                emitResult(response)
            }
        LOGGER.debug("Sending the message has been finished")
    }

    override fun placeOrderMassCancelRequestFIX(
        request: PlaceMessageRequestTyped,
        responseObserver: StreamObserver<PlaceMessageResponseTyped>
    ) {
        LOGGER.debug("placeOrderMassCancelRequestFIX request: ${shortDebugString(request)}")

        actionFactory.createAction(
            responseObserver,
            "placeOrderMassCancelRequestFIX",
            "Place order mass cancel request FIX",
            request.parentEventId,
            10_000
        )
            .preFilter { msg ->
                msg.messageType != "Heartbeat"
                        && msg.sessionAlias == request.metadata.id.connectionId.sessionAlias
                        && msg.direction == Direction.FIRST
            }
            .execute {
                val requestMessage = convertorsRequest.createMessage(request)
                val checkpoint = registerCheckPoint(requestMessage.parentEventId, request.description)

                send(requestMessage, requestMessage.sessionAlias)

                val receiveMessage = receive(10_000, requestMessage.sessionAlias, Direction.FIRST) {
                    passOn("OrderMassCancelReport") {
                        fieldsMap["ClOrdID"] == requestMessage.fieldsMap["ClOrdID"]
                    }
                }

                val placeMessageResponseTyped: PlaceMessageResponseTyped = PlaceMessageResponseTyped.newBuilder()
                    .setResponseMessageTyped(convertorsResponse.createResponseMessage(receiveMessage))
                    .setStatus(RequestStatus.newBuilder().setStatus(RequestStatus.Status.SUCCESS))
                    .setCheckpointId(checkpoint)
                    .build()

                emitResult(placeMessageResponseTyped)
            }
        LOGGER.debug("placeOrderMassCancelRequestFIX finished")
    }

    override fun placeQuoteCancelFIX(
        request: PlaceMessageRequestTyped,
        responseObserver: StreamObserver<PlaceMessageResponseTyped>
    ) {
        LOGGER.debug("placeQuoteCancelFIX request: ${shortDebugString(request)}")

        actionFactory.createAction(
            responseObserver,
            "placeQuoteCancelFIX",
            "Place quote cancel FIX",
            request.parentEventId,
            10_000
        )
            .preFilter { msg ->
                msg.messageType != "Heartbeat"
                        && msg.sessionAlias == request.metadata.id.connectionId.sessionAlias
                        && msg.direction == Direction.FIRST
            }
            .execute {
                val requestMessage = convertorsRequest.createMessage(request)
                val checkpoint = registerCheckPoint(requestMessage.parentEventId, request.description)

                send(requestMessage, requestMessage.sessionAlias)

                val receiveMessage = receive(10_000, requestMessage.sessionAlias, Direction.FIRST) {
                    passOn("MassQuoteAcknowledgement") {
                        fieldsMap["QuoteID"] == requestMessage.fieldsMap["QuoteMsgID"]
                    }
                }

                val placeMessageResponseTyped: PlaceMessageResponseTyped = PlaceMessageResponseTyped.newBuilder()
                    .setResponseMessageTyped(convertorsResponse.createResponseMessage(receiveMessage))
                    .setStatus(RequestStatus.newBuilder().setStatus(RequestStatus.Status.SUCCESS))
                    .setCheckpointId(checkpoint)
                    .build()

                emitResult(placeMessageResponseTyped)
            }
        LOGGER.debug("placeQuoteCancelFIX has finished")
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
                val requestMessage = convertorsRequest.createMessage(request)
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

    override fun placeQuoteRequestFIX(
        request: PlaceMessageRequestTyped,
        responseObserver: StreamObserver<PlaceMessageResponseTyped>
    ) {
        LOGGER.debug("placeQuoteRequestFIX request: ${shortDebugString(request)}")

        actionFactory.createAction(
            responseObserver,
            "placeQuoteRequestFIX",
            "Place quote request FIX",
            request.parentEventId,
            10_000
        )
            .preFilter { msg ->
                msg.messageType != "Heartbeat"
                        && msg.sessionAlias == request.metadata.id.connectionId.sessionAlias
                        && msg.direction == Direction.FIRST
            }
            .execute {
                val requestMessage = convertorsRequest.createMessage(request)
                val checkpoint = registerCheckPoint(requestMessage.parentEventId, request.description)

                send(requestMessage, requestMessage.sessionAlias)

                val receiveMessage = receive(10_000, requestMessage.sessionAlias, Direction.FIRST) {
                    passOn("QuoteStatusReport") {
                        fieldsMap["QuoteReqID"] == requestMessage.fieldsMap["QuoteReqID"]
                    }
                }

                val placeMessageResponseTyped: PlaceMessageResponseTyped = PlaceMessageResponseTyped.newBuilder()
                    .setResponseMessageTyped(convertorsResponse.createResponseMessage(receiveMessage))
                    .setStatus(RequestStatus.newBuilder().setStatus(RequestStatus.Status.SUCCESS))
                    .setCheckpointId(checkpoint)
                    .build()

                emitResult(placeMessageResponseTyped)
            }
        LOGGER.debug("placeQuoteRequestFIX has finished")
    }

    override fun placeQuoteResponseFIX(
        request: PlaceMessageRequestTyped,
        responseObserver: StreamObserver<PlaceMessageResponseTyped>
    ) {
        LOGGER.debug("placeQuoteResponseFIX request: ${shortDebugString(request)}")

        actionFactory.createAction(
            responseObserver,
            "placeQuoteResponseFIX",
            "Place quote response FIX",
            request.parentEventId,
            10_000
        )
            .preFilter { msg ->
                msg.messageType != "Heartbeat"
                        && msg.sessionAlias == request.metadata.id.connectionId.sessionAlias
                        && msg.direction == Direction.FIRST
            }
            .execute {
                val requestMessage = convertorsRequest.createMessage(request)
                val checkpoint = registerCheckPoint(requestMessage.parentEventId, request.description)

                send(requestMessage, requestMessage.sessionAlias)

                val receiveMessage = receive(10_000, requestMessage.sessionAlias, Direction.FIRST) {
                    passOn("ExecutionReport") {
                        fieldsMap["RFQID"] == requestMessage.fieldsMap["RFQID"]
                    }
                    passOn("QuoteStatusReport") {
                        fieldsMap["RFQID"] == requestMessage.fieldsMap["RFQID"]
                    }
                }

                val placeMessageResponseTyped: PlaceMessageResponseTyped = PlaceMessageResponseTyped.newBuilder()
                    .setResponseMessageTyped(convertorsResponse.createResponseMessage(receiveMessage))
                    .setStatus(RequestStatus.newBuilder().setStatus(RequestStatus.Status.SUCCESS))
                    .setCheckpointId(checkpoint)
                    .build()

                emitResult(placeMessageResponseTyped)
            }
        LOGGER.debug("placeQuoteResponseFIX has finished")
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
                val requestMessage = convertorsRequest.createMessage(request)
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

                val quote = receive(10_000, requestMessage.sessionAlias, Direction.FIRST) {
                    passOn("Quote") {
                        (fieldsMap["QuoteType"] == 0.toValue()
                                && fieldsMap["NoQuoteQualifiers"]?.listValue?.valuesList?.get(0)?.messageValue?.get("QuoteQualifier")?.simpleValue == "R"
                                && fieldsMap["Symbol"] == requestMessage.fieldsMap["Symbol"])
                    }
                }

                val placeMessageResponseTyped = mutableListOf<PlaceMessageResponseTyped>()

                placeMessageResponseTyped.add(
                    PlaceMessageResponseTyped.newBuilder()
                        .setResponseMessageTyped(convertorsResponse.createResponseMessage(quoteStatusReportReceive))
                        .setStatus(RequestStatus.newBuilder().setStatus(RequestStatus.Status.SUCCESS))
                        .setCheckpointId(checkpoint).build()
                )


                placeMessageResponseTyped.add(
                    PlaceMessageResponseTyped.newBuilder()
                        .setResponseMessageTyped(convertorsResponse.createResponseMessage(quote))
                        .setStatus(RequestStatus.newBuilder().setStatus(RequestStatus.Status.SUCCESS))
                        .setCheckpointId(checkpoint).build()
                )

                val placeMessageMultipleResponseTyped = PlaceMessageMultipleResponseTyped.newBuilder()
                    .addAllPlaceMessageResponseTyped(placeMessageResponseTyped).build()

                emitResult(placeMessageMultipleResponseTyped)
            }
        LOGGER.debug("placeQuoteFIX has finished")
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

    companion object {
        private val LOGGER = LoggerFactory.getLogger(ActHandlerTyped::class.java)
    }
}