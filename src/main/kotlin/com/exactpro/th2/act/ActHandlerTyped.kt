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

package com.exactpro.th2.act

import com.exactpro.th2.act.convertors.ConvertorsRequest
import com.exactpro.th2.act.convertors.ConvertorsResponse
import com.exactpro.th2.act.core.action.ActionFactory
import com.exactpro.th2.act.grpc.*
import com.exactpro.th2.act.grpc.ActTypedGrpc.ActTypedImplBase
import com.exactpro.th2.common.grpc.*
import com.exactpro.th2.common.message.direction
import com.exactpro.th2.common.message.messageType
import com.exactpro.th2.common.message.sessionAlias
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

        actionFactory.createAction(responseObserver, "placeOrderFIX", "placeOrderFIX", request.parentEventId, 10_000)
            .preFilter { msg ->
                msg.messageType != "Heartbeat"
                        && msg.sessionAlias == request.metadata.id.connectionId.sessionAlias
                        && msg.direction == Direction.FIRST
            }
            .execute {
                val requestMessage = convertorsRequest.createMessage(request)
                val checkpoint: Checkpoint = registerCheckPoint(requestMessage.parentEventId)

                send(requestMessage, requestMessage.sessionAlias)

                val receiveMessage = receive(10_000, requestMessage.sessionAlias, Direction.FIRST) {
                    passOn("ExecutionReport") {
                        fieldsMap["ClOrdID"] == requestMessage.fieldsMap["ClOrdID"]
                    }
                    failOn("BusinessMessageReject") {
                        fieldsMap["BusinessRejectRefID"] == requestMessage.fieldsMap["ClOrdID"]
                    }
                }

                val placeMessageResponseTyped: PlaceMessageResponseTyped =
                    PlaceMessageResponseTyped.newBuilder()
                        .setResponseMessage(convertorsResponse.createResponseMessage(receiveMessage))
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

        actionFactory.createAction(responseObserver, "sendMessage", "sendMessage", request.parentEventId, 10_000)
            .execute {
                val requestMessage = convertorsRequest.createMessage(request)
                val checkpoint = registerCheckPoint(requestMessage.parentEventId)

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
            "placeOrderMassCancelRequestFIX",
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
                val checkpoint = registerCheckPoint(requestMessage.parentEventId)

                send(requestMessage, requestMessage.sessionAlias)

                val receiveMessage = receive(10_000, requestMessage.sessionAlias, Direction.FIRST) {
                    passOn("OrderMassCancelReport") {
                        fieldsMap["ClOrdID"] == requestMessage.fieldsMap["ClOrdID"]
                    }
                }

                val placeMessageResponseTyped: PlaceMessageResponseTyped = PlaceMessageResponseTyped.newBuilder()
                    .setResponseMessage(convertorsResponse.createResponseMessage(receiveMessage))
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

        actionFactory.createAction(responseObserver, "placeQuoteCancelFIX", "placeQuoteCancelFIX", request.parentEventId, 10_000)
            .preFilter { msg ->
                msg.messageType != "Heartbeat"
                        && msg.sessionAlias == request.metadata.id.connectionId.sessionAlias
                        && msg.direction == Direction.FIRST
            }
            .execute {
                val requestMessage = convertorsRequest.createMessage(request)
                val checkpoint = registerCheckPoint(requestMessage.parentEventId)

                send(requestMessage, requestMessage.sessionAlias)

                val receiveMessage = receive(10_000, requestMessage.sessionAlias, Direction.FIRST) {
                    passOn("MassQuoteAcknowledgement") {
                        fieldsMap["QuoteID"] == requestMessage.fieldsMap["QuoteMsgID"]
                    }
                }

                val placeMessageResponseTyped: PlaceMessageResponseTyped = PlaceMessageResponseTyped.newBuilder()
                    .setResponseMessage(convertorsResponse.createResponseMessage(receiveMessage))
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
            "placeSecurityListRequest",
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
                val checkpoint = registerCheckPoint(requestMessage.parentEventId)

                send(requestMessage, requestMessage.sessionAlias)

                val receiveMessage = receive(10_000, requestMessage.sessionAlias, Direction.FIRST) {
                    passOn("SecurityStatus") {
                        fieldsMap["SecurityID"] == requestMessage.fieldsMap["SecurityID"]
                    }
                }

                val placeSecurityListResponse = PlaceSecurityListResponse.newBuilder()
                    .setStatus(RequestStatus.newBuilder().setStatus(RequestStatus.Status.SUCCESS))
                    .setCheckpointId(checkpoint)

                val securityListDictionary = createSecurityListDictionary(receiveMessage)
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
            "placeQuoteRequestFIX",
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
                val checkpoint = registerCheckPoint(requestMessage.parentEventId)

                send(requestMessage, requestMessage.sessionAlias)

                val receiveMessage = receive(10_000, requestMessage.sessionAlias, Direction.FIRST) {
                    passOn("QuoteStatusReport") {
                        fieldsMap["QuoteReqID"] == requestMessage.fieldsMap["QuoteReqID"]
                    }
                }

                val placeMessageResponseTyped: PlaceMessageResponseTyped = PlaceMessageResponseTyped.newBuilder()
                    .setResponseMessage(convertorsResponse.createResponseMessage(receiveMessage))
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
            "placeQuoteResponseFIX",
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
                val checkpoint = registerCheckPoint(requestMessage.parentEventId)

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
                    .setResponseMessage(convertorsResponse.createResponseMessage(receiveMessage))
                    .setStatus(RequestStatus.newBuilder().setStatus(RequestStatus.Status.SUCCESS))
                    .setCheckpointId(checkpoint)
                    .build()

                emitResult(placeMessageResponseTyped)
            }
        LOGGER.debug("placeQuoteResponseFIX has finished")
    }

    override fun placeQuoteFIX(
        request: PlaceMessageRequestTyped,
        responseObserver: StreamObserver<PlaceMessageMultiResponseTyped>
    ) {
        LOGGER.debug("placeQuoteFIX request: ${shortDebugString(request)}")

        actionFactory.createAction(responseObserver, "placeQuoteFIX", "placeQuoteFIX", request.parentEventId, 10_000)
            .preFilter { msg ->
                msg.messageType != "Heartbeat"
                        && msg.sessionAlias == request.metadata.id.connectionId.sessionAlias
                        && msg.direction == Direction.FIRST
            }
            .execute {
                val requestMessage = convertorsRequest.createMessage(request)
                val checkpoint = registerCheckPoint(requestMessage.parentEventId)

                send(requestMessage, requestMessage.sessionAlias)

                val receiveMessage = receive(10_000, requestMessage.sessionAlias, Direction.FIRST) {
                    passOn("QuoteAck") {
                        fieldsMap["RFQID"] == requestMessage.fieldsMap["RFQID"]
                    }
                }

                val placeMessageMultipleResponseTyped = PlaceMessageMultiResponseTyped.newBuilder()
                    .addPlaceMessageResponseTyped(
                        PlaceMessageResponseTyped.newBuilder()
                            .setResponseMessage(convertorsResponse.createResponseMessage(receiveMessage))
                            .setStatus(RequestStatus.newBuilder().setStatus(RequestStatus.Status.SUCCESS))
                            .setCheckpointId(checkpoint)
                    ).build()

                emitResult(placeMessageMultipleResponseTyped)
            }
        LOGGER.debug("placeQuoteFIX has finished")
    }

    private fun createSecurityListDictionary(responseMessage: Message): Map<Int, Symbols> {
        val securityList: MutableList<String> = ArrayList()
        val noRelatedSym = responseMessage.fieldsMap["NoRelatedSym"]
        if (noRelatedSym != null) {
            noRelatedSym.listValue.valuesList.forEach {
                val symbol = it.messageValue.fieldsMap["Symbol"]
                if (symbol != null) {
                    securityList.add(symbol.simpleValue)
                }
            }
            val securityListDictionary = mutableMapOf<Int, Symbols>()
            for (i in 0 until securityList.size step 100) {
                securityListDictionary[i / 100] =
                    Symbols.newBuilder().addAllSymbol(
                        securityList.subList(i, (i + 100).coerceAtMost(securityList.size))
                    ).build()
            }
            return securityListDictionary
        }
        return mutableMapOf()
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(ActHandlerTyped::class.java)
    }
}