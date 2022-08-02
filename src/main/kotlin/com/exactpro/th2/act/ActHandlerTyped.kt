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
import com.exactpro.th2.check1.grpc.Check1Service
import com.exactpro.th2.check1.grpc.CheckpointRequest
import com.exactpro.th2.check1.grpc.CheckpointResponse
import com.exactpro.th2.common.grpc.*
import com.exactpro.th2.common.message.messageType
import com.exactpro.th2.common.message.sessionAlias
import com.google.protobuf.TextFormat
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory

class ActHandlerTyped(
    private val actionFactory: ActionFactory,
    private val check1Service: Check1Service
) : ActTypedImplBase() {

    private val convertorsRequest = ConvertorsRequest()
    private val convertorsResponse = ConvertorsResponse()

    override fun placeOrderFIX(
        request: PlaceMessageRequestTyped,
        responseObserver: StreamObserver<PlaceMessageResponseTyped>
    ) {
        val requestMessage = convertorsRequest.createMessage(request)
        LOGGER.debug("placeOrderFIX request: $requestMessage")

        val parentId = requestMessage.parentEventId
        val checkpoint: Checkpoint = registerCheckPoint(parentId)

        actionFactory.apply {
            createAction(responseObserver, "placeOrderFIX", "placeOrderFIX", parentId, 10_000)
                .preFilter { msg -> msg.messageType != "Heartbeat" }
                .execute {
                    send(requestMessage, requestMessage.sessionAlias, 1_000)

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
                            .setResponseMessageTyped(convertorsResponse.createResponseMessage(receiveMessage))
                            .setStatus(RequestStatus.newBuilder().setStatus(RequestStatus.Status.SUCCESS))
                            .setCheckpointId(checkpoint)
                            .build()

                    emitResult(placeMessageResponseTyped)
                    responseObserver.onCompleted()
                }
        }
        LOGGER.debug("placeOrderFIX has finished")
    }

    override fun sendMessage(
        request: PlaceMessageRequestTyped,
        responseObserver: StreamObserver<SendMessageResponse>
    ) {
        val requestMessage = convertorsRequest.createMessage(request)
        LOGGER.debug("Sending  message request: $requestMessage")

        val parentId = requestMessage.parentEventId
        val checkpoint = registerCheckPoint(parentId)

        actionFactory.apply {
            createAction(responseObserver, "sendMessage", "sendMessage", parentId, 10_000)
                .execute {
                    send(requestMessage, requestMessage.sessionAlias, 1_000)

                    val response = SendMessageResponse.newBuilder()
                        .setStatus(RequestStatus.newBuilder().setStatus(RequestStatus.Status.SUCCESS))
                        .setCheckpointId(checkpoint)
                        .build()

                    emitResult(response)
                    responseObserver.onCompleted()
                }
        }
        LOGGER.debug("Sending the message has been finished")
    }

    override fun placeOrderMassCancelRequestFIX(
        request: PlaceMessageRequestTyped,
        responseObserver: StreamObserver<PlaceMessageResponseTyped>
    ) {
        val requestMessage = convertorsRequest.createMessage(request)
        LOGGER.debug("placeOrderMassCancelRequestFIX request: $requestMessage")

        val parentId = requestMessage.parentEventId
        val checkpoint = registerCheckPoint(parentId)

        actionFactory.apply {
            createAction(
                responseObserver,
                "placeOrderMassCancelRequestFIX",
                "placeOrderMassCancelRequestFIX",
                parentId,
                10_000
            )
                .preFilter { msg -> msg.messageType != "Heartbeat" }
                .execute {
                    send(requestMessage, requestMessage.sessionAlias, 1_000)

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
                    responseObserver.onCompleted()
                }
        }
        LOGGER.debug("placeOrderMassCancelRequestFIX finished")
    }

    override fun placeQuoteCancelFIX(
        request: PlaceMessageRequestTyped,
        responseObserver: StreamObserver<PlaceMessageResponseTyped>
    ) {
        val requestMessage = convertorsRequest.createMessage(request)
        LOGGER.debug("placeQuoteCancelFIX request: {}", requestMessage)

        val parentId = requestMessage.parentEventId
        val checkpoint = registerCheckPoint(parentId)

        actionFactory.apply {
            createAction(responseObserver, "placeQuoteCancelFIX", "placeQuoteCancelFIX", parentId, 10_000)
                .preFilter { msg -> msg.messageType != "Heartbeat" }
                .execute {
                    send(requestMessage, requestMessage.sessionAlias, 1_000)

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
                    responseObserver.onCompleted()
                }
        }
        LOGGER.debug("placeQuoteCancelFIX has finished")
    }

    override fun placeSecurityListRequest(
        request: PlaceMessageRequestTyped,
        responseObserver: StreamObserver<PlaceSecurityListResponse>
    ) {
        val requestMessage = convertorsRequest.createMessage(request)
        LOGGER.debug("placeSecurityListRequest: $requestMessage")

        val parentId = requestMessage.parentEventId
        val checkpoint = registerCheckPoint(parentId)

        actionFactory.apply {
            createAction(
                responseObserver,
                "placeSecurityListRequest",
                "placeSecurityListRequest",
                parentId,
                10_000
            )
                .preFilter { msg -> msg.messageType != "Heartbeat" }
                .execute {
                    send(requestMessage, requestMessage.sessionAlias, 1_000)

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
                    responseObserver.onCompleted()
                }
        }
        LOGGER.debug("placeSecurityStatusRequest has finished")
    }

    override fun placeQuoteRequestFIX(
        request: PlaceMessageRequestTyped,
        responseObserver: StreamObserver<PlaceMessageResponseTyped>
    ) {
        val requestMessage = convertorsRequest.createMessage(request)
        LOGGER.debug("placeQuoteRequestFIX request: {}", requestMessage)

        val parentId = requestMessage.parentEventId
        val checkpoint = registerCheckPoint(parentId)

        actionFactory.apply {
            createAction(
                responseObserver,
                "placeQuoteRequestFIX",
                "placeQuoteRequestFIX",
                parentId,
                10_000
            )
                .preFilter { msg -> msg.messageType != "Heartbeat" }
                .execute {
                    send(requestMessage, requestMessage.sessionAlias, 1_000)

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
                    responseObserver.onCompleted()
                }
        }
        LOGGER.debug("placeQuoteRequestFIX has finished")
    }

    override fun placeQuoteResponseFIX(
        request: PlaceMessageRequestTyped,
        responseObserver: StreamObserver<PlaceMessageResponseTyped>
    ) {
        val requestMessage = convertorsRequest.createMessage(request)
        LOGGER.debug("placeQuoteResponseFIX request: {}", requestMessage)

        val parentId = requestMessage.parentEventId
        val checkpoint = registerCheckPoint(parentId)

        actionFactory.apply {
            createAction(
                responseObserver,
                "placeQuoteResponseFIX",
                "placeQuoteResponseFIX",
                parentId,
                10_000
            )
                .preFilter { msg -> msg.messageType != "Heartbeat" }
                .execute {
                    send(requestMessage, requestMessage.sessionAlias, 1_000)

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
                    responseObserver.onCompleted()
                }
        }
        LOGGER.debug("placeQuoteResponseFIX has finished")
    }

    override fun placeQuoteFIX(
        request: PlaceMessageRequestTyped,
        responseObserver: StreamObserver<PlaceMessageMultipleResponseTyped>
    ) {
        val requestMessage = convertorsRequest.createMessage(request)
        LOGGER.debug("placeQuoteFIX request: {}", requestMessage)

        val parentId = requestMessage.parentEventId
        val checkpoint = registerCheckPoint(parentId)

        actionFactory.apply {
            createAction(responseObserver, "placeQuoteFIX", "placeQuoteFIX", parentId, 10_000)
                .preFilter { msg -> msg.messageType != "Heartbeat" }
                .execute {
                    send(requestMessage, requestMessage.sessionAlias, 1_000)

                    val receiveMessage = receive(10_000, requestMessage.sessionAlias, Direction.FIRST) {
                        passOn("QuoteAck") {
                            fieldsMap["RFQID"] == requestMessage.fieldsMap["RFQID"]
                        }
                    }

                    val placeMessageMultipleResponseTyped = PlaceMessageMultipleResponseTyped.newBuilder()
                        .addPlaceMessageResponseTyped(
                            PlaceMessageResponseTyped.newBuilder()
                                .setResponseMessageTyped(convertorsResponse.createResponseMessage(receiveMessage))
                                .setStatus(RequestStatus.newBuilder().setStatus(RequestStatus.Status.SUCCESS))
                                .setCheckpointId(checkpoint)
                        ).build()

                    emitResult(placeMessageMultipleResponseTyped)
                    responseObserver.onCompleted()
                }
        }
        LOGGER.debug("placeQuoteFIX has finished")
    }

    private fun registerCheckPoint(parentEventId: EventID): Checkpoint {
        LOGGER.debug("Registering the checkpoint started")
        val response: CheckpointResponse = check1Service.createCheckpoint(
            CheckpointRequest.newBuilder().setParentEventId(parentEventId).build()
        )
        LOGGER.debug("Registering the checkpoint ended. Response " + TextFormat.shortDebugString(response))
        return response.checkpoint
    }

    private fun createSecurityListDictionary(responseMessage: Message): Map<Int, Symbols> {
        val securityList: MutableList<String> = ArrayList()
        val noRelatedSym = responseMessage.fieldsMap["NoRelatedSym"]
        if (noRelatedSym != null) {
            for (value in noRelatedSym.listValue.valuesList) {
                val symbol = value.messageValue.fieldsMap["Symbol"]
                if (symbol != null) {
                    securityList.add(symbol.simpleValue)
                }
            }
            val securityListDictionary = mutableMapOf<Int, Symbols>()
            var end = 0
            while (end < securityList.size) {
                securityListDictionary[end / 100] = Symbols.newBuilder()
                    .addAllSymbol(securityList.subList(end, (end + 100).coerceAtMost(securityList.size))).build()
                end += 100
            }
            return securityListDictionary
        }
        return mutableMapOf()
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(ActHandlerTyped::class.java)
    }
}