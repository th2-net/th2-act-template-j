/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.act;

import com.exactpro.th2.check1.grpc.Check1Service;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.schema.factory.CommonFactory;
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter;
import com.exactpro.th2.common.schema.message.MessageListener;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class ActMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(ActMain.class);
    public static final String FIRST_ATTRIBUTE_NAME = "first";
    public static final String OE_ATTRIBUTE_NAME = "oe";
    public static final String SUBSCRIBE_ATTRIBUTE_NAME = "subscribe";

    public static void main(String[] args) {
        try {
            CommonFactory factory;
            try {
                factory = CommonFactory.createFromArguments(args);
            } catch (Exception e) {
                factory = new CommonFactory();
                LOGGER.warn("Can not create common factory from arguments", e);
            }

            GrpcRouter grpcRouter = factory.getGrpcRouter();

            MessageRouter<MessageBatch> messageRouterParsedBatch = factory.getMessageRouterParsedBatch();

            List<MessageListener<MessageBatch>> callbackList = new CopyOnWriteArrayList<>();
            SubscriberMonitor subscriberMonitor = messageRouterParsedBatch.subscribeAll((consumingTag, batch) ->
                    callbackList.forEach(callback -> {
                        try {
                            callback.handler(consumingTag, batch);
                        } catch (Exception e) {
                            LOGGER.error("Could not process incoming message", e);
                        }
                    }),
                    FIRST_ATTRIBUTE_NAME, OE_ATTRIBUTE_NAME, SUBSCRIBE_ATTRIBUTE_NAME);

            ActHandler actHandler = new ActHandler(
                    messageRouterParsedBatch,
                    callbackList,
                    factory.getEventBatchRouter(),
                    factory.getGrpcRouter().getService(Check1Service.class)
            );
            ActServer actServer = new ActServer(grpcRouter.startServer(actHandler));
            addShutdownHook(factory, subscriberMonitor, actServer);
            LOGGER.info("Act started");
            actServer.blockUntilShutdown();
        } catch (Throwable e) {
            LOGGER.error("Exit the program, caused by: {}", e.getMessage(), e);
            System.exit(-1);
        }
    }

    private static void addShutdownHook(CommonFactory commonFactory, SubscriberMonitor subscriberMonitor, ActServer actServer) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Act is terminating");
            try {
                commonFactory.close();
            } finally {
                LOGGER.info("CommonFactory closed");
            }
            try {
                subscriberMonitor.unsubscribe();
            } catch (Exception e) {
                LOGGER.error("Could not stop subscriber", e);
            }
            try {
                actServer.stop();
            } catch (InterruptedException e) {
                LOGGER.error("gRPC server shutdown is interrupted", e);
            }
            LOGGER.info("Act terminated");
        }));
    }
}
