/******************************************************************************
 * Copyright 2009-2020 Exactpro (Exactpro Systems Limited)
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
 ******************************************************************************/
package com.exactpro.th2.act;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.exactpro.th2.connectivity.grpc.QueueInfoOrBuilder;
import com.exactpro.th2.infra.grpc.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.RabbitMqSubscriber;
import com.exactpro.th2.configuration.RabbitMQConfiguration;
import com.exactpro.th2.infra.grpc.MessageBatch;
import com.google.protobuf.InvalidProtocolBufferException;
import com.rabbitmq.client.Delivery;

public class MessageReceiver implements AutoCloseable {
    private final Logger logger = LoggerFactory.getLogger(getClass().getName() + '@' + hashCode());
    private final RabbitMqSubscriber subscriber;
    private final CheckRule checkRule;
    private final Lock responseLock = new ReentrantLock();
    private final Condition responseReceivedCondition = responseLock.newCondition();

    public MessageReceiver(RabbitMQConfiguration rabbitMQconfiguration, String subscriber, String exchangeName, String messageQueue, CheckRule checkRule)
            throws IOException, TimeoutException {
        long startTime = System.currentTimeMillis();

        this.checkRule = checkRule;
        this.subscriber = new RabbitMqSubscriber(exchangeName, this::processIncomingMessages, null, messageQueue);
        this.subscriber.startListening(rabbitMQconfiguration.getHost(), rabbitMQconfiguration.getVirtualHost(),
                rabbitMQconfiguration.getPort(), rabbitMQconfiguration.getUsername(), rabbitMQconfiguration.getPassword(), subscriber);

        logger.info("Receiver is created with MQ configuration, exchange name '{}', queue name {} during {}", exchangeName, messageQueue,  System.currentTimeMillis() - startTime);

    }

    public void awaitSync(long timeout, TimeUnit timeUnit) throws InterruptedException {
        if (getResponseMessage() == null) {
            try {
                responseLock.lock();
                responseReceivedCondition.await(timeout, timeUnit);
            } finally {
                responseLock.unlock();
            }
        }
    }

    public Message getResponseMessage() {
        return checkRule.getResponse();
    }

    public void close() {
        try {
            if (subscriber != null) {
                subscriber.close();
            }
        } catch (IOException e) {
            logger.error("Could not stop subscriber", e);
        }
    }

    private void processIncomingMessages(String consumingTag, Delivery delivery) {
        try {
            MessageBatch batch = MessageBatch.parseFrom(delivery.getBody());
            logger.debug("Message received batch, size {}", batch.getSerializedSize());
            for (Message message : batch.getMessagesList()) {
                if (checkRule.onMessage(message)) {
                    signalAboutReceived();
                }
            }
        } catch (InvalidProtocolBufferException e) {
            logger.error("Could not parse proto message", e);
        } catch (RuntimeException e) {
            logger.error("Could not process incoming message", e);
        }
    }

    private void signalAboutReceived() {
        try {
            responseLock.lock();
            responseReceivedCondition.signalAll();
        } finally {
            responseLock.unlock();
        }
    }
}
