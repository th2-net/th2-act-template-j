/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.exactpro.th2.act;

import com.exactpro.th2.infra.grpc.Message;
import com.exactpro.th2.infra.grpc.MessageBatch;
import com.exactpro.th2.schema.message.MessageListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MessageReceiver implements AutoCloseable {
    private final Logger logger = LoggerFactory.getLogger(getClass().getName() + '@' + hashCode());
    private final List<MessageListener<MessageBatch>> callbackList;
    private final MessageListener<MessageBatch> callback = this::processIncomingMessages;
    private final CheckRule checkRule;
    private final Lock responseLock = new ReentrantLock();
    private final Condition responseReceivedCondition = responseLock.newCondition();

    //FIXME: Add queue name
    public MessageReceiver(List<MessageListener<MessageBatch>> callbackList, CheckRule checkRule) {
        this.callbackList = callbackList;
        this.callbackList.add(callback);
        this.checkRule = checkRule;
        //logger.info("Receiver is created with MQ configuration, queue name '{}' during '{}'", messageQueue,  System.currentTimeMillis() - startTime);

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
        this.callbackList.remove(callback);
    }

    private void processIncomingMessages(String consumingTag, MessageBatch batch) {
        try {
            logger.debug("Message received batch, size {}", batch.getSerializedSize());
            for (Message message : batch.getMessagesList()) {
                if (checkRule.onMessage(message)) {
                    signalAboutReceived();
                }
            }
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
