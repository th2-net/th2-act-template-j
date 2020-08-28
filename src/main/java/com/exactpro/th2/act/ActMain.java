/******************************************************************************
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
 ******************************************************************************/
package com.exactpro.th2.act;

import com.exactpro.th2.configuration.MicroserviceConfiguration;
import com.exactpro.th2.configuration.RabbitMQConfiguration;
import com.exactpro.th2.configuration.Th2Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.exactpro.th2.ConfigurationUtils.safeLoad;

public class ActMain {

    private final static Logger LOGGER = LoggerFactory.getLogger(ActMain.class);

    /**
     * Environment variables:
     *  {@link com.exactpro.th2.configuration.Configuration#ENV_GRPC_PORT}
     *  {@link RabbitMQConfiguration#ENV_RABBITMQ_HOST}
     *  {@link RabbitMQConfiguration#ENV_RABBITMQ_PORT}
     *  {@link RabbitMQConfiguration#ENV_RABBITMQ_USER}
     *  {@link RabbitMQConfiguration#ENV_RABBITMQ_PASS}
     *  {@link RabbitMQConfiguration#ENV_RABBITMQ_VHOST}
     *  {@link Th2Configuration#ENV_RABBITMQ_EXCHANGE_NAME_TH2_CONNECTIVITY}
     *  {@link Th2Configuration#ENV_TH2_VERIFIER_GRPC_HOST}
     *  {@link Th2Configuration#ENV_TH2_VERIFIER_GRPC_PORT}
     *  {@link Th2Configuration#ENV_TH2_EVENT_STORAGE_GRPC_HOST}
     *  {@link Th2Configuration#ENV_TH2_EVENT_STORAGE_GRPC_PORT}
     */
    public static void main(String[] args) {
        try {
            MicroserviceConfiguration configuration = readConfiguration(args);
            ActHandler actHandler = new ActHandler(configuration);
            ActServer actServer = new ActServer(configuration.getPort(), actHandler);
            addShutdownHook(actHandler, actServer);
            LOGGER.info("Act started on {} port", configuration.getPort());
            actServer.blockUntilShutdown();
        } catch (Throwable e) {
            LOGGER.error("Exit the program, caused by: {}", e.getMessage(), e);
            System.exit(-1);
        }
    }

    private static void addShutdownHook(ActHandler actHandler, ActServer actServer) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                LOGGER.info("Act is terminating");
                actHandler.close();
                actServer.stop();
            } catch (InterruptedException e) {
                LOGGER.error("gRPC server shutdown is interrupted", e);
            } finally {
                LOGGER.info("Act terminated");
            }
        }));
    }

    private static MicroserviceConfiguration readConfiguration(String[] args) {
        MicroserviceConfiguration configuration = args.length > 0
                ? safeLoad(MicroserviceConfiguration::load, MicroserviceConfiguration::new, args[0])
                : new MicroserviceConfiguration();
        LOGGER.info("Loading act with configuration: {}", configuration);
        return configuration;
    }
}
