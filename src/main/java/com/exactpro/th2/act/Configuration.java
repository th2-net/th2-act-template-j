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

import com.exactpro.th2.configuration.MicroserviceConfiguration;
import com.exactpro.th2.configuration.RabbitMQConfiguration;
import com.exactpro.th2.configuration.Th2Configuration;

import java.io.IOException;
import java.io.InputStream;

import static java.lang.System.getenv;
import static org.apache.commons.lang3.math.NumberUtils.toInt;

public class Configuration extends MicroserviceConfiguration {

    //FIXME: Act should resolve queue information from session info which passed by caller (script)
    public static final String ENV_TH2_FIX_CONNECTIVITY_SEND_MQ = "TH2_FIX_CONNECTIVITY_SEND_MQ";

    public static String getEnvTH2FIXConnectivitySendMQ() {
        return getenv(ENV_TH2_FIX_CONNECTIVITY_SEND_MQ);
    }

    //FIXME: Act should resolve queue information from session info which passed by caller (script)
    public static final String ENV_TH2_FIX_CONNECTIVITY_IN_MQ = "TH2_FIX_CONNECTIVITY_IN_MQ";

    public static String getEnvTH2FIXConnectivityInMQ() {
        return getenv(ENV_TH2_FIX_CONNECTIVITY_IN_MQ);
    }

    public static Configuration load(InputStream inputStream) throws IOException {
        return YAML_READER.readValue(inputStream, Configuration.class);
    }

    private RabbitMQConfiguration rabbitMQ = new RabbitMQConfiguration();
    private Th2Configuration th2 = new Th2Configuration();
    private String th2FIXConnectivitySendMQ = getEnvTH2FIXConnectivitySendMQ();
    private String th2FIXConnectivityInMQ = getEnvTH2FIXConnectivityInMQ();

    public Th2Configuration getTh2() {
        return th2;
    }

    public void setTh2(Th2Configuration th2) {
        this.th2 = th2;
    }

    public RabbitMQConfiguration getRabbitMQ() {
        return rabbitMQ;
    }

    public void setRabbitMQ(RabbitMQConfiguration rabbitMQ) {
        this.rabbitMQ = rabbitMQ;
    }

    public String getTh2FIXConnectivitySendMQ() {
        return th2FIXConnectivitySendMQ;
    }

    public void setTh2FIXConnectivitySendMQ(String th2FIXConnectivitySendMQ) {
        this.th2FIXConnectivitySendMQ = th2FIXConnectivitySendMQ;
    }

    public String getTh2FIXConnectivityInMQ() {
        return th2FIXConnectivityInMQ;
    }

    public void setTh2FIXConnectivityInMQ(String th22FIXConnectivityInMQ) {
        this.th2FIXConnectivityInMQ = th22FIXConnectivityInMQ;
    }
}
