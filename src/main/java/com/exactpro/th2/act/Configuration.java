/*
 * Copyright 2025 Exactpro (Exactpro Systems Limited)
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

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;

@SuppressWarnings("unused")
public class Configuration {

    @JsonProperty("check1Enabled")
    @JsonAlias({"check1Enabled", "check1-enabled"})
    private boolean check1Enabled = true;

    @JsonProperty("responseTimeout")
    @JsonAlias({"responseTimeout", "response-timeout"})
    private int responseTimeout = 10_000;

    public boolean isCheck1Enabled() {
        return check1Enabled;
    }

    public int getResponseTimeout() {
        return responseTimeout;
    }
}
