/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isEmpty;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponseMapper {
    private static final Logger LOGGER = LoggerFactory.getLogger(ResponseMapper.class);
    private final ResponseStatus status;
    private final String responseType;
    private final Map<FieldPath, ValueMatcher> matchingFields;

    ResponseMapper(ResponseStatus status, String responseType, Map<FieldPath, ValueMatcher> matchingFields) {
        rejectBlankValues(status, responseType, matchingFields);
        this.status = status;
        this.responseType = responseType;
        this.matchingFields = matchingFields;
    }

    private static void rejectBlankValues(ResponseStatus status, String responseType, Map<FieldPath, ValueMatcher> matchingFields) {
        if (isEmpty(responseType) || status == null || matchingFields.entrySet().isEmpty()) {
            throw new IllegalArgumentException(format(
                    "'ResponseMapping' must not contain blank values. MsgType: '%s'.", responseType)
            );
        }
    }

    public ResponseStatus getStatus() {
        return status;
    }

    public String getResponseType() {
        return responseType;
    }

    public Map<FieldPath, ValueMatcher> getMatchingFields() {
        return matchingFields;
    }

    public enum ResponseStatus {
        PASSED, FAILED
    }

    public static class FieldPath {
        private final List<String> path;

        FieldPath(String... path) {
            rejectBlankValues(path);
            this.path = Arrays.asList(path);
        }

        private static void rejectBlankValues(String... path) {
            if (path.length == 0) {
                throw new IllegalArgumentException(format(
                        "'FieldPath' must not contain blank values. MsgType: '%s'.", StringUtils.join(path, '/'))
                );
            }
        }

        public List<String> getPath() {
            return path;
        }
    }

    public static class ValueMatcher {
        private final MatchingType matchingType;
        private String exactValue;
        private List<String> valueInList;
        private Double numberValue;

        private ValueMatcher(MatchingType matchingType) {
            this.matchingType = matchingType;
        }

        public static ValueMatcher equal(String value) {
            ValueMatcher matcher = new ValueMatcher(MatchingType.EQUAL);
            matcher.exactValue = value;
            return matcher;
        }

        public static ValueMatcher notEqual(String value) {
            ValueMatcher matcher = new ValueMatcher(MatchingType.NOT_EQUAL);
            matcher.exactValue = value;
            return matcher;
        }

        public static ValueMatcher in(String... values) {
            ValueMatcher matcher = new ValueMatcher(MatchingType.IN);
            matcher.valueInList = Arrays.asList(values);
            return matcher;
        }

        public static ValueMatcher notIn(String... values) {
            ValueMatcher matcher = new ValueMatcher(MatchingType.NOT_IN);
            matcher.valueInList = Arrays.asList(values);
            return matcher;
        }

        public static ValueMatcher more(String value) {

            ValueMatcher matcher = new ValueMatcher(MatchingType.MORE);
            matcher.numberValue = parseStringAsDouble(value);
            return matcher;
        }

        public static ValueMatcher less(String value) {
            ValueMatcher matcher = new ValueMatcher(MatchingType.LESS);
            matcher.numberValue = parseStringAsDouble(value);
            return matcher;
        }

        private static Double parseStringAsDouble(String stringValue) {
            try {
                return Double.valueOf(stringValue);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(format(
                        "Value for 'MORE' or 'LESS' Matchers should contain a parsable double. Can't parse %s.", stringValue), e);
            }
        }

        public boolean isMatches(String actualValue) {
            switch (matchingType) {
            case EQUAL:
                return exactValue.equals(actualValue);
            case NOT_EQUAL:
                return !exactValue.equals(actualValue);
            case IN:
                return valueInList.contains(actualValue);
            case NOT_IN:
                return !valueInList.contains(actualValue);
            case MORE:
            case LESS:
                return compare(actualValue, matchingType);
            default:
                return false;
            }
        }

        public String getMatcherDescription() {
            switch (matchingType) {
            case EQUAL:
                return format("EQUAL TO: %s", exactValue);
            case NOT_EQUAL:
                return format("NOT EQUAL TO: %s", exactValue);
            case IN:
                return format("IN: %s", valueInList);
            case NOT_IN:
                return format("NOT IN: %s", valueInList);
            case MORE:
                return format("MORE THAT: %s", numberValue);
            case LESS:
                return format("LESS THAT: %s", numberValue);
            default:
                return "ERROR";
            }
        }

        private boolean compare(String value, MatchingType operation) {
            try {
                switch (operation) {
                case MORE:
                    return Double.parseDouble(value) > numberValue;
                case LESS:
                    return Double.parseDouble(value) < numberValue;
                default:
                    return false;
                }
            } catch (NumberFormatException e) {
                LOGGER.info(format("Unable to parse %s as Double. '%s than %s' comparison is failed.", value, operation, numberValue), e);
                return false;
            }

        }

        private enum MatchingType {
            EQUAL, NOT_EQUAL, IN, NOT_IN, MORE, LESS
        }
    }
}

