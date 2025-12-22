/*
 *  Copyright (c) 2025, WSO2 LLC. (https://www.wso2.com).
 *
 *  WSO2 LLC. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package io.ballerina.stdlib.workflow.utils;

import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;

import java.util.Map;

public class TypesUtil {

    public static Object convertJavaToBallerinaType(Object javaValue) {
        if (javaValue == null) {
            return null;
        }
        if (javaValue instanceof String) {
            return StringUtils.fromString((String) javaValue);
        } else if (javaValue instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> javaMap = (Map<String, Object>) javaValue;
            BMap<BString, Object> ballerinaMap = ValueCreator.createMapValue(
                io.ballerina.runtime.api.creators.TypeCreator.createMapType(
                    io.ballerina.runtime.api.types.PredefinedTypes.TYPE_ANYDATA));
            for (Map.Entry<String, Object> entry : javaMap.entrySet()) {
                ballerinaMap.put(
                        StringUtils.fromString(entry.getKey()),
                        convertJavaToBallerinaType(entry.getValue())
                                );
            }
            return ballerinaMap;
        } else {
            // Primitives, numbers, booleans pass through
            return javaValue;
        }
    }
}
