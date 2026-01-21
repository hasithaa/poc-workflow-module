/*
 * Copyright (c) 2025, WSO2 LLC. (http://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.stdlib.workflow.v0.utils;

import io.ballerina.runtime.api.values.BFunctionPointer;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.creators.ErrorCreator;

/**
 * Utility functions for V0 workflow module.
 */
public class FunctionUtils {

    /**
     * Extract function name from Ballerina function reference.
     * This is used to get the activity name from a function reference.
     *
     * @param func Ballerina function object
     * @return Function name as BString
     */
    public static BString extractFunctionName(BFunctionPointer func) {
        try {
            if (func == null) {
                return StringUtils.fromString("unknown");
            }
            
            // Get the function name from the type
            String typeName = func.getType().getName();
            if (typeName != null && !typeName.isEmpty()) {
                return StringUtils.fromString(typeName);
            }
            
            // Fallback to toString parsing if type name is not available
            String fullName = func.toString();
            if (fullName.contains("::")) {
                String[] parts = fullName.split("::");
                return StringUtils.fromString(parts[parts.length - 1]);
            }
            
            return StringUtils.fromString(fullName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to extract function name: " + e.getMessage(), e);
        }
    }
}
