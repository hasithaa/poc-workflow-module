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

package io.ballerina.stdlib.workflow.context;

import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.creators.ErrorCreator;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BFunctionPointer;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.Runtime;

import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.Workflow;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Native implementation for workflow Context operations.
 */
public class ContextNative {

    /**
     * Wait for a boolean condition to become true.
     * 
     * @param workflowInfo Workflow information handle
     * @param condition Ballerina function pointer for condition
     * @param timeoutSeconds Timeout in seconds
     * @return true if condition met, false if timeout
     */
    public static Object awaitCondition(
            Object workflowInfo,
            BFunctionPointer condition,
            long timeoutSeconds) {
        try {
            // CRITICAL: Must call Workflow.await() directly from this thread
            return WorkflowAwaitWrapper.awaitCondition(
                (int) timeoutSeconds,
                () -> {
                    Object result = condition.call(null);
                    return (Boolean) result;
                }
            );
        } catch (Exception e) {
            return ErrorCreator.createError(
                    StringUtils.fromString("Await condition failed: " + e.getMessage()));
        }
    }

    /**
     * Wait for a specific signal by name.
     * 
     * @param workflowInfo Workflow information handle
     * @param signalName Signal name to wait for
     * @param timeoutSeconds Timeout in seconds
     * @return Map containing signal data, or error if timeout
     */
    public static Object awaitSignal(
            Object workflowInfo,
            BString signalName,
            long timeoutSeconds) {
        try {
            Map<String, String> data = SignalAwaitWrapper.awaitSignal(
                signalName.getValue(),
                (int) timeoutSeconds
            );
            
            if (data != null) {
                // Convert Java Map to Ballerina Map
                @SuppressWarnings("unchecked")
                BMap<BString, Object> ballerinaMap = ValueCreator.createMapValue();
                for (Map.Entry<String, String> entry : data.entrySet()) {
                    ballerinaMap.put(
                        StringUtils.fromString(entry.getKey()),
                        StringUtils.fromString(entry.getValue())
                    );
                }
                return (BMap<BString, BString>) (Object) ballerinaMap;
            } else {
                return ErrorCreator.createError(
                        StringUtils.fromString("Timeout waiting for signal: " + signalName.getValue()));
            }
        } catch (Exception e) {
            return ErrorCreator.createError(
                    StringUtils.fromString("Await signal failed: " + e.getMessage()));
        }
    }

    /**
     * Sleep for specified duration.
     * 
     * @param workflowInfo Workflow information handle
     * @param seconds Duration in seconds
     * @return null on success, error on failure
     */
    public static Object sleep(Object workflowInfo, long seconds) {
        try {
            WorkflowAwaitWrapper.sleep((int) seconds);
            return null;
        } catch (Exception e) {
            return ErrorCreator.createError(
                    StringUtils.fromString("Sleep failed: " + e.getMessage()));
        }
    }

    /**
     * Execute an activity using Temporal's dynamic activity stub.
     * 
     * @param workflowInfo Workflow information handle
     * @param activityName Activity name
     * @param args Activity arguments
     * @return Activity result or error
     */
    public static Object callActivity(
            Object workflowInfo,
            BString activityName,
            BArray args) {
        try {
            // Configure activity options with timeout and retry
            ActivityOptions options = ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(30))
                    .setScheduleToCloseTimeout(Duration.ofSeconds(60))
                    .setRetryOptions(io.temporal.common.RetryOptions.newBuilder()
                        .setMaximumAttempts(3)
                        .setInitialInterval(Duration.ofSeconds(1))
                        .setMaximumInterval(Duration.ofSeconds(10))
                        .setBackoffCoefficient(2.0)
                        .build())
                    .build();
            
            // Convert Ballerina array to Java Object array
            // Handle conversion from Ballerina types to Java types
            Object[] javaArgs = new Object[Math.toIntExact(args.size())];
            for (int i = 0; i < args.size(); i++) {
                Object arg = args.get(i);
                // Convert Ballerina types to Java types for Temporal serialization
                if (arg instanceof BString) {
                    javaArgs[i] = ((BString) arg).getValue();
                } else if (arg instanceof BMap) {
                    // For maps, convert to HashMap
                    BMap<?, ?> bMap = (BMap<?, ?>) arg;
                    Map<String, Object> javaMap = new HashMap<>();
                    for (Object key : bMap.getKeys()) {
                        Object value = bMap.get(key);
                        String keyStr = key instanceof BString ? ((BString) key).getValue() : key.toString();
                        Object valueObj = value instanceof BString ? ((BString) value).getValue() : value;
                        javaMap.put(keyStr, valueObj);
                    }
                    javaArgs[i] = javaMap;
                } else {
                    // Pass through numeric types, booleans, etc.
                    javaArgs[i] = arg;
                }
            }
            
            // Use Temporal's untyped activity stub to execute the activity dynamically
            // This allows calling activities by name without predefined interfaces
            Object result = Workflow.newUntypedActivityStub(options)
                .execute(activityName.getValue(), Object.class, javaArgs);
            
            // Convert result back to Ballerina type if needed
            if (result instanceof String) {
                return StringUtils.fromString((String) result);
            } else if (result instanceof Map) {
                // Convert Java Map to Ballerina Map
                @SuppressWarnings("unchecked")
                Map<String, Object> javaMap = (Map<String, Object>) result;
                BMap<BString, Object> ballerinaMap = ValueCreator.createMapValue();
                for (Map.Entry<String, Object> entry : javaMap.entrySet()) {
                    Object value = entry.getValue();
                    Object ballerinaValue = value instanceof String ? 
                        StringUtils.fromString((String) value) : value;
                    ballerinaMap.put(StringUtils.fromString(entry.getKey()), ballerinaValue);
                }
                return ballerinaMap;
            }
            
            return result;
        } catch (Exception e) {
            return ErrorCreator.createError(
                    StringUtils.fromString("Activity execution failed: " + e.getMessage()));
        }
    }

    /**
     * Record a signal (for signal handler methods).
     * 
     * @param signalName Signal name
     * @param signalData Signal data as Ballerina map
     * @return null
     */
    public static Object recordSignal(BString signalName, BMap<BString, BString> signalData) {
        Map<String, String> javaMap = new HashMap<>();
        
        if (signalData != null) {
            for (BString key : signalData.getKeys()) {
                javaMap.put(key.getValue(), signalData.get(key).getValue());
            }
        }
        
        SignalAwaitWrapper.recordSignal(signalName.getValue(), javaMap);
        return null;
    }
}
