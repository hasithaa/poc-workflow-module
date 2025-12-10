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
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.Runtime;

import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.ActivityStub;
import io.temporal.workflow.Workflow;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

/**
 * Native implementation for workflow context operations.
 * Provides workflow-specific operations like activity execution, signals, and sleep.
 */
public class WorkflowContextNative {

    /**
     * Context information holder.
     */
    private static class ContextInfo {
        String workflowId;
        String workflowType;
        Map<String, String> correlationData;
        ActivityStub activityStub;
        
        ContextInfo() {
            this.correlationData = new HashMap<>();
            
            // Create untyped activity stub for executing activities
            ActivityOptions activityOptions = ActivityOptions.newBuilder()
                .setStartToCloseTimeout(Duration.ofMinutes(2))
                .setRetryOptions(io.temporal.common.RetryOptions.newBuilder()
                    .setMaximumAttempts(3)
                    .build())
                .build();
            
            this.activityStub = Workflow.newUntypedActivityStub(activityOptions);
        }
    }

    /**
     * Execute an activity with replay protection.
     * 
     * @param contextHandle Context handle (ContextInfo)
     * @param activityName Activity function name
     * @param args Activity arguments (BArray)
     * @return Activity result or error
     */
    public static Object executeActivity(
            Object contextHandle,
            BString activityName,
            BArray args) {
        System.out.println("[DEBUG] ContextNative.executeActivity() called for: " + activityName.getValue() +
            " with " + args.size() + " args");
        try {
            if (!(contextHandle instanceof ContextInfo)) {
                return ErrorCreator.createError(
                    StringUtils.fromString("Invalid context handle"));
            }
            
            ContextInfo context = (ContextInfo) contextHandle;
            
            // Convert Ballerina array to Java Object array
            // CRITICAL: Must convert Ballerina types to plain Java types for Temporal serialization
            Object[] javaArgs = new Object[(int) args.size()];
            for (int i = 0; i < args.size(); i++) {
                Object arg = args.get(i);
                // Convert Ballerina types to Java types
                if (arg instanceof BString) {
                    javaArgs[i] = ((BString) arg).getValue();
                } else if (arg instanceof BMap) {
                    // Convert BMap to HashMap
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
            
            // Execute activity through Temporal
            Object result = context.activityStub.execute(
                activityName.getValue(),
                Object.class,
                javaArgs
            );
            
            // Convert result back to Ballerina types if needed
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
     * Wait for a signal by name.
     * 
     * @param contextHandle Context handle
     * @param signalName Signal name to wait for
     * @param timeoutSeconds Timeout in seconds
     * @return Signal data or error on timeout
     */
    public static Object awaitSignal(
            Object contextHandle,
            BString signalName,
            long timeoutSeconds) {
        try {
            // Use the SignalAwaitWrapper for signal handling
            Map<String, String> signalData = SignalAwaitWrapper.awaitSignal(
                signalName.getValue(),
                (int) timeoutSeconds
            );
            
            if (signalData != null) {
                // Convert Java Map to Ballerina Map - must use Object as value type then cast
                @SuppressWarnings("unchecked")
                BMap<BString, Object> ballerinaMap = ValueCreator.createMapValue();
                for (Map.Entry<String, String> entry : signalData.entrySet()) {
                    ballerinaMap.put(
                        StringUtils.fromString(entry.getKey()),
                        StringUtils.fromString(entry.getValue())
                    );
                }
                // Safe cast: we know all values are BString
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
     * Wait for a boolean condition to become true.
     * 
     * @param contextHandle Context handle
     * @param timeoutSeconds Timeout in seconds
     * @param condition Ballerina function pointer for condition
     * @param runtime Ballerina runtime instance (automatically provided by Ballerina)
     * @return true if condition met, false if timeout, error on failure
     */
    public static Object awaitCondition(
            Object contextHandle,
            long timeoutSeconds,
            BFunctionPointer condition) {
        try {
            // Use WorkflowAwaitWrapper for condition handling
            boolean result = WorkflowAwaitWrapper.awaitCondition(
                (int) timeoutSeconds,
                () -> {
                    Object conditionResult = condition.call(null);
                    return (Boolean) conditionResult;
                }
            );
            
            return result;
            
        } catch (Exception e) {
            return ErrorCreator.createError(
                StringUtils.fromString("Await condition failed: " + e.getMessage()));
        }
    }

    /**
     * Wait for any of multiple signals.
     * 
     * @param contextHandle Context handle
     * @param signalNames Array of signal names (BArray)
     * @param timeoutSeconds Timeout in seconds
     * @return SignalResult (BMap with signalName and data) or error on timeout
     */
    public static Object awaitAnySignal(
            Object contextHandle,
            BArray signalNames,
            long timeoutSeconds) {
        try {
            // Convert BArray to Java String array
            List<String> signalNamesList = new ArrayList<>();
            for (int i = 0; i < signalNames.size(); i++) {
                Object item = signalNames.get(i);
                if (item instanceof BString) {
                    signalNamesList.add(((BString) item).getValue());
                } else if (item instanceof String) {
                    signalNamesList.add((String) item);
                }
            }
            
            // Wait for any signal using SignalAwaitWrapper
            String[] signalNamesArray = signalNamesList.toArray(new String[0]);
            
            // Note: This requires enhanced SignalAwaitWrapper to support multiple signals
            // For now, we'll wait for the first signal name as a fallback
            if (signalNamesArray.length == 0) {
                return ErrorCreator.createError(
                    StringUtils.fromString("No signal names provided"));
            }
            
            // Wait for first signal (simplified implementation)
            Map<String, String> signalData = SignalAwaitWrapper.awaitSignal(
                signalNamesArray[0],
                (int) timeoutSeconds
            );
            
            if (signalData != null) {
                // Create SignalResult record
                @SuppressWarnings("unchecked")
                BMap<BString, Object> result = ValueCreator.createMapValue();
                result.put(
                    StringUtils.fromString("signalName"),
                    StringUtils.fromString(signalNamesArray[0])
                );
                
                // Convert signal data to Ballerina map - must use Object as value type then cast
                @SuppressWarnings("unchecked")
                BMap<BString, Object> dataMap = ValueCreator.createMapValue();
                for (Map.Entry<String, String> entry : signalData.entrySet()) {
                    dataMap.put(
                        StringUtils.fromString(entry.getKey()),
                        StringUtils.fromString(entry.getValue())
                    );
                }
                // Cast to the correct type for the record field
                result.put(StringUtils.fromString("data"), (BMap<BString, BString>) (Object) dataMap);
                
                return result;
            } else {
                return ErrorCreator.createError(
                    StringUtils.fromString("Timeout waiting for any signal"));
            }
            
        } catch (Exception e) {
            return ErrorCreator.createError(
                StringUtils.fromString("Await any signal failed: " + e.getMessage()));
        }
    }

    /**
     * Durable sleep - suspends workflow.
     * 
     * @param contextHandle Context handle
     * @param seconds Sleep duration in seconds
     * @return null on success, error on failure
     */
    public static Object sleep(Object contextHandle, long seconds) {
        try {
            // Use Temporal's Workflow.sleep for durable sleep
            Workflow.sleep(Duration.ofSeconds(seconds));
            return null;
            
        } catch (Exception e) {
            return ErrorCreator.createError(
                StringUtils.fromString("Sleep failed: " + e.getMessage()));
        }
    }

    /**
     * Get correlation ID for this workflow instance.
     * 
     * @param contextHandle Context handle
     * @return Correlation ID string
     */
    public static BString getCorrelationId(Object contextHandle) {
        try {
            if (contextHandle instanceof ContextInfo) {
                ContextInfo context = (ContextInfo) contextHandle;
                if (context.workflowId != null) {
                    return StringUtils.fromString(context.workflowId);
                }
            }
            
            // Fallback: get from Temporal workflow info
            String workflowId = Workflow.getInfo().getWorkflowId();
            return StringUtils.fromString(workflowId);
            
        } catch (Exception e) {
            return StringUtils.fromString("unknown");
        }
    }

    /**
     * Check if workflow is currently replaying.
     * 
     * @param contextHandle Context handle
     * @return true if replaying, false otherwise
     */
    public static boolean isReplaying(Object contextHandle) {
        try {
            return Workflow.isReplaying();
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Create a context info object for a workflow execution.
     * This is typically called by the worker when starting a workflow.
     * 
     * @param workflowId Workflow ID
     * @param workflowType Workflow type name
     * @param correlationData Correlation data
     * @return ContextInfo handle
     */
    public static Object createContext(
            String workflowId,
            String workflowType,
            Map<String, String> correlationData) {
        ContextInfo context = new ContextInfo();
        context.workflowId = workflowId;
        context.workflowType = workflowType;
        if (correlationData != null) {
            context.correlationData.putAll(correlationData);
        }
        return context;
    }

    /**
     * Record a signal for later retrieval by awaitSignal.
     * This is called by signal handlers.
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
