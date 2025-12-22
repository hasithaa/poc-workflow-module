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

import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.creators.ErrorCreator;
import io.ballerina.runtime.api.types.PredefinedTypes;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BFunctionPointer;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.Runtime;

import io.ballerina.stdlib.workflow.utils.TypesUtil;
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
                .setStartToCloseTimeout(Duration.ofMinutes(10))  // Max time for activity execution
                .setScheduleToStartTimeout(Duration.ofMinutes(1))  // Max time waiting in queue
                .setScheduleToCloseTimeout(Duration.ofMinutes(11))  // Total timeout including queue time
                .setRetryOptions(io.temporal.common.RetryOptions.newBuilder()
                    .setMaximumAttempts(1)
                    .setDoNotRetry(RuntimeException.class.getName())  // Don't retry on runtime exceptions
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
        System.out.println("[JContext] ========== executeActivity() ENTRY ==========");
        System.out.println("[JContext] Activity name: " + activityName.getValue());
        System.out.println("[JContext] ContextNative.executeActivity() called for: " + activityName.getValue() +
            " with " + args.size() + " args");
        try {
            if (!(contextHandle instanceof ContextInfo)) {
                System.err.println("[JContext] Invalid context handle type: " + contextHandle.getClass().getName());
                return ErrorCreator.createError(
                    StringUtils.fromString("Invalid context handle"));
            }
            
            ContextInfo context = (ContextInfo) contextHandle;
            System.out.println("[JContext] Context validated, workflow ID: " + context.workflowId);
            
            // Convert Ballerina array to Java Object array
            // CRITICAL: Must convert Ballerina types to plain Java types for Temporal serialization
            System.out.println("[JContext] Converting " + args.size() + " arguments from Ballerina to Java types...");
            Object[] javaArgs = new Object[(int) args.size()];
            for (int i = 0; i < args.size(); i++) {
                Object arg = args.get(i);
                System.out.println("[JContext] Processing arg[" + i + "] of type: " + arg.getClass().getSimpleName());
                // Convert Ballerina types to Java types
                if (arg instanceof BString) {
                    javaArgs[i] = ((BString) arg).getValue();
                    System.out.println("[JContext] Converted BString to String: " + javaArgs[i]);
                } else if (arg instanceof BMap) {
                    System.out.println("[JContext] Converting BMap to HashMap...");
                    // Convert BMap to HashMap
                    BMap<?, ?> bMap = (BMap<?, ?>) arg;
                    Map<String, Object> javaMap = new HashMap<>();
                    for (Object key : bMap.getKeys()) {
                        Object value = bMap.get(key);
                        String keyStr = key instanceof BString ? ((BString) key).getValue() : key.toString();
                        Object valueObj = value instanceof BString ? ((BString) value).getValue() : value;
                        javaMap.put(keyStr, valueObj);
                        System.out.println("[JContext] Map entry - " + keyStr + ": " + valueObj);
                    }
                    javaArgs[i] = javaMap;
                } else {
                    // Pass through numeric types, booleans, etc.
                    javaArgs[i] = arg;
                    System.out.println("[JContext] Pass-through type: " + arg);
                }
            }
            
            // Execute activity through Temporal
            // NOTE: Don't catch Temporal exceptions - let them propagate to fail the workflow
            System.out.println("[JContext] Executing activity '" + activityName.getValue() + "' through Temporal...");
            Object result = context.activityStub.execute(
                activityName.getValue(),
                Object.class,
                javaArgs
            );
            System.out.println("[JContext] Activity execution completed, result type: " + 
                (result != null ? result.getClass().getSimpleName() : "null"));
            
            // Convert result back to Ballerina types if needed
            System.out.println("[JContext] Converting result back to Ballerina types...");
            
            // Use centralized conversion method for all return types
            Object ballerinaResult = convertJavaToBallerinaType(result);
            
            System.out.println("[JContext] ========== executeActivity() EXIT [SUCCESS] ==========");
            System.out.println("[JContext] Converted result type: " + 
                (ballerinaResult != null ? ballerinaResult.getClass().getSimpleName() : "null"));
            return ballerinaResult;
            
        } catch (io.temporal.failure.ActivityFailure e) {
            // Activity failed - extract error message and return as Ballerina error
            // The Ballerina 'check' operator will fail the workflow
            System.err.println("[JContext] ========== executeActivity() EXIT [ACTIVITY FAILURE] ==========");
            String errorMessage = extractActivityErrorMessage(e);
            System.err.println("[JContext] Activity '" + activityName.getValue() + "' failed: " + errorMessage);
            return ErrorCreator.createError(
                StringUtils.fromString(errorMessage));
                
        } catch (Exception e) {
            // Other errors - wrap as Ballerina error for non-Temporal exceptions
            System.err.println("[JContext] ========== executeActivity() EXIT [ERROR] ==========");
            System.err.println("[JContext] Unexpected error during activity execution: " + e.getMessage());
            return ErrorCreator.createError(
                StringUtils.fromString("Activity execution error: " + e.getMessage()));
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
        System.out.println("[JContext] ========== awaitSignal() ENTRY ==========");
        System.out.println("[JContext] Signal name: " + signalName.getValue());
        System.out.println("[JContext] Timeout: " + timeoutSeconds + " seconds");
        try {
            // Use the SignalAwaitWrapper for signal handling
            System.out.println("[JContext] Calling SignalAwaitWrapper.awaitSignal()...");
            Map<String, Object> signalData = SignalAwaitWrapper.awaitSignal(
                signalName.getValue(),
                (int) timeoutSeconds
            );
            
            if (signalData != null) {
                System.out.println("[JContext] Signal received with " + signalData.size() + " data entries");
                // Convert Java Map to Ballerina Map with correct type (map<anydata>)
                @SuppressWarnings("unchecked")
                BMap<BString, Object> ballerinaMap = ValueCreator.createMapValue(
                    TypeCreator.createMapType(PredefinedTypes.TYPE_ANYDATA));
                for (Map.Entry<String, Object> entry : signalData.entrySet()) {
                    ballerinaMap.put(
                        StringUtils.fromString(entry.getKey()),
                        TypesUtil.convertJavaToBallerinaType(entry.getValue())
                    );
                    System.out.println("[JContext] Signal data - " + entry.getKey() + ": " + entry.getValue());
                }
                System.out.println("[JContext] ========== awaitSignal() EXIT [SUCCESS] ==========");
                return ballerinaMap;
            } else {
                System.err.println("[JContext] ========== awaitSignal() EXIT [TIMEOUT] ==========");
                System.err.println("[JContext] Timeout waiting for signal: " + signalName.getValue());
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
     * @return true if condition met, false if timeout, error on failure
     */
    public static Object awaitCondition(
            Object contextHandle,
            long timeoutSeconds,
            BFunctionPointer condition) {
        System.out.println("[JContext] ========== awaitCondition() ENTRY ==========");
        System.out.println("[JContext] Timeout: " + timeoutSeconds + " seconds");
        try {
            // Use WorkflowAwaitWrapper for condition handling
            System.out.println("[JContext] Calling WorkflowAwaitWrapper.awaitCondition()...");
            boolean result = WorkflowAwaitWrapper.awaitCondition(
                (int) timeoutSeconds,
                () -> {
                    System.out.println("[JContext] Evaluating condition function...");
                    Object conditionResult = condition.call(null);
                    boolean boolResult = (Boolean) conditionResult;
                    System.out.println("[JContext] Condition evaluated to: " + boolResult);
                    return boolResult;
                }
            );
            
            System.out.println("[JContext] ========== awaitCondition() EXIT [SUCCESS] - Result: " + result + " ==========");
            return result;
            
        } catch (Exception e) {
            System.err.println("[JContext] ========== awaitCondition() EXIT [ERROR] ==========");
            System.err.println("[JContext] Await condition failed: " + e.getMessage());
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
        System.out.println("[JContext] ========== awaitAnySignal() ENTRY ==========");
        System.out.println("[JContext] Waiting for any of " + signalNames.size() + " signals");
        System.out.println("[JContext] Timeout: " + timeoutSeconds + " seconds");
        try {
            // Convert BArray to Java String array
            System.out.println("[JContext] Converting signal names from BArray...");
            List<String> signalNamesList = new ArrayList<>();
            for (int i = 0; i < signalNames.size(); i++) {
                Object item = signalNames.get(i);
                if (item instanceof BString) {
                    String signalName = ((BString) item).getValue();
                    signalNamesList.add(signalName);
                    System.out.println("[JContext] Signal[" + i + "]: " + signalName);
                } else if (item instanceof String) {
                    signalNamesList.add((String) item);
                    System.out.println("[JContext] Signal[" + i + "]: " + item);
                }
            }
            
            // Wait for any signal using SignalAwaitWrapper
            String[] signalNamesArray = signalNamesList.toArray(new String[0]);
            
            if (signalNamesArray.length == 0) {
                System.err.println("[JContext] No signal names provided");
                return ErrorCreator.createError(
                    StringUtils.fromString("No signal names provided"));
            }
            
            // Wait for ANY of the signals using SignalAwaitWrapper.awaitAnySignal()
            System.out.println("[JContext] Waiting for any of these signals: " + String.join(", ", signalNamesArray));
            SignalAwaitWrapper.SignalResult signalResult = SignalAwaitWrapper.awaitAnySignal(
                signalNamesArray,
                (int) timeoutSeconds
            );
            
            if (signalResult != null) {
                try {
                    String receivedSignalName = signalResult.getSignalName();
                    Map<String, Object> signalData = signalResult.getData();
                    System.out.println("[JContext] Signal received: " + receivedSignalName + " with " + signalData.size() + " data entries");
                    
                    // Create the data map with correct type (map<anydata>)
                    System.out.println("[JContext] Converting signal data to BMap...");
                    @SuppressWarnings("unchecked")
                    BMap<BString, Object> dataMap = ValueCreator.createMapValue(
                        TypeCreator.createMapType(PredefinedTypes.TYPE_ANYDATA));
                    
                    for (Map.Entry<String, Object> entry : signalData.entrySet()) {
                        BString key = StringUtils.fromString(entry.getKey());
                        Object value = TypesUtil.convertJavaToBallerinaType(entry.getValue());
                        dataMap.put(key, value);
                        System.out.println("[JContext] Signal data - " + entry.getKey() + ": " + entry.getValue());
                    }
                    
                    System.out.println("[JContext] dataMap created with " + dataMap.size() + " entries");
                    
                    // Create the result map with signal name and data fields
                    // The structure matches SignalResult: record {| string signalName; map<anydata> data; |}
                    System.out.println("[JContext] Creating result map...");
                    @SuppressWarnings("unchecked")
                    BMap<BString, Object> result = ValueCreator.createMapValue(
                        TypeCreator.createMapType(PredefinedTypes.TYPE_ANYDATA));
                    
                    result.put(
                        StringUtils.fromString("signalName"),
                        StringUtils.fromString(receivedSignalName)
                    );
                    result.put(StringUtils.fromString("data"), dataMap);
                    
                    System.out.println("[JContext] SignalResult record complete: signalName=" + receivedSignalName + ", data entries=" + dataMap.size());
                    System.out.println("[JContext] ========== awaitAnySignal() EXIT [SUCCESS] ==========");
                    return result;
                } catch (Exception e) {
                    System.err.println("[JContext] ========================================");
                    System.err.println("[JContext] ERROR in awaitAnySignal result creation");
                    System.err.println("[JContext] Exception: " + e.getClass().getName());
                    System.err.println("[JContext] Message: " + e.getMessage());
                    e.printStackTrace(System.err);
                    System.err.println("[JContext] ========================================");
                    throw e;
                }
            } else {
                System.err.println("[JContext] ========== awaitAnySignal() EXIT [TIMEOUT] ==========");
                System.err.println("[JContext] Timeout waiting for any signal");
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
        System.out.println("[JContext] ========== sleep() ENTRY ==========");
        System.out.println("[JContext] Sleep duration: " + seconds + " seconds");
        try {
            // Use Temporal's Workflow.sleep for durable sleep
            System.out.println("[JContext] Calling Temporal Workflow.sleep()...");
            Workflow.sleep(Duration.ofSeconds(seconds));
            System.out.println("[JContext] ========== sleep() EXIT [SUCCESS] ==========");
            return null;
            
        } catch (Exception e) {
            System.err.println("[JContext] ========== sleep() EXIT [ERROR] ==========");
            System.err.println("[JContext] Sleep failed: " + e.getMessage());
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
        System.out.println("[JContext] ========== getCorrelationId() ENTRY ==========");
        try {
            if (contextHandle instanceof ContextInfo) {
                ContextInfo context = (ContextInfo) contextHandle;
                if (context.workflowId != null) {
                    System.out.println("[JContext] Returning workflow ID from context: " + context.workflowId);
                    System.out.println("[JContext] ========== getCorrelationId() EXIT [SUCCESS] ==========");
                    return StringUtils.fromString(context.workflowId);
                }
            }
            
            // Fallback: get from Temporal workflow info
            System.out.println("[JContext] Getting workflow ID from Temporal Workflow.getInfo()...");
            String workflowId = Workflow.getInfo().getWorkflowId();
            System.out.println("[JContext] Workflow ID: " + workflowId);
            System.out.println("[JContext] ========== getCorrelationId() EXIT [SUCCESS] ==========");
            return StringUtils.fromString(workflowId);
            
        } catch (Exception e) {
            System.err.println("[JContext] ========== getCorrelationId() EXIT [ERROR] ==========");
            System.err.println("[JContext] Error getting correlation ID: " + e.getMessage());
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
        System.out.println("[JContext] ========== isReplaying() ENTRY ==========");
        try {
            boolean replaying = Workflow.isReplaying();
            System.out.println("[JContext] Is replaying: " + replaying);
            System.out.println("[JContext] ========== isReplaying() EXIT [SUCCESS] ==========");
            return replaying;
        } catch (Exception e) {
            System.err.println("[JContext] ========== isReplaying() EXIT [ERROR] ==========");
            System.err.println("[JContext] Error checking replay status: " + e.getMessage());
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
        System.out.println("[JContext] ========== createContext() ENTRY ==========");
        System.out.println("[JContext] Workflow ID: " + workflowId);
        System.out.println("[JContext] Workflow type: " + workflowType);
        if (correlationData != null) {
            System.out.println("[JContext] Correlation data entries: " + correlationData.size());
            for (Map.Entry<String, String> entry : correlationData.entrySet()) {
                System.out.println("[JContext] Correlation - " + entry.getKey() + ": " + entry.getValue());
            }
        }
        ContextInfo context = new ContextInfo();
        context.workflowId = workflowId;
        context.workflowType = workflowType;
        if (correlationData != null) {
            context.correlationData.putAll(correlationData);
        }
        System.out.println("[JContext] ========== createContext() EXIT [SUCCESS] ==========");
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
        System.out.println("[JContext] ========== recordSignal() ENTRY ==========");
        System.out.println("[JContext] Signal name: " + signalName.getValue());
        Map<String, Object> javaMap = new HashMap<>();
        
        if (signalData != null) {
            System.out.println("[JContext] Signal data entries: " + signalData.size());
            for (BString key : signalData.getKeys()) {
                String keyStr = key.getValue();
                String valueStr = signalData.get(key).getValue();
                javaMap.put(keyStr, valueStr);
                System.out.println("[JContext] Signal data - " + keyStr + ": " + valueStr);
            }
        }
        
        System.out.println("[JContext] Calling SignalAwaitWrapper.recordSignal()...");
        SignalAwaitWrapper.recordSignal(signalName.getValue(), javaMap);
        System.out.println("[JContext] ========== recordSignal() EXIT [SUCCESS] ==========");
        return null;
    }
    
    /**
     * Convert Java types to Ballerina types for proper error detail reconstruction.
     * Also handles BError reconstruction from serialized error maps.
     * 
     * @param javaValue Java object to convert
     * @return Ballerina-compatible value
     */
    private static Object convertJavaToBallerinaType(Object javaValue) {
        if (javaValue == null) {
            return null;
        }
        
        if (javaValue instanceof String) {
            return StringUtils.fromString((String) javaValue);
        } else if (javaValue instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> javaMap = (Map<String, Object>) javaValue;
            
            // Check if this is an error map (activity returned BError)
            if (javaMap.containsKey("__error__") && Boolean.TRUE.equals(javaMap.get("__error__"))) {
                System.out.println("[JContext] Detected error map, reconstructing BError");
                
                String errorMessage = (String) javaMap.get("message");
                
                // For now, just return error with message only
                // TODO: Properly serialize and deserialize error details
                System.out.println("[JContext] Creating BError with message only: " + errorMessage);
                return ErrorCreator.createError(StringUtils.fromString(errorMessage));
            }
            
            // Regular map conversion
            BMap<BString, Object> ballerinaMap = ValueCreator.createMapValue(TypeCreator.createMapType(PredefinedTypes.TYPE_ANYDATA));
            for (Map.Entry<String, Object> entry : javaMap.entrySet()) {
                ballerinaMap.put(
                    StringUtils.fromString(entry.getKey()),
                    convertJavaToBallerinaType(entry.getValue())
                );
            }
            return ballerinaMap;
        } else if (javaValue instanceof List) {
            @SuppressWarnings("unchecked")
            List<Object> javaList = (List<Object>) javaValue;
            // For lists, convert to Ballerina map since we can't easily determine the array type
            // In most error detail cases, lists are not commonly used
            // If needed, could return as-is and let Ballerina handle it
            return javaList;
        } else if (javaValue instanceof Integer) {
            // Convert Integer to Long for Ballerina compatibility
            return ((Integer) javaValue).longValue();
        } else if (javaValue instanceof Long) {
            return javaValue;
        } else if (javaValue instanceof Double) {
            return javaValue;
        } else if (javaValue instanceof Boolean) {
            return javaValue;
        } else if (javaValue instanceof java.math.BigDecimal) {
            return javaValue;
        } else if (javaValue instanceof Float) {
            // Convert Float to Double for Ballerina compatibility
            return ((Float) javaValue).doubleValue();
        } else {
            // For unknown types, convert to string for safety
            System.out.println("[JContext] Converting unknown Java type to BString: " + javaValue.getClass().getName());
            return StringUtils.fromString(javaValue.toString());
        }
    }
    
    /**
     * Extract a clean error message from Temporal ActivityFailure exception.
     * Looks for the root cause which is typically the Ballerina error message.
     * 
     * @param e ActivityFailure exception
     * @return Clean error message without Java stack traces
     */
    private static String extractActivityErrorMessage(io.temporal.failure.ActivityFailure e) {
        // ActivityFailure wraps ApplicationFailure which contains the actual error
        Throwable cause = e.getCause();
        
        if (cause instanceof io.temporal.failure.ApplicationFailure) {
            io.temporal.failure.ApplicationFailure appFailure = 
                (io.temporal.failure.ApplicationFailure) cause;
            
            // Get the original message - this is the clean Ballerina error
            String message = appFailure.getOriginalMessage();
            return message != null ? message : "Activity failed";
        }
        
        // Fallback - shouldn't reach here with proper error handling
        return "Activity execution failed";
    }
}
