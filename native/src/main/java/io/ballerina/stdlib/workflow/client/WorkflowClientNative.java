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

package io.ballerina.stdlib.workflow.client;

import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.creators.ErrorCreator;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.types.PredefinedTypes;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BDecimal;

import io.ballerina.stdlib.workflow.utils.CorrelationUtils;
import io.ballerina.stdlib.workflow.utils.TypesUtil;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Native implementation for workflow client operations.
 * Provides methods to start workflows, send signals, and query workflow state.
 */
public class WorkflowClientNative {

    /**
     * Initialize workflow client.
     * 
     * @param temporalClient Temporal client handle
     * @return Client handle or error
     */
    public static Object initClient(Object temporalClient) {
        // System.out.println("[JClient] ========== initClient() ENTRY ==========");
        try {
            if (!(temporalClient instanceof WorkflowClient)) {
                // System.err.println("[JClient] Invalid Temporal client handle type: " + temporalClient.getClass().getName());
                return ErrorCreator.createError(
                    StringUtils.fromString("Invalid Temporal client handle"));
            }
            
            // System.out.println("[JClient] Temporal client validated successfully");
            // System.out.println("[JClient] ========== initClient() EXIT [SUCCESS] ==========");
            // Return the temporal client as-is since we'll use it directly
            return temporalClient;
        } catch (Exception e) {
            // System.err.println("[JClient] ========== initClient() EXIT [ERROR] ==========");
            // System.err.println("[JClient] Error: " + e.getMessage());
            return ErrorCreator.createError(
                StringUtils.fromString("Failed to initialize workflow client: " + e.getMessage()));
        }
    }

    /**
     * Start a workflow with correlation data.
     * 
     * @param clientHandle Client handle from initClient
     * @param workflowType Workflow type name (service name)
     * @param params Workflow start parameters (BMap containing correlationData, workflowArgs, executionTimeout)
     * @return Workflow ID or error
     */
    public static Object startWorkflow(
            Object clientHandle,
            BString workflowType,
            BMap<BString, Object> params) {
        // System.out.println("[JClient] ========== startWorkflow() ENTRY ==========");
        // System.out.println("[JClient] Workflow type: " + workflowType.getValue());
        try {
            if (!(clientHandle instanceof WorkflowClient)) {
                // System.err.println("[JClient] Invalid client handle type");
                return ErrorCreator.createError(
                    StringUtils.fromString("Invalid client handle"));
            }
            
            WorkflowClient client = (WorkflowClient) clientHandle;
            // System.out.println("[JClient] WorkflowClient validated");
            
            // Extract parameters from BMap
            // System.out.println("[JClient] Extracting parameters from BMap...");
            @SuppressWarnings("unchecked")
            BMap<BString, BString> correlationData = 
                (BMap<BString, BString>) params.get(StringUtils.fromString("correlationData"));
            
            @SuppressWarnings("unchecked")
            BArray workflowArgs = (BArray) params.get(StringUtils.fromString("workflowArgs"));
            
            Object timeoutObj = params.get(StringUtils.fromString("executionTimeout"));
            long executionTimeout = timeoutObj instanceof Long ? (Long) timeoutObj : 0L;
            
            // System.out.println("[JClient] Execution timeout: " + executionTimeout + " seconds");
            // System.out.println("[JClient] Workflow args count: " + workflowArgs.size());
            
            // Extract task queue (default to \"default\" if not specified)
            Object taskQueueObj = params.get(StringUtils.fromString("taskQueue"));
            String taskQueue = "default";
            if (taskQueueObj instanceof BString) {
                taskQueue = ((BString) taskQueueObj).getValue();
            }
            
            // System.out.println("[JClient] Starting workflow '" + workflowType.getValue() + "' on task queue: " + taskQueue);
            
            // Convert correlation data to Java Map
            // System.out.println("[JClient] Converting correlation data to Java Map...");
            Map<String, String> correlationMap = new HashMap<>();
            if (correlationData != null) {
                for (BString key : correlationData.getKeys()) {
                    String keyStr = key.getValue();
                    String valueStr = correlationData.get(key).getValue();
                    correlationMap.put(keyStr, valueStr);
                    // System.out.println("[JClient] Correlation - " + keyStr + ": " + valueStr);
                }
            }
            
            // Generate workflow ID from correlation data
            // System.out.println("[JClient] Generating workflow ID from correlation data...");
            String workflowId = CorrelationUtils.generateWorkflowId(
                workflowType.getValue(), 
                correlationMap
            );
            // System.out.println("[JClient] Generated workflow ID: " + workflowId);
            
            // Build workflow options
            // System.out.println("[JClient] Building workflow options...");
            WorkflowOptions.Builder optionsBuilder = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(taskQueue);
            
            if (executionTimeout > 0) {
                // System.out.println("[JClient] Setting workflow execution timeout: " + executionTimeout + " seconds");
                optionsBuilder.setWorkflowExecutionTimeout(Duration.ofSeconds(executionTimeout));
            }
            
            WorkflowOptions options = optionsBuilder.build();
            // System.out.println("[JClient] Workflow options built successfully");
            
            // Create untyped workflow stub
            // System.out.println("[JClient] Creating untyped workflow stub...");
            WorkflowStub workflow = client.newUntypedWorkflowStub(
                workflowType.getValue(), 
                options
            );
            // System.out.println("[JClient] Workflow stub created");
            
            // Convert workflow args to Object array with proper type conversion
            // System.out.println("[JClient] Converting " + workflowArgs.size() + " workflow arguments...");
            Object[] args = new Object[(int) workflowArgs.size()];
            for (int i = 0; i < workflowArgs.size(); i++) {
                Object arg = workflowArgs.get(i);
                args[i] = convertBallerinaToJava(arg);
                // System.out.println("[JClient] Arg[" + i + "]: " + args[i] + " (type: " + args[i].getClass().getSimpleName() + ")");
            }
            
            // Start workflow asynchronously
            // System.out.println("[JClient] Starting workflow execution...");
            workflow.start(args);
            // System.out.println("[JClient] Workflow started successfully with ID: " + workflowId);
            // System.out.println("[JClient] ========== startWorkflow() EXIT [SUCCESS] ==========");
            
            return StringUtils.fromString(workflowId);
            
        } catch (Exception e) {
            // System.err.println("[JClient] ========== startWorkflow() EXIT [ERROR] ==========");
            // System.err.println("[JClient] Error starting workflow: " + e.getClass().getName() + ": " + e.getMessage());
            return ErrorCreator.createError(
                StringUtils.fromString("Failed to start workflow: " + e.getClass().getName() + ": " + e.getMessage()));
        }
    }

    /**
     * Send signal to workflow using correlation data.
     * 
     * @param clientHandle Client handle
     * @param workflowType Workflow type name (for correlation resolution)
     * @param correlationData Correlation data to identify workflow instance (without workflowType)
     * @param signalName Signal name
     * @param signalData Signal payload data
     * @return null on success, error on failure
     */
    public static Object sendSignal(
            Object clientHandle,
            BString workflowType,
            BMap<BString, BString> correlationData,
            BString signalName,
            BMap<BString, BString> signalData) {
        // System.out.println("[JClient] ========== sendSignal() ENTRY ==========");
        // System.out.println("[JClient] Workflow type: " + workflowType.getValue());
        // System.out.println("[JClient] Signal name: " + signalName.getValue());
        try {
            if (!(clientHandle instanceof WorkflowClient)) {
                // System.err.println("[JClient] Invalid client handle");
                return ErrorCreator.createError(
                    StringUtils.fromString("Invalid client handle"));
            }
            
            WorkflowClient client = (WorkflowClient) clientHandle;
            // System.out.println("[JClient] WorkflowClient validated");
            
            // Convert correlation data to Java Map
            // System.out.println("[JClient] Converting correlation data...");
            Map<String, String> correlationMap = new HashMap<>();
            // Add workflowType to correlation data for resolution
            correlationMap.put("workflowType", workflowType.getValue());
            if (correlationData != null) {
                for (BString key : correlationData.getKeys()) {
                    String keyStr = key.getValue();
                    String valueStr = correlationData.get(key).getValue();
                    correlationMap.put(keyStr, valueStr);
                    // System.out.println("[JClient] Correlation - " + keyStr + ": " + valueStr);
                }
            }
            
            // Resolve workflow ID from correlation data
            // System.out.println("[JClient] Resolving workflow ID from correlation data...");
            String workflowId = CorrelationUtils.resolveWorkflowId(correlationMap);
            // System.out.println("[JClient] Resolved workflow ID: " + workflowId);
            
            if (workflowId == null) {
                // System.err.println("[JClient] Could not resolve workflow ID from correlation data");
                return ErrorCreator.createError(
                    StringUtils.fromString("Could not resolve workflow ID from correlation data"));
            }
            
            // Get workflow stub
            // System.out.println("[JClient] Creating workflow stub for ID: " + workflowId);
            WorkflowStub workflow = client.newUntypedWorkflowStub(workflowId);
            
            // Convert signal data to Java Map
            // System.out.println("[JClient] Converting signal data...");
            Map<String, String> signalMap = new HashMap<>();
            if (signalData != null) {
                for (BString key : signalData.getKeys()) {
                    String keyStr = key.getValue();
                    String valueStr = signalData.get(key).getValue();
                    signalMap.put(keyStr, valueStr);
                    // System.out.println("[JClient] Signal data - " + keyStr + ": " + valueStr);
                }
            }
            
            // Send signal
            // System.out.println("[JClient] Sending signal '" + signalName.getValue() + "' to workflow...");
            workflow.signal(signalName.getValue(), signalMap);
            // System.out.println("[JClient] Signal sent successfully");
            // System.out.println("[JClient] ========== sendSignal() EXIT [SUCCESS] ==========");
            
            return null;
            
        } catch (Exception e) {
            // System.err.println("[JClient] ========== sendSignal() EXIT [ERROR] ==========");
            // System.err.println("[JClient] Error sending signal: " + e.getMessage());
            return ErrorCreator.createError(
                StringUtils.fromString("Failed to send signal: " + e.getMessage()));
        }
    }

    /**
     * Query workflow state.
     * 
     * @param clientHandle Client handle
     * @param workflowType Workflow type name (for correlation resolution)
     * @param correlationData Correlation data to identify workflow instance (without workflowType)
     * @param queryName Query name
     * @return Query result or error
     */
    public static Object queryWorkflow(
            Object clientHandle,
            BString workflowType,
            BMap<BString, BString> correlationData,
            BString queryName) {
        // System.out.println("[JClient] ========== queryWorkflow() ENTRY ==========");
        // System.out.println("[JClient] Workflow type: " + workflowType.getValue());
        // System.out.println("[JClient] Query name: " + queryName.getValue());
        try {
            if (!(clientHandle instanceof WorkflowClient)) {
                // System.err.println("[JClient] Invalid client handle");
                return ErrorCreator.createError(
                    StringUtils.fromString("Invalid client handle"));
            }
            
            WorkflowClient client = (WorkflowClient) clientHandle;
            // System.out.println("[JClient] WorkflowClient validated");
            
            // Convert correlation data to Java Map
            // System.out.println("[JClient] Converting correlation data...");
            Map<String, String> correlationMap = new HashMap<>();
            // Add workflowType to correlation data for resolution
            correlationMap.put("workflowType", workflowType.getValue());
            if (correlationData != null) {
                for (BString key : correlationData.getKeys()) {
                    String keyStr = key.getValue();
                    String valueStr = correlationData.get(key).getValue();
                    correlationMap.put(keyStr, valueStr);
                    // System.out.println("[JClient] Correlation - " + keyStr + ": " + valueStr);
                }
            }
            
            // Resolve workflow ID from correlation data
            // System.out.println("[JClient] Resolving workflow ID from correlation data...");
            String workflowId = CorrelationUtils.resolveWorkflowId(correlationMap);
            // System.out.println("[JClient] Resolved workflow ID: " + workflowId);
            
            if (workflowId == null) {
                // System.err.println("[JClient] Could not resolve workflow ID from correlation data");
                return ErrorCreator.createError(
                    StringUtils.fromString("Could not resolve workflow ID from correlation data"));
            }
            
            // Get workflow stub
            // System.out.println("[JClient] Creating workflow stub for ID: " + workflowId);
            WorkflowStub workflow = client.newUntypedWorkflowStub(workflowId);
            
            // Execute query
            // System.out.println("[JClient] Executing query '" + queryName.getValue() + "'...");
            Object result = workflow.query(queryName.getValue(), Object.class);
            // System.out.println("[JClient] Query result type: " + (result != null ? result.getClass().getSimpleName() : "null"));
            // System.out.println("[JClient] ========== queryWorkflow() EXIT [SUCCESS] ==========");
            
            // Convert result to appropriate Ballerina type
            if (result == null) {
                return null;
            }
            
            return TypesUtil.convertJavaToBallerinaType(result);
            
        } catch (Exception e) {
            // System.err.println("[JClient] ========== queryWorkflow() EXIT [ERROR] ==========");
            // System.err.println("[JClient] Error querying workflow: " + e.getMessage());
            return ErrorCreator.createError(
                StringUtils.fromString("Failed to query workflow: " + e.getMessage()));
        }
    }

    /**
     * Convert Ballerina value to Java primitive/object for Temporal serialization.
     * This avoids Jackson serialization issues with Ballerina runtime types.
     */
    private static Object convertBallerinaToJava(Object ballerinaValue) {
        if (ballerinaValue instanceof BString) {
            return ((BString) ballerinaValue).getValue();
        } else if (ballerinaValue instanceof Long) {
            return ballerinaValue;
        } else if (ballerinaValue instanceof Double) {
            return ballerinaValue;
        } else if (ballerinaValue instanceof Boolean) {
            return ballerinaValue;
        } else if (ballerinaValue instanceof BDecimal) {
            return ((BDecimal) ballerinaValue).decimalValue();
        } else if (ballerinaValue instanceof BMap) {
            // Convert BMap to Java Map
            @SuppressWarnings("unchecked")
            BMap<BString, Object> bMap = (BMap<BString, Object>) ballerinaValue;
            Map<String, Object> javaMap = new HashMap<>();
            for (BString key : bMap.getKeys()) {
                javaMap.put(key.getValue(), convertBallerinaToJava(bMap.get(key)));
            }
            return javaMap;
        } else if (ballerinaValue instanceof BArray) {
            // Convert BArray to Java array
            BArray bArray = (BArray) ballerinaValue;
            Object[] javaArray = new Object[(int) bArray.size()];
            for (int i = 0; i < bArray.size(); i++) {
                javaArray[i] = convertBallerinaToJava(bArray.get(i));
            }
            return javaArray;
        }
        // Return as-is for other types
        return ballerinaValue;
    }
}
