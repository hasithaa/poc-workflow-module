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
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BDecimal;

import io.ballerina.stdlib.workflow.utils.CorrelationUtils;

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
        try {
            if (!(temporalClient instanceof WorkflowClient)) {
                return ErrorCreator.createError(
                    StringUtils.fromString("Invalid Temporal client handle"));
            }
            
            // Return the temporal client as-is since we'll use it directly
            return temporalClient;
        } catch (Exception e) {
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
        try {
            if (!(clientHandle instanceof WorkflowClient)) {
                return ErrorCreator.createError(
                    StringUtils.fromString("Invalid client handle"));
            }
            
            WorkflowClient client = (WorkflowClient) clientHandle;
            
            // Extract parameters from BMap
            @SuppressWarnings("unchecked")
            BMap<BString, BString> correlationData = 
                (BMap<BString, BString>) params.get(StringUtils.fromString("correlationData"));
            
            @SuppressWarnings("unchecked")
            BArray workflowArgs = (BArray) params.get(StringUtils.fromString("workflowArgs"));
            
            Object timeoutObj = params.get(StringUtils.fromString("executionTimeout"));
            long executionTimeout = timeoutObj instanceof Long ? (Long) timeoutObj : 0L;
            
            // Extract task queue (default to "default" if not specified)
            Object taskQueueObj = params.get(StringUtils.fromString("taskQueue"));
            String taskQueue = "default";
            if (taskQueueObj instanceof BString) {
                taskQueue = ((BString) taskQueueObj).getValue();
            }
            
            System.out.println("[DEBUG] Starting workflow '" + workflowType.getValue() + "' on task queue: " + taskQueue);
            
            // Convert correlation data to Java Map
            Map<String, String> correlationMap = new HashMap<>();
            if (correlationData != null) {
                for (BString key : correlationData.getKeys()) {
                    correlationMap.put(key.getValue(), correlationData.get(key).getValue());
                }
            }
            
            // Generate workflow ID from correlation data
            String workflowId = CorrelationUtils.generateWorkflowId(
                workflowType.getValue(), 
                correlationMap
            );
            
            // Build workflow options
            WorkflowOptions.Builder optionsBuilder = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(taskQueue);
            
            if (executionTimeout > 0) {
                optionsBuilder.setWorkflowExecutionTimeout(Duration.ofSeconds(executionTimeout));
            }
            
            WorkflowOptions options = optionsBuilder.build();
            
            // Create untyped workflow stub
            WorkflowStub workflow = client.newUntypedWorkflowStub(
                workflowType.getValue(), 
                options
            );
            
            // Convert workflow args to Object array with proper type conversion
            Object[] args = new Object[(int) workflowArgs.size()];
            for (int i = 0; i < workflowArgs.size(); i++) {
                args[i] = convertBallerinaToJava(workflowArgs.get(i));
            }
            
            // Start workflow asynchronously
            workflow.start(args);
            
            return StringUtils.fromString(workflowId);
            
        } catch (Exception e) {
            e.printStackTrace(); // Log full stack trace for debugging
            return ErrorCreator.createError(
                StringUtils.fromString("Failed to start workflow: " + e.getClass().getName() + ": " + e.getMessage()));
        }
    }

    /**
     * Send signal to workflow using correlation data.
     * 
     * @param clientHandle Client handle
     * @param correlationData Correlation data to identify workflow instance
     * @param signalName Signal name
     * @param signalData Signal payload data
     * @return null on success, error on failure
     */
    public static Object sendSignal(
            Object clientHandle,
            BMap<BString, BString> correlationData,
            BString signalName,
            BMap<BString, BString> signalData) {
        try {
            if (!(clientHandle instanceof WorkflowClient)) {
                return ErrorCreator.createError(
                    StringUtils.fromString("Invalid client handle"));
            }
            
            WorkflowClient client = (WorkflowClient) clientHandle;
            
            // Convert correlation data to Java Map
            Map<String, String> correlationMap = new HashMap<>();
            if (correlationData != null) {
                for (BString key : correlationData.getKeys()) {
                    correlationMap.put(key.getValue(), correlationData.get(key).getValue());
                }
            }
            
            // Resolve workflow ID from correlation data
            String workflowId = CorrelationUtils.resolveWorkflowId(correlationMap);
            
            if (workflowId == null) {
                return ErrorCreator.createError(
                    StringUtils.fromString("Could not resolve workflow ID from correlation data"));
            }
            
            // Get workflow stub
            WorkflowStub workflow = client.newUntypedWorkflowStub(workflowId);
            
            // Convert signal data to Java Map
            Map<String, String> signalMap = new HashMap<>();
            if (signalData != null) {
                for (BString key : signalData.getKeys()) {
                    signalMap.put(key.getValue(), signalData.get(key).getValue());
                }
            }
            
            // Send signal
            workflow.signal(signalName.getValue(), signalMap);
            
            return null;
            
        } catch (Exception e) {
            return ErrorCreator.createError(
                StringUtils.fromString("Failed to send signal: " + e.getMessage()));
        }
    }

    /**
     * Query workflow state.
     * 
     * @param clientHandle Client handle
     * @param correlationData Correlation data to identify workflow instance
     * @param queryName Query name
     * @return Query result or error
     */
    public static Object queryWorkflow(
            Object clientHandle,
            BMap<BString, BString> correlationData,
            BString queryName) {
        try {
            if (!(clientHandle instanceof WorkflowClient)) {
                return ErrorCreator.createError(
                    StringUtils.fromString("Invalid client handle"));
            }
            
            WorkflowClient client = (WorkflowClient) clientHandle;
            
            // Convert correlation data to Java Map
            Map<String, String> correlationMap = new HashMap<>();
            if (correlationData != null) {
                for (BString key : correlationData.getKeys()) {
                    correlationMap.put(key.getValue(), correlationData.get(key).getValue());
                }
            }
            
            // Resolve workflow ID from correlation data
            String workflowId = CorrelationUtils.resolveWorkflowId(correlationMap);
            
            if (workflowId == null) {
                return ErrorCreator.createError(
                    StringUtils.fromString("Could not resolve workflow ID from correlation data"));
            }
            
            // Get workflow stub
            WorkflowStub workflow = client.newUntypedWorkflowStub(workflowId);
            
            // Execute query
            Object result = workflow.query(queryName.getValue(), Object.class);
            
            // Convert result to appropriate Ballerina type
            // For now, return as-is; may need type conversion based on result type
            return result;
            
        } catch (Exception e) {
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
