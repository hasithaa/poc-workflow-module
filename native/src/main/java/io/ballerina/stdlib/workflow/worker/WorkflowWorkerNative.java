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

package io.ballerina.stdlib.workflow.worker;

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.Runtime;
import io.ballerina.runtime.api.concurrent.StrandMetadata;
import io.ballerina.runtime.api.creators.ErrorCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BFunctionPointer;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.internal.values.FPValue;
import io.ballerina.stdlib.workflow.context.SignalAwaitWrapper;
import io.temporal.activity.DynamicActivity;
import io.temporal.client.WorkflowClient;
import io.temporal.common.converter.EncodedValues;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import io.temporal.workflow.DynamicWorkflow;
import io.temporal.workflow.Workflow;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.ballerina.stdlib.workflow.utils.TypesUtil.convertJavaToBallerinaType;

/**
 * Native implementation for workflow worker operations. Provides methods to register and manage workflow services.
 */
public class WorkflowWorkerNative {

    // Static registry to store service objects accessible during workflow execution
    private static final Map<String, BObject> SERVICE_REGISTRY = new ConcurrentHashMap<>();

    // Static registry to store activity implementations (activity name -> BFunctionPointer)
    private static final Map<String, BFunctionPointer> ACTIVITY_REGISTRY = new ConcurrentHashMap<>();

    // Store workflow module for creating Context objects
    private static io.ballerina.runtime.api.Module workflowModule;

    // Store Runtime instance for creating Strands
    private static Runtime ballerinaRuntime;

    /**
     * Module initialization - called by Ballerina runtime Captures the Module and Runtime from Environment for later
     * use
     */
    public static void init(Environment env) {
        workflowModule = env.getCurrentModule();
        ballerinaRuntime = env.getRuntime();
        System.out.println("[JWorker] WorkflowWorkerNative initialized with module: " + workflowModule);
        System.out.println("[JWorker] Ballerina Runtime captured: " + ballerinaRuntime);
    }

    /**
     * Initialize workflow worker.
     *
     * @param temporalClient Temporal client handle
     * @param config         Listener configuration (BMap containing taskQueue, maxConcurrentWorkflows, etc.)
     * @return Worker context handle or error
     */
    public static Object initWorker(Object temporalClient, BMap<BString, Object> config) {
        try {
            System.out.println("[JWorker] WorkflowWorkerNative.initWorker() called");
            if (!(temporalClient instanceof WorkflowClient)) {
                return ErrorCreator.createError(
                        StringUtils.fromString("Invalid Temporal client handle"));
            }

            WorkflowClient client = (WorkflowClient) temporalClient;

            // Extract config parameters
            BString taskQueueBStr = (BString) config.get(StringUtils.fromString("taskQueue"));
            if (taskQueueBStr == null) {
                return ErrorCreator.createError(
                        StringUtils.fromString("taskQueue is required in listener config"));
            }
            String taskQueue = taskQueueBStr.getValue();
            System.out.println("[JWorker] Initializing worker for task queue: " + taskQueue);

            Object maxConcurrentWorkflowsObj = config.get(StringUtils.fromString("maxConcurrentWorkflows"));
            int maxConcurrentWorkflows = maxConcurrentWorkflowsObj instanceof Long ?
                                         ((Long) maxConcurrentWorkflowsObj).intValue() : 100;

            Object maxConcurrentActivitiesObj = config.get(StringUtils.fromString("maxConcurrentActivities"));
            int maxConcurrentActivities = maxConcurrentActivitiesObj instanceof Long ?
                                          ((Long) maxConcurrentActivitiesObj).intValue() : 100;

            // Create worker factory
            WorkerFactory workerFactory = WorkerFactory.newInstance(client);

            // Create worker for task queue
            Worker worker = workerFactory.newWorker(taskQueue);

            // Configure worker options (if needed)
            // Note: Worker configuration is typically done through WorkerOptions
            // which can be passed to newWorker() method

            // Create and return worker context
            WorkerContext context = new WorkerContext();
            context.workerFactory = workerFactory;
            context.worker = worker;
            context.taskQueue = taskQueue;

            System.out.println("[JWorker] Worker context created successfully for task queue: " + taskQueue);
            return context;

        } catch (Exception e) {
            return ErrorCreator.createError(
                    StringUtils.fromString("Failed to initialize worker: " + e.getMessage()));
        }
    }

    /**
     * Attach workflow service to worker.
     *
     * @param workerHandle Worker context handle
     * @param serviceObj   Ballerina service object
     * @param serviceName  Service name (workflow type)
     * @return null on success, error on failure
     */
    public static Object attachService(
            Object workerHandle,
            BObject serviceObj,
            BString serviceName) {
        try {
            System.out.println("[JWorker] WorkflowWorkerNative.attachService() called for: " + serviceName.getValue());
            if (!(workerHandle instanceof WorkerContext)) {
                return ErrorCreator.createError(
                        StringUtils.fromString("Invalid worker handle"));
            }

            WorkerContext context = (WorkerContext) workerHandle;
            String workflowType = serviceName.getValue();

            if (workflowType == null || workflowType.isEmpty()) {
                return ErrorCreator.createError(
                        StringUtils.fromString("Service name (workflow type) cannot be empty"));
            }

            // Store the service object in both local context and static registry
            context.registeredServices.put(workflowType, serviceObj);
            SERVICE_REGISTRY.put(workflowType, serviceObj);

            System.out.println("[JWorker] Registered service for workflow type: " + workflowType);

            // Register dynamic workflow implementation ONCE per worker
            if (!context.dynamicWorkflowRegistered) {
                System.out.println(
                        "[JWorker] Registering dynamic workflow adapter on task queue: " + context.taskQueue);

                // Register the BallerinaWorkflowAdapter to handle all workflow types on this queue
                // It implements DynamicWorkflow so it will handle any workflow type
                context.worker.registerWorkflowImplementationTypes(BallerinaWorkflowAdapter.class);
                context.dynamicWorkflowRegistered = true;

                System.out.println("[JWorker] Dynamic workflow adapter registered successfully");
            } else {
                System.out.println("[JWorker] Dynamic workflow adapter already registered for this worker");
            }

            // Register dynamic activity implementation ONCE per worker
            if (!context.dynamicActivityRegistered) {
                System.out.println(
                        "[JWorker] Registering dynamic activity adapter on task queue: " + context.taskQueue);

                // Register the BallerinaActivityAdapter to handle all activity types on this queue
                context.worker.registerActivitiesImplementations(new BallerinaActivityAdapter());
                context.dynamicActivityRegistered = true;

                System.out.println("[JWorker] Dynamic activity adapter registered successfully");
            } else {
                System.out.println("[JWorker] Dynamic activity adapter already registered for this worker");
            }

            return null;

        } catch (Exception e) {
            return ErrorCreator.createError(
                    StringUtils.fromString("Failed to attach service: " + e.getMessage()));
        }
    }

    /**
     * Detach workflow service from worker. }
     * <p>
     * /** Detach workflow service from worker.
     *
     * @param workerHandle Worker context handle
     * @param serviceObj   Ballerina service object
     * @return null on success, error on failure
     */
    public static Object detachService(Object workerHandle, BObject serviceObj) {
        try {
            if (!(workerHandle instanceof WorkerContext)) {
                return ErrorCreator.createError(
                        StringUtils.fromString("Invalid worker handle"));
            }

            WorkerContext context = (WorkerContext) workerHandle;

            // Find and remove the service
            String workflowType = null;
            for (Map.Entry<String, BObject> entry : context.registeredServices.entrySet()) {
                if (entry.getValue().equals(serviceObj)) {
                    workflowType = entry.getKey();
                    break;
                }
            }

            if (workflowType != null) {
                context.registeredServices.remove(workflowType);
            }

            // Note: Temporal workers don't support unregistering workflows at runtime
            // Once registered, they remain until the worker is stopped

            return null;

        } catch (Exception e) {
            return ErrorCreator.createError(
                    StringUtils.fromString("Failed to detach service: " + e.getMessage()));
        }
    }

    /**
     * Start the workflow worker (blocking).
     *
     * @param workerHandle Worker context handle
     * @return null on success, error on failure
     */
    public static Object startWorker(Object workerHandle) {
        try {
            System.out.println("[JWorker] WorkflowWorkerNative.startWorker() called");
            if (!(workerHandle instanceof WorkerContext)) {
                return ErrorCreator.createError(
                        StringUtils.fromString("Invalid worker handle"));
            }

            WorkerContext context = (WorkerContext) workerHandle;

            System.out.println("[JWorker] Starting worker factory for task queue: " + context.taskQueue);

            // Start the worker factory in a background thread to avoid blocking
            Thread workerThread = new Thread(() -> {
                try {
                    System.out.println("[JWorker] Worker thread starting for task queue: " + context.taskQueue);
                    context.workerFactory.start();
                    System.out.println(
                            "[JWorker] Worker factory started and polling for task queue: " + context.taskQueue);
                } catch (Exception e) {
                    System.err.println(
                            "[JWorker] Worker failed for task queue " + context.taskQueue + ": " + e.getMessage());
                }
            }, "temporal-worker-" + context.taskQueue);

            workerThread.setDaemon(false); // Keep JVM alive
            workerThread.start();

            // Give it a moment to initialize
            Thread.sleep(100);

            System.out.println("[JWorker] Worker thread launched successfully for task queue: " + context.taskQueue);
            return null;

        } catch (Exception e) {
            return ErrorCreator.createError(
                    StringUtils.fromString("Failed to start worker: " + e.getMessage()));
        }
    }

    /**
     * Stop the workflow worker.
     *
     * @param workerHandle Worker context handle
     * @return null on success, error on failure
     */
    public static Object stopWorker(Object workerHandle) {
        try {
            if (!(workerHandle instanceof WorkerContext)) {
                return ErrorCreator.createError(
                        StringUtils.fromString("Invalid worker handle"));
            }

            WorkerContext context = (WorkerContext) workerHandle;

            // Shutdown the worker factory
            context.workerFactory.shutdown();

            // Wait for shutdown to complete
            context.workerFactory.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);

            return null;

        } catch (Exception e) {
            return ErrorCreator.createError(
                    StringUtils.fromString("Failed to stop worker: " + e.getMessage()));
        }
    }

    static Object convertBallerinaToJavaType(Object ballerinaValue) {
        if (ballerinaValue == null) {
            return null;
        }

        // Handle Ballerina ErrorValue - convert to a serializable map representation
        // This is a valid return value, not a failure
        if (ballerinaValue instanceof io.ballerina.runtime.api.values.BError) {
            Map<String, Object> errorMap = getErrorMap((BError) ballerinaValue);

            System.out.println("[JActivityAdapter] Converted BError to serializable map: " + errorMap);
            return errorMap;
        }

        if (ballerinaValue instanceof BString) {
            return ((BString) ballerinaValue).getValue();
        } else if (ballerinaValue instanceof BMap) {
            BMap<?, ?> bMap = (BMap<?, ?>) ballerinaValue;
            Map<String, Object> javaMap = new HashMap<>();
            for (Object key : bMap.getKeys()) {
                String keyStr = key instanceof BString ? ((BString) key).getValue() : key.toString();
                Object value = bMap.get(key);
                javaMap.put(keyStr, convertBallerinaToJavaType(value));
            }
            return javaMap;
        } else if (ballerinaValue instanceof io.ballerina.runtime.api.values.BArray) {
            io.ballerina.runtime.api.values.BArray bArray =
                    (io.ballerina.runtime.api.values.BArray) ballerinaValue;
            List<Object> javaList = new ArrayList<>();
            for (int i = 0; i < bArray.size(); i++) {
                javaList.add(convertBallerinaToJavaType(bArray.get(i)));
            }
            return javaList;
        } else if (ballerinaValue instanceof io.ballerina.runtime.api.values.BDecimal) {
            return ((io.ballerina.runtime.api.values.BDecimal) ballerinaValue).decimalValue();
        } else if (ballerinaValue instanceof Long ||
                ballerinaValue instanceof Double ||
                ballerinaValue instanceof Boolean ||
                ballerinaValue instanceof Integer ||
                ballerinaValue instanceof String ||
                ballerinaValue instanceof java.math.BigDecimal) {
            // Primitives pass through
            return ballerinaValue;
        } else {
            // For unknown types, convert to string to avoid serialization issues
            System.out.println("[JActivityAdapter] Converting unknown type to string: " +
                                       ballerinaValue.getClass().getName());
            return ballerinaValue.toString();
        }
    }

    static Map<String, Object> getErrorMap(BError ballerinaValue) {
        BError error =
                ballerinaValue;

        // Convert BError to a serializable map
        Map<String, Object> errorMap = new HashMap<>();
        errorMap.put("__error__", true);
        errorMap.put("message", error.getMessage());
        errorMap.put("details", error.getDetails() != null ?
                                convertBallerinaToJavaType(error.getDetails()) : null);
        return errorMap;
    }

    /**
     * Register an activity implementation. Called from Ballerina code to register activity functions.
     *
     * @param activityName     Name of the activity
     * @param activityFunction Ballerina function pointer
     * @return null on success, error on failure
     */
    public static Object registerActivity(BString activityName, BFunctionPointer activityFunction) {
        try {
            String name = activityName.getValue();
            ACTIVITY_REGISTRY.put(name, activityFunction);
            System.out.println("[JActivity] Registered activity: " + name);
            return null;
        } catch (Exception e) {
            return ErrorCreator.createError(
                    StringUtils.fromString("Failed to register activity: " + e.getMessage()));
        }
    }

    /**
     * Worker context holding worker factory, worker, and registered services.
     */
    private static class WorkerContext {
        WorkerFactory workerFactory;
        Worker worker;
        Map<String, BObject> registeredServices = new ConcurrentHashMap<>();
        String taskQueue;
        boolean dynamicWorkflowRegistered = false; // Track if we've registered the dynamic workflow handler
        boolean dynamicActivityRegistered = false; // Track if we've registered the dynamic activity handler
    }

    /**
     * Dynamic workflow implementation that routes to Ballerina service. This is used as a template for creating
     * workflow implementations.
     */
    public static class BallerinaWorkflowAdapter implements DynamicWorkflow {

        private BObject serviceObject;
        private String workflowType;

        // Workflow logger for deterministic logging
        private static final Logger logger = Workflow.getLogger(BallerinaWorkflowAdapter.class);

        // No-arg constructor required by Temporal for dynamic workflows
        public BallerinaWorkflowAdapter() {
            // Register a dynamic signal handler that handles all signals
            Workflow.registerListener(
                (io.temporal.workflow.DynamicSignalHandler) (signalName, encodedArgs) -> {
                    logger.info("[JWorkflowAdapter] Signal received: {}", signalName);
                    
                    // Extract signal data from encodedArgs
                    Map<String, Object> signalData = new HashMap<>();
                    try {
                        // Try to get the first argument as a Map
                        @SuppressWarnings("unchecked")
                        Map<String, String> argMap = encodedArgs.get(0, Map.class);
                        if (argMap != null) {
                            signalData.putAll(argMap);
                            logger.info("[JWorkflowAdapter] Signal data extracted: {} entries", signalData.size());
                        }
                    } catch (Exception e) {
                        logger.warn("[JWorkflowAdapter] Could not extract signal data as Map: {}", e.getMessage());
                    }
                    
                    // Record the signal so awaitSignal can pick it up
                    logger.info("[JWorkflowAdapter] Recording signal: {} with {} data entries", 
                        signalName, signalData.size());
                    SignalAwaitWrapper.recordSignal(signalName, signalData);
                    logger.info("[JWorkflowAdapter] Signal {} recorded successfully", signalName);
                }
            );
            logger.info("[JWorkflowAdapter] Dynamic signal handler registered");
        }

        @Override
        public Object execute(EncodedValues args) {
            try {
                // Get workflow type from Temporal's Workflow.getInfo()
                io.temporal.workflow.WorkflowInfo info = Workflow.getInfo();
                this.workflowType = info.getWorkflowType();

                boolean isReplaying = Workflow.isReplaying();

                if (!isReplaying) {
                    logger.info("[JWorkflowAdapter] Executing workflow: {}", workflowType);
                }

                // Get the service object from static registry
                this.serviceObject = SERVICE_REGISTRY.get(workflowType);

                if (this.serviceObject == null) {
                    String errorMsg = String.format("Workflow service '%s' is not registered. " +
                        "Please ensure the workflow service is attached to the listener.", workflowType);
                    logger.error("[JWorkflowAdapter] {}", errorMsg);
                    
                    io.temporal.failure.ApplicationFailure failure = 
                        io.temporal.failure.ApplicationFailure.newFailure(
                            errorMsg,
                            "BallerinaServiceNotFound"
                        );
                    failure.setNonRetryable(true);
                    throw failure;
                }

                // Create Ballerina Context object with native workflow context handle
                BObject contextObj = createWorkflowContext();

                // Extract workflow arguments from EncodedValues
                // Carefully extract known number of arguments based on workflow type
                Object[] workflowArgs = extractWorkflowArguments(args);

                // Build arguments array: first is Context, rest are workflow args
                Object[] ballerinaArgs = new Object[workflowArgs.length + 1];
                ballerinaArgs[0] = contextObj;
                System.arraycopy(workflowArgs, 0, ballerinaArgs, 1, workflowArgs.length);

                if (!isReplaying) {
                    logger.info("[JWorkflowAdapter] Invoking Ballerina execute method for {} with {} args",
                            workflowType, workflowArgs.length);
                    logger.info("[JWorkflowAdapter] Executing on thread: {}", Thread.currentThread());
                }

                // Execute Ballerina workflow logic directly on calling thread
                // Note: Ballerina runtime appears incompatible with virtual threads
                Object result = ballerinaRuntime.callMethod(serviceObject, "execute", new StrandMetadata(true, Collections.emptyMap()), ballerinaArgs);

                if (!isReplaying) {
                    logger.info("[JWorkflowAdapter] Workflow {} completed with result type: {}",
                            workflowType, (result != null ? result.getClass().getSimpleName() : "null"));
                }

                // Check if workflow returned an error - this should fail the workflow execution
                if (result instanceof io.ballerina.runtime.api.values.BError) {
                    io.ballerina.runtime.api.values.BError error =
                            (io.ballerina.runtime.api.values.BError) result;
                    String errorMsg = error.getMessage();
                    
                    // Only log during actual execution, not during replay
                    if (!isReplaying) {
                        logger.error("[JWorkflowAdapter] Workflow returned error (Business Failure): {}", errorMsg);
                        logger.error("[JWorkflowAdapter] Failing workflow execution with ApplicationFailure (non-retryable)");
                    }
                    
                    // Create a clean ApplicationFailure without Java stack trace
                    // Use newFailureWithCause with null cause to avoid stack trace pollution
                    Map<String, Object> errorDetails = getErrorMap(error);
                    
                    // Build a Ballerina-friendly error message
                    String ballerinaErrorMsg = String.format("Workflow '%s' failed: %s", workflowType, errorMsg);
                    
                    // Create ApplicationFailure without stack trace by using newFailure() and setting it manually
                    io.temporal.failure.ApplicationFailure failure = 
                        io.temporal.failure.ApplicationFailure.newFailure(
                            ballerinaErrorMsg,
                            "BallerinaWorkflowError",
                            errorDetails
                        );
                    
                    // Mark as non-retryable
                    failure.setNonRetryable(true);
                    
                    throw failure;
                }

                // Convert Ballerina result to Java type for Temporal serialization
                Object javaResult = convertBallerinaToJavaType(result);

                if (!isReplaying) {
                    logger.info("[JWorkflowAdapter] Workflow completed successfully, result: {}", javaResult);
                }

                return javaResult;

            } catch (io.temporal.failure.TemporalFailure e) {
                // Re-throw Temporal failures as-is (ApplicationFailure, etc.)
                throw e;
            } catch (Exception e) {
                // Wrap unexpected exceptions in ApplicationFailure to avoid workflow task retry loop
                // LOG FULL STACK TRACE FOR DEBUGGING
                logger.error("[JWorkflowAdapter] ========================================");
                logger.error("[JWorkflowAdapter] WORKFLOW EXECUTION FAILED");
                logger.error("[JWorkflowAdapter] Workflow Type: {}", workflowType);
                logger.error("[JWorkflowAdapter] Exception Type: {}", e.getClass().getName());
                logger.error("[JWorkflowAdapter] Error Message: {}", e.getMessage());
                logger.error("[JWorkflowAdapter] Full Stack Trace:", e);
                logger.error("[JWorkflowAdapter] ========================================");
                
                // Print to stderr as well for immediate visibility
                System.err.println("[JWorkflowAdapter] ========================================");
                System.err.println("[JWorkflowAdapter] WORKFLOW EXECUTION FAILED: " + workflowType);
                System.err.println("[JWorkflowAdapter] Exception: " + e.getClass().getName());
                System.err.println("[JWorkflowAdapter] Message: " + e.getMessage());
                e.printStackTrace(System.err);
                System.err.println("[JWorkflowAdapter] ========================================");
                
                // Create detailed error message with exception type and message
                String detailedErrorMsg = String.format("Workflow '%s' encountered an error: %s - %s", 
                    workflowType, e.getClass().getSimpleName(), e.getMessage());
                
                io.temporal.failure.ApplicationFailure failure = 
                    io.temporal.failure.ApplicationFailure.newFailure(
                        detailedErrorMsg,
                        "BallerinaWorkflowExecutionError"
                    );
                failure.setNonRetryable(true);
                
                throw failure;
            }
        }

        private Object[] extractWorkflowArguments(EncodedValues args) {
            // Extract arguments carefully without blocking
            // For approval workflow: documentId (String), submitter (String)
            // For order workflow: orderId, customerId, amount
            // For saga workflow: transactionId, fromAccount, toAccount, amount

            List<Object> argsList = new ArrayList<>();

            // Try to extract up to 4 arguments (max we need for saga workflow)
            for (int i = 0; i < 4; i++) {
                try {
                    Object arg = args.get(i, Object.class);
                    if (arg != null) {
                        // Convert Java types to Ballerina types
                        Object ballerinaArg = convertJavaToBallerinaType(arg);
                        argsList.add(ballerinaArg);
                    }
                } catch (Exception e) {
                    // No more arguments
                    break;
                }
            }

            return argsList.toArray();
        }

        private Object convertJavaToBallerinaType(Object javaValue) {
            if (javaValue instanceof String) {
                return StringUtils.fromString((String) javaValue);
            } else if (javaValue instanceof Integer) {
                return Long.valueOf((Integer) javaValue);
            } else if (javaValue instanceof Long) {
                return javaValue;
            } else if (javaValue instanceof Double) {
                return javaValue;
            } else if (javaValue instanceof Boolean) {
                return javaValue;
            } else if (javaValue instanceof java.math.BigDecimal) {
                // Keep BigDecimal as is, Ballerina can handle it
                return javaValue;
            } else {
                // For other types, return as-is and let Ballerina handle it
                return javaValue;
            }
        }

        /**
         * Convert Ballerina types to Java types for Temporal serialization. This is critical because Jackson cannot
         * serialize Ballerina internal types.
         */
        private Object convertBallerinaToJavaType(Object ballerinaValue) {
            if (ballerinaValue == null) {
                return null;
            }

            // Handle Ballerina ErrorValue - preserve as serializable error map
            if (ballerinaValue instanceof io.ballerina.runtime.api.values.BError) {
                io.ballerina.runtime.api.values.BError error =
                        (io.ballerina.runtime.api.values.BError) ballerinaValue;
                java.util.HashMap<String, Object> errorMap = new java.util.HashMap<>();
                errorMap.put("__error__", true);
                errorMap.put("message", error.getMessage());
                errorMap.put("details", convertBallerinaToJavaType(error.getDetails()));
                return errorMap;
            }

            // Handle BString
            if (ballerinaValue instanceof io.ballerina.runtime.api.values.BString) {
                return ((io.ballerina.runtime.api.values.BString) ballerinaValue).getValue();
            }

            // Handle BMap (convert to HashMap)
            if (ballerinaValue instanceof io.ballerina.runtime.api.values.BMap) {
                io.ballerina.runtime.api.values.BMap<?, ?> bmap =
                        (io.ballerina.runtime.api.values.BMap<?, ?>) ballerinaValue;
                java.util.HashMap<String, Object> map = new java.util.HashMap<>();
                for (Object key : bmap.getKeys()) {
                    String keyStr = key instanceof io.ballerina.runtime.api.values.BString ?
                                    ((io.ballerina.runtime.api.values.BString) key).getValue() : key.toString();
                    Object value = bmap.get(key);
                    map.put(keyStr, convertBallerinaToJavaType(value));
                }
                return map;
            }

            // Handle BArray (convert to ArrayList)
            if (ballerinaValue instanceof io.ballerina.runtime.api.values.BArray) {
                io.ballerina.runtime.api.values.BArray barray =
                        (io.ballerina.runtime.api.values.BArray) ballerinaValue;
                java.util.ArrayList<Object> list = new java.util.ArrayList<>();
                for (int i = 0; i < barray.size(); i++) {
                    list.add(convertBallerinaToJavaType(barray.get(i)));
                }
                return list;
            }

            // Handle BDecimal
            if (ballerinaValue instanceof io.ballerina.runtime.api.values.BDecimal) {
                return ((io.ballerina.runtime.api.values.BDecimal) ballerinaValue).decimalValue();
            }

            // Handle primitive types (Long, Double, Boolean) - pass through
            if (ballerinaValue instanceof Long ||
                    ballerinaValue instanceof Double ||
                    ballerinaValue instanceof Boolean ||
                    ballerinaValue instanceof Integer ||
                    ballerinaValue instanceof String ||
                    ballerinaValue instanceof java.math.BigDecimal) {
                return ballerinaValue;
            }

            // For other types, convert to string representation
            logger.warn("[JWorkflowAdapter] Converting unknown Ballerina type to string: {}",
                    ballerinaValue.getClass().getName());
            return ballerinaValue.toString();
        }

        private BObject createWorkflowContext() {
            // Ensure workflow module is initialized
            if (workflowModule == null) {
                String errorMsg = "Ballerina workflow module is not properly initialized. " +
                    "This is an internal configuration error.";
                logger.error("[JWorkflowAdapter] {}", errorMsg);
                
                io.temporal.failure.ApplicationFailure failure = 
                    io.temporal.failure.ApplicationFailure.newFailure(
                        errorMsg,
                        "BallerinaModuleNotInitialized"
                    );
                failure.setNonRetryable(true);
                throw failure;
            }

            // Create a proper ContextInfo object from WorkflowContextNative
            // This is what the native methods expect as the context handle
            io.temporal.workflow.WorkflowInfo temporalInfo = io.temporal.workflow.Workflow.getInfo();
            Object contextInfo = io.ballerina.stdlib.workflow.context.WorkflowContextNative.createContext(
                    temporalInfo.getWorkflowId(),
                    temporalInfo.getWorkflowType(),
                    new HashMap<>() // correlation data
                                                                                                         );

            // Wrap in HandleValue for Ballerina
            Object nativeContextHandle = ValueCreator.createHandleValue(contextInfo);

            // Create the Context object using ValueCreator with the proper module
            // Context has init(handle nativeContext) constructor
            BObject contextObj = ValueCreator.createObjectValue(
                    workflowModule,
                    "Context",
                    nativeContextHandle
                                                               );

            return contextObj;
        }
    }

    /**
     * Dynamic activity implementation that routes activity calls to registered Ballerina functions. Uses Temporal's
     * DynamicActivity interface for true dynamic routing without predefined method signatures.
     */
    public static class BallerinaActivityAdapter implements DynamicActivity {

        @Override
        public Object execute(EncodedValues args) {
            try {
                // Get activity name from Temporal's Activity.getExecutionContext()
                io.temporal.activity.ActivityExecutionContext activityContext =
                        io.temporal.activity.Activity.getExecutionContext();
                String activityName = activityContext.getInfo().getActivityType();

                System.out.println("[JActivityAdapter] BallerinaActivityAdapter executing activity: " + activityName);

                // Look up the registered Ballerina function for this activity
                BFunctionPointer activityFunction = ACTIVITY_REGISTRY.get(activityName);
                if (activityFunction == null) {
                    String errorMsg = "Activity not registered: " + activityName +
                            ". Available activities: " + ACTIVITY_REGISTRY.keySet();
                    System.err.println("[JActivityAdapter] " + errorMsg);
                    throw new RuntimeException(errorMsg);
                }

                // Decode arguments from Temporal - get each argument by index
                // EncodedValues doesn't have a size() method, so try to get up to 10 args
                List<Object> argsList = new ArrayList<>();
                for (int i = 0; i < 10; i++) {
                    try {
                        Object arg = args.get(i, Object.class);
                        if (arg != null) {
                            argsList.add(arg);
                        } else {
                            break;
                        }
                    } catch (Exception e) {
                        // No more arguments
                        break;
                    }
                }
                Object[] javaArgs = argsList.toArray();
                System.out.println("[JActivityAdapter] Activity args count: " + javaArgs.length);
                if (javaArgs.length > 0) {
                    System.out.println("[JActivityAdapter] First arg: " + javaArgs[0] + " (type: " +
                                               javaArgs[0].getClass().getSimpleName() + ")");
                }


                Object[] ballerinaArgs = new Object[javaArgs != null ? javaArgs.length : 0];
                for (int i = 0; i < ballerinaArgs.length; i++) {
                    ballerinaArgs[i] = convertJavaToBallerinaType(javaArgs[i]);
                }

                // Call the Ballerina function with Runtime directly on calling thread
                // Note: Ballerina runtime appears incompatible with virtual threads
                System.out.println("[JActivityAdapter] Invoking Ballerina activity function: " + activityName);
                System.out.println("[JActivityAdapter] Executing on thread: " + Thread.currentThread());

                FPValue fpValue = (FPValue) activityFunction;
                fpValue.metadata = new StrandMetadata(true, fpValue.metadata.properties());
                Object result = activityFunction.call(ballerinaRuntime, ballerinaArgs);
                System.out.println("[JActivityAdapter] Activity function call completed, result type: " +
                                           (result != null ? result.getClass().getSimpleName() : "null"));

                // Check if result is a BError - this is a valid return value, not a failure
                if (result instanceof io.ballerina.runtime.api.values.BError) {
                    io.ballerina.runtime.api.values.BError error =
                        (io.ballerina.runtime.api.values.BError) result;
                    System.out.println("[JActivityAdapter] Activity returned error value (treated as normal value): " +
                                   error.getMessage());
                }

                // Convert result back to Java types for Temporal
                // BError will be converted to a serializable error representation
                Object javaResult = convertBallerinaToJavaType(result);
                System.out.println("[JActivityAdapter] Activity " + activityName + " completed, result: " + javaResult);

                return javaResult;

            } catch (Exception e) {
                // Activity threw an exception (panic/uncontrolled error) - this is a real failure
                System.err.println("[JActivityAdapter] Activity execution failed with exception: " + e.getMessage());
                System.err.println("[JActivityAdapter] This is an activity failure that will be retried by Temporal");
                throw e;
            }
        }
    }
}
