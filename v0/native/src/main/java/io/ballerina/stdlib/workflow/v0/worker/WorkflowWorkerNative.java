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

package io.ballerina.stdlib.workflow.v0.worker;

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.Runtime;
import io.ballerina.runtime.api.creators.ErrorCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.*;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.stdlib.workflow.v0.utils.TypesUtil;
import io.temporal.activity.DynamicActivity;
import io.temporal.client.WorkflowClient;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import io.temporal.workflow.DynamicWorkflow;
import io.temporal.workflow.Workflow;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.ballerina.stdlib.workflow.v0.utils.TypesUtil.convertJavaToBallerinaType;

/**
 * Native implementation for workflow worker operations.
 */
public class WorkflowWorkerNative {

    // Static registry to store workflow types (workflow name -> typedesc)
    private static final Map<String, BTypedesc> WORKFLOW_REGISTRY = new ConcurrentHashMap<>();

    // Static registry to store activity implementations (activity name -> BFunctionPointer)
    private static final Map<String, BFunctionPointer> ACTIVITY_REGISTRY = new ConcurrentHashMap<>();

    // Thread-local storage for current workflow object (for dynamic activity registration)
    private static final ThreadLocal<BObject> CURRENT_WORKFLOW_OBJECT = new ThreadLocal<>();

    // Store workflow module for creating Context objects
    private static io.ballerina.runtime.api.Module workflowModule;

    // Store Runtime instance for creating Strands
    private static Runtime ballerinaRuntime;

    /**
     * Module initialization - called by Ballerina runtime
     */
    public static void init(Environment env) {
        workflowModule = env.getCurrentModule();
        ballerinaRuntime = env.getRuntime();
    }

    /**
     * Register an activity dynamically (called from ContextNative).
     * Extracts the activity function from the current workflow object.
     */
    public static void registerActivity(String activityName) {
        // Check if already registered
        if (ACTIVITY_REGISTRY.containsKey(activityName)) {
            return;
        }

        // Get the current workflow object from thread-local
        BObject workflowObj = CURRENT_WORKFLOW_OBJECT.get();
        if (workflowObj == null) {
            throw new RuntimeException("No workflow object in context for activity registration: " + activityName);
        }

        // Get the function pointer for the activity method
        Object methodObj = workflowObj.get(StringUtils.fromString(extractMethodName(activityName)));
        if (methodObj instanceof BFunctionPointer) {
            ACTIVITY_REGISTRY.put(activityName, (BFunctionPointer) methodObj);
        } else {
            throw new RuntimeException("Could not find activity method: " + activityName);
        }
    }

    /**
     * Register an activity directly with the provided function pointer
     */
    public static void registerActivityWithFunction(String activityName, BFunctionPointer activityFunc) {
        System.out.println("[DEBUG] registerActivityWithFunction called");
        System.out.println("[DEBUG] Activity name: " + activityName);
        System.out.println("[DEBUG] Activity function: " + activityFunc);
        
        // Check if already registered
        if (ACTIVITY_REGISTRY.containsKey(activityName)) {
            System.out.println("[DEBUG] Activity already registered, skipping");
            return;
        }

        // Register the activity with the provided function pointer
        ACTIVITY_REGISTRY.put(activityName, activityFunc);
        System.out.println("[DEBUG] Activity registered successfully");
        System.out.println("[DEBUG] Registry now contains: " + ACTIVITY_REGISTRY.keySet());
    }

    /**
     * Extract the simple method name from the generated activity name.
     * E.g., "$anon$method$delegate$ApprovalService.sendEmail$0" -> "sendEmail"
     */
    private static String extractMethodName(String activityName) {
        // Activity names are in format: $anon$method$delegate$ClassName.methodName$index
        int lastDot = activityName.lastIndexOf('.');
        if (lastDot >= 0) {
            String afterDot = activityName.substring(lastDot + 1);
            // Remove the trailing $index
            int dollarIndex = afterDot.lastIndexOf('$');
            if (dollarIndex >= 0) {
                return afterDot.substring(0, dollarIndex);
            }
            return afterDot;
        }
        return activityName;
    }

    /**
     * Initialize workflow worker.
     */
    public static Object initWorker(Object temporalClient, BMap<BString, Object> config) {
        try {
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

            // Create worker factory
            WorkerFactory workerFactory = WorkerFactory.newInstance(client);

            // Create worker for task queue
            Worker worker = workerFactory.newWorker(taskQueue);

            // Create and return worker context
            WorkerContext context = new WorkerContext();
            context.workerFactory = workerFactory;
            context.worker = worker;
            context.taskQueue = taskQueue;

            return context;

        } catch (Exception e) {
            return ErrorCreator.createError(
                    StringUtils.fromString("Failed to initialize worker: " + e.getMessage()));
        }
    }

    /**
     * Attach workflow service to worker.
     */
    public static Object attachService(Object workerHandle, BString workflowName, BTypedesc workflowType) {
        try {
            if (!(workerHandle instanceof WorkerContext)) {
                return ErrorCreator.createError(
                        StringUtils.fromString("Invalid worker handle"));
            }

            WorkerContext context = (WorkerContext) workerHandle;
            String wfName = workflowName.getValue();

            if (wfName == null || wfName.isEmpty()) {
                return ErrorCreator.createError(
                        StringUtils.fromString("Workflow name cannot be empty"));
            }

            // Store the workflow type in registry using the provided name
            WORKFLOW_REGISTRY.put(wfName, workflowType);
            context.registeredWorkflows.put(wfName, workflowType);

            // Register dynamic workflow implementation ONCE per worker
            if (!context.dynamicWorkflowRegistered) {
                context.worker.registerWorkflowImplementationTypes(BallerinaWorkflowAdapter.class);
                context.dynamicWorkflowRegistered = true;
            }

            // Register dynamic activity implementation ONCE per worker
            if (!context.dynamicActivityRegistered) {
                context.worker.registerActivitiesImplementations(new BallerinaActivityAdapter());
                context.dynamicActivityRegistered = true;
            }

            return null;

        } catch (Exception e) {
            return ErrorCreator.createError(
                    StringUtils.fromString("Failed to attach service: " + e.getMessage()));
        }
    }

    /**
     * Start the worker to begin processing workflows.
     */
    public static Object startWorker(Object workerHandle) {
        try {
            if (!(workerHandle instanceof WorkerContext)) {
                return ErrorCreator.createError(
                        StringUtils.fromString("Invalid worker handle"));
            }

            WorkerContext context = (WorkerContext) workerHandle;
            context.workerFactory.start();
            return null;

        } catch (Exception e) {
            return ErrorCreator.createError(
                    StringUtils.fromString("Failed to start worker: " + e.getMessage()));
        }
    }

    /**
     * Stop the worker.
     */
    public static Object stopWorker(Object workerHandle) {
        try {
            if (!(workerHandle instanceof WorkerContext)) {
                return ErrorCreator.createError(
                        StringUtils.fromString("Invalid worker handle"));
            }

            WorkerContext context = (WorkerContext) workerHandle;
            context.workerFactory.shutdown();
            return null;

        } catch (Exception e) {
            return ErrorCreator.createError(
                    StringUtils.fromString("Failed to stop worker: " + e.getMessage()));
        }
    }

    /**
     * Dynamic workflow adapter - handles all workflow types registered with the worker.
     */
    public static class BallerinaWorkflowAdapter implements DynamicWorkflow {
        @Override
        public Object execute(io.temporal.common.converter.EncodedValues args) {
            try {
                System.out.println("[DEBUG] BallerinaWorkflowAdapter.execute() called");
                String workflowType = Workflow.getInfo().getWorkflowType();
                System.out.println("[DEBUG] Workflow type: " + workflowType);
                BTypedesc typedesc = WORKFLOW_REGISTRY.get(workflowType);
                
                if (typedesc == null) {
                    throw new RuntimeException("Workflow type not registered: " + workflowType);
                }

                // Get the service type from typedesc
                Type serviceType = typedesc.getDescribingType();
                System.out.println("[DEBUG] Service type: " + serviceType);
                
                // Create workflow instance from the service type's package and name
                BObject workflowObj = ValueCreator.createObjectValue(
                    serviceType.getPackage(),
                    serviceType.getName()
                );
                System.out.println("[DEBUG] Workflow object created: " + workflowObj);

                // Create WFContext
                BObject wfContext = ValueCreator.createObjectValue(
                    workflowModule,
                    "WFContext"
                );
                System.out.println("[DEBUG] WFContext created");

                // Extract arguments from EncodedValues
                // Try to extract up to 10 arguments (should be enough for most use cases)
                java.util.List<Object> workflowArgsList = new java.util.ArrayList<>();
                for (int i = 0; i < 10; i++) {
                    try {
                        Object arg = args.get(i, Object.class);
                        if (arg != null) {
                            workflowArgsList.add(convertJavaToBallerinaType(arg));
                        }
                    } catch (Exception e) {
                        // No more arguments
                        break;
                    }
                }
                System.out.println("[DEBUG] Extracted " + workflowArgsList.size() + " arguments");

                // Call execute method with context and args
                Object[] ballerinaArgs = new Object[workflowArgsList.size() + 1];
                ballerinaArgs[0] = wfContext;
                for (int i = 0; i < workflowArgsList.size(); i++) {
                    ballerinaArgs[i + 1] = workflowArgsList.get(i);
                }

                // Store workflow object in thread-local for activity registration
                CURRENT_WORKFLOW_OBJECT.set(workflowObj);
                System.out.println("[DEBUG] Workflow object stored in thread-local");
                
                try {
                    // Call execute method on workflow object using Ballerina runtime
                    System.out.println("[DEBUG] Calling workflow execute method...");
                    Object result = ballerinaRuntime.callMethod(
                        workflowObj,
                        "execute",
                        new io.ballerina.runtime.api.concurrent.StrandMetadata(true, java.util.Collections.emptyMap()),
                        ballerinaArgs
                    );
                    System.out.println("[DEBUG] Workflow execute returned: " + result);

                    // Check if result is an error
                    if (result instanceof io.ballerina.runtime.api.values.BError) {
                        System.out.println("[ERROR] Workflow returned error: " + result);
                        throw new RuntimeException("Workflow failed: " + result);
                    }

                    return result;
                } finally {
                    // Clean up thread-local
                    CURRENT_WORKFLOW_OBJECT.remove();
                    System.out.println("[DEBUG] Thread-local cleared");
                }
            } catch (Exception e) {
                System.out.println("[ERROR] Exception in workflow adapter: " + e.getMessage());
                e.printStackTrace();
                throw e;
            }
        }
    }

    /**
     * Dynamic activity adapter - handles all activities.
     */
    public static class BallerinaActivityAdapter implements DynamicActivity {
        @Override
        public Object execute(io.temporal.common.converter.EncodedValues args) {
            System.out.println("[DEBUG] BallerinaActivityAdapter.execute() called");
            String activityName = io.temporal.activity.Activity.getExecutionContext().getInfo().getActivityType();
            System.out.println("[DEBUG] Activity type: " + activityName);
            BFunctionPointer activityFn = ACTIVITY_REGISTRY.get(activityName);
            System.out.println("[DEBUG] Activity function from registry: " + activityFn);

            if (activityFn == null) {
                System.out.println("[ERROR] Activity not found in registry!");
                System.out.println("[DEBUG] Registry contents: " + ACTIVITY_REGISTRY.keySet());
                throw new RuntimeException("Activity not registered: " + activityName);
            }

            // Extract arguments from EncodedValues (same as workflow args extraction)
            java.util.List<Object> argsList = new java.util.ArrayList<>();
            for (int i = 0; i < 10; i++) {
                try {
                    Object arg = args.get(i, Object.class);
                    if (arg != null) {
                        argsList.add(TypesUtil.convertJavaToBallerinaType(arg));
                    }
                } catch (Exception e) {
                    // No more arguments
                    break;
                }
            }
            System.out.println("[DEBUG] Extracted " + argsList.size() + " arguments");

            Object[] ballerinaArgs = argsList.toArray();

            // Call the activity function
            System.out.println("[DEBUG] Calling activity function...");
            Object result = activityFn.call(ballerinaRuntime, ballerinaArgs);
            System.out.println("[DEBUG] Activity function returned: " + result);
            
            // Convert result back to Java for Temporal
            Object javaResult = TypesUtil.convertBallerinaToJava(result);
            System.out.println("[DEBUG] Converted result: " + javaResult);
            return javaResult;
        }
    }

    /**
     * Worker context to store worker state.
     */
    static class WorkerContext {
        WorkerFactory workerFactory;
        Worker worker;
        String taskQueue;
        Map<String, BTypedesc> registeredWorkflows = new HashMap<>();
        boolean dynamicWorkflowRegistered = false;
        boolean dynamicActivityRegistered = false;
    }

    public static Map<String, BFunctionPointer> getActivityRegistry() {
        return ACTIVITY_REGISTRY;
    }
}
