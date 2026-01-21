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

package io.ballerina.stdlib.workflow.v0.context;

import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.util.function.Supplier;

/**
 * Workflow await wrapper for condition-based waiting.
 * 
 * CRITICAL: Must be called directly from workflow thread.
 * Do NOT wrap in CompletableFuture.supplyAsync() or similar constructs.
 */
public class WorkflowAwaitWrapper {

    /**
     * Wait for a boolean condition to become true.
     * 
     * @param timeoutSeconds Timeout in seconds
     * @param condition Supplier that returns true when condition is met
     * @return true if condition met, false if timeout
     */
    public static boolean awaitCondition(int timeoutSeconds, Supplier<Boolean> condition) {
        return Workflow.await(Duration.ofSeconds(timeoutSeconds), condition);
    }

    /**
     * Sleep for specified duration.
     * 
     * @param seconds Duration in seconds
     */
    public static void sleep(int seconds) {
        Workflow.sleep(Duration.ofSeconds(seconds));
    }

    /**
     * Wait for a boolean flag to become true.
     * 
     * @param flagSupplier Supplier for the flag
     * @param timeoutSeconds Timeout in seconds
     * @return true if flag became true, false if timeout
     */
    public static boolean waitForFlag(Supplier<Boolean> flagSupplier, int timeoutSeconds) {
        return Workflow.await(Duration.ofSeconds(timeoutSeconds), flagSupplier);
    }
}
