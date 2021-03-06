/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.titus.master.scheduler.constraint;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.queues.QueuableTask;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.agent.model.InstanceLifecycleState;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.agent.service.AgentStatusMonitor;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.master.scheduler.SchedulerAttributes;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;
import com.netflix.titus.master.scheduler.SchedulerUtils;

import static com.netflix.titus.master.scheduler.SchedulerUtils.getTier;

/**
 * A system constraint that integrates with agent management in order to determine whether or a not a task
 * should be placed.
 */
@Singleton
public class AgentManagementConstraint implements SystemConstraint {

    public static final String NAME = "AgentManagementConstraint";

    private static final Result INSTANCE_GROUP_NOT_FOUND = new Result(false, "Instance group not found");
    private static final Result INSTANCE_GROUP_NOT_ACTIVE = new Result(false, "Instance group is not active or phased out");
    private static final Result INSTANCE_GROUP_TIER_MISMATCH = new Result(false, "Task cannot run on instance group tier");
    private static final Result INSTANCE_GROUP_DOES_NOT_HAVE_GPUS = new Result(false, "Instance group does not have gpus");
    private static final Result INSTANCE_GROUP_CANNOT_RUN_NON_GPU_TASKS = new Result(false, "Instance group does not run non gpu tasks");

    private static final Result INSTANCE_NOT_FOUND = new Result(false, "Instance not found");
    private static final Result INSTANCE_NOT_STARTED = new Result(false, "Instance not in Started state");
    private static final Result INSTANCE_UNHEALTHY = new Result(false, "Unhealthy agent");

    private static final Result SYSTEM_NO_PLACEMENT = new Result(false, "Cannot place on instance group or agent instance due to systemNoPlacement attribute");
    private static final Result NO_PLACEMENT = new Result(false, "Cannot place on instance group or agent instance due to noPlacement attribute");

    private static final Result TRUE_RESULT = new Result(true, null);

    private static final Set<String> FAILURE_REASONS = CollectionsExt.asSet(
            INSTANCE_GROUP_NOT_FOUND.getFailureReason(),
            INSTANCE_GROUP_NOT_ACTIVE.getFailureReason(),
            INSTANCE_GROUP_TIER_MISMATCH.getFailureReason(),
            INSTANCE_GROUP_DOES_NOT_HAVE_GPUS.getFailureReason(),
            INSTANCE_GROUP_CANNOT_RUN_NON_GPU_TASKS.getFailureReason(),
            INSTANCE_NOT_FOUND.getFailureReason(),
            INSTANCE_NOT_STARTED.getFailureReason(),
            INSTANCE_UNHEALTHY.getFailureReason(),
            SYSTEM_NO_PLACEMENT.getFailureReason(),
            NO_PLACEMENT.getFailureReason()
    );

    private final SchedulerConfiguration schedulerConfiguration;
    private final AgentManagementService agentManagementService;
    private final AgentStatusMonitor agentStatusMonitor;

    @Inject
    public AgentManagementConstraint(SchedulerConfiguration schedulerConfiguration,
                                     AgentManagementService agentManagementService,
                                     AgentStatusMonitor agentStatusMonitor) {
        this.schedulerConfiguration = schedulerConfiguration;
        this.agentManagementService = agentManagementService;
        this.agentStatusMonitor = agentStatusMonitor;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        Optional<AgentInstance> instanceOpt = SchedulerUtils.findInstance(agentManagementService, schedulerConfiguration.getInstanceAttributeName(), targetVM);
        if (!instanceOpt.isPresent()) {
            return INSTANCE_NOT_FOUND;
        }

        AgentInstance instance = instanceOpt.get();
        String instanceGroupId = instance.getInstanceGroupId();

        Optional<AgentInstanceGroup> instanceGroupOpt = agentManagementService.findInstanceGroup(instanceGroupId);
        if (!instanceGroupOpt.isPresent()) {
            return INSTANCE_GROUP_NOT_FOUND;
        }

        Result instanceGroupEvaluationResult = evaluateInstanceGroup(taskRequest, instanceGroupOpt.get());
        if (instanceGroupEvaluationResult != TRUE_RESULT) {
            return instanceGroupEvaluationResult;
        }

        Result InstanceEvaluationResult = evaluateInstance(instance);
        if (InstanceEvaluationResult != TRUE_RESULT) {
            return InstanceEvaluationResult;
        }

        return TRUE_RESULT;
    }

    public static boolean isAgentManagementConstraintReason(String reason) {
        return reason != null && FAILURE_REASONS.contains(reason);
    }

    private Result evaluateInstanceGroup(TaskRequest taskRequest, AgentInstanceGroup instanceGroup) {
        InstanceGroupLifecycleState state = instanceGroup.getLifecycleStatus().getState();

        //TODO safer way to know what is active?
        if (state != InstanceGroupLifecycleState.Active && state != InstanceGroupLifecycleState.PhasedOut) {
            return INSTANCE_GROUP_NOT_ACTIVE;
        }

        Result instanceGroupAttributesResult = evaluateInstanceGroupAttributes(instanceGroup);
        if (instanceGroupAttributesResult != TRUE_RESULT) {
            return instanceGroupAttributesResult;
        }

        Tier tier = getTier((QueuableTask) taskRequest);
        if (instanceGroup.getTier() != tier) {
            return INSTANCE_GROUP_TIER_MISMATCH;
        }

        //TODO read job resource dimensions when we get rid of v2
        boolean gpuTask = taskRequestsGpu(taskRequest);
        boolean gpuAgent = instanceGroup.getResourceDimension().getGpu() > 0;
        if (gpuTask && !gpuAgent) {
            return INSTANCE_GROUP_DOES_NOT_HAVE_GPUS;
        }
        if (!gpuTask && gpuAgent) {
            return INSTANCE_GROUP_CANNOT_RUN_NON_GPU_TASKS;
        }

        return TRUE_RESULT;
    }

    private Result evaluateInstance(AgentInstance instance) {
        InstanceLifecycleState state = instance.getLifecycleStatus().getState();
        if (state != InstanceLifecycleState.Started) {
            return INSTANCE_NOT_STARTED;
        }

        Result instanceAttributesResult = evaluateAgentInstanceAttributes(instance);
        if (instanceAttributesResult != TRUE_RESULT) {
            return instanceAttributesResult;
        }

        if (!agentStatusMonitor.isHealthy(instance.getId())) {
            return INSTANCE_UNHEALTHY;
        }

        return TRUE_RESULT;
    }

    private boolean taskRequestsGpu(TaskRequest taskRequest) {
        Map<String, Double> scalars = taskRequest.getScalarRequests();
        if (scalars != null && !scalars.isEmpty()) {
            final Double gpu = scalars.get("gpu");
            return gpu != null && gpu >= 1.0;
        }
        return false;
    }

    private Result evaluateInstanceGroupAttributes(AgentInstanceGroup instanceGroup) {
        Map<String, String> attributes = instanceGroup.getAttributes();
        boolean systemNoPlacement = Boolean.parseBoolean(attributes.get(SchedulerAttributes.SYSTEM_NO_PLACEMENT));
        if (systemNoPlacement) {
            return SYSTEM_NO_PLACEMENT;
        }

        boolean noPlacement = Boolean.parseBoolean(attributes.get(SchedulerAttributes.NO_PLACEMENT));
        if (noPlacement) {
            return NO_PLACEMENT;
        }

        return TRUE_RESULT;
    }

    private Result evaluateAgentInstanceAttributes(AgentInstance agentInstance) {
        Map<String, String> attributes = agentInstance.getAttributes();
        boolean systemNoPlacement = Boolean.parseBoolean(attributes.get(SchedulerAttributes.SYSTEM_NO_PLACEMENT));
        if (systemNoPlacement) {
            return SYSTEM_NO_PLACEMENT;
        }

        boolean noPlacement = Boolean.parseBoolean(attributes.get(SchedulerAttributes.NO_PLACEMENT));
        if (noPlacement) {
            return NO_PLACEMENT;
        }

        return TRUE_RESULT;
    }
}