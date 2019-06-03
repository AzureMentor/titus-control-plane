/*
 * Copyright 2019 Netflix, Inc.
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

import java.util.List;
import java.util.Optional;

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.jobmanager.model.job.vpc.IpAddressAllocation;
import com.netflix.titus.api.jobmanager.model.job.vpc.IpAddressLocation;
import com.netflix.titus.common.annotation.Experimental;
import com.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;
import com.netflix.titus.master.scheduler.SchedulerUtils;

/**
 * Experimental constraint that prefers a machine that can allocate a specific IP.
 */
@Experimental(deadline = "06/2019")
public class IpAllocationConstraint implements ConstraintEvaluator {

    public static final String NAME = "IpAllocationConstraint";

    private static final Result VALID = new Result(true, null);
    private static final Result MACHINE_DOES_NOT_EXIST = new Result(false, "The machine does not exist");
    private static final Result IP_ALLOCATION_FIELDS_DO_NOT_MATCH = new Result(false, "The machine does not match the specified IP allocation fields");
    private static final Result NO_ZONE_ID = new Result(false, "Host without zone data");

    private final SchedulerConfiguration configuration;
    private final TaskCache taskCache;
    private final AgentManagementService agentManagementService;

    public IpAllocationConstraint(SchedulerConfiguration configuration, TaskCache taskCache, AgentManagementService agentManagementService) {
        this.configuration = configuration;
        this.taskCache = taskCache;
        this.agentManagementService = agentManagementService;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        Optional<AgentInstance> instanceOpt = SchedulerUtils.findInstance(agentManagementService, configuration.getInstanceAttributeName(), targetVM);
        if (!instanceOpt.isPresent()) {
            return MACHINE_DOES_NOT_EXIST;
        }

        AgentInstance agentInstance = instanceOpt.get();
        List<IpAddressAllocation> ipAddressAllocations = ((V3QueueableTask)taskRequest).getJob().getJobDescriptor()
                .getContainer()
                .getContainerResources()
                .getIpAddressAllocations();
        return evaluate(agentInstance, ipAddressAllocations);
    }

    // Finds an unused IP allocation and matches its location (region, zone, subnet) to the agent instance
    private Result evaluate(AgentInstance agentInstance, List<IpAddressAllocation> ipAddressAllocations) {
        String instanceAvailabilityZone = agentInstance.getAttributes().getOrDefault(configuration.getAvailabilityZoneAttributeName(), "");


    }

    private IpAddressLocation getUnassignedIpAddressLocation(List<IpAddressAllocation> ipAddressAllocations) {
        //
    }
}
