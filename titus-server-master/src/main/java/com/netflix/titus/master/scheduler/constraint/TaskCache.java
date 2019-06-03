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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.util.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class that aggregates task data by multiple criteria used by Fenzo constraint/fitness evaluators.
 */
@Singleton
public class TaskCache {
    private static final Logger logger = LoggerFactory.getLogger(TaskCache.class);

    private final V3JobOperations v3JobOperations;
    private final AtomicReference<TaskCacheValue> currentCacheValue;

    @Inject
    public TaskCache(V3JobOperations v3JobOperations) {
        this.v3JobOperations = v3JobOperations;
        this.currentCacheValue = new AtomicReference<>();
    }

    public void prepare() {
        currentCacheValue.set(new TaskCacheValue());
    }

    public Map<String, Integer> getTasksByZoneIdCounters(String jobId) {
        return currentCacheValue.get().getTasksByZoneIdCounters(jobId);
    }

    private class TaskCacheValue {

        private final Map<String, Map<String, Integer>> zoneBalanceCountersByJobId;
        // Map<Job ID, Map<Task ID, Allocation ID>>
        // private final Map<String, Map<String, String>> ipAddressAllocationsByJobId;

        private TaskCacheValue() {
            List<Pair<Job, List<Task>>> jobsAndTasks = v3JobOperations.getJobsAndTasks();
            this.zoneBalanceCountersByJobId = buildZoneBalanceCountersByJobId(jobsAndTasks);
        }

        private Map<String, Integer> getTasksByZoneIdCounters(String jobId) {
            return zoneBalanceCountersByJobId.getOrDefault(jobId, Collections.emptyMap());
        }

        private Map<String, Map<String, Integer>> buildZoneBalanceCountersByJobId(List<Pair<Job, List<Task>>> jobsAndTasks) {
            Map<String, Map<String, Integer>> result = new HashMap<>();
            for (Pair<Job, List<Task>> jobAndTask : jobsAndTasks) {
                logger.error("Looking at zone balancer for job {}", jobAndTask.getLeft());
                Map<String, Integer> jobZoneBalancing = new HashMap<>();
                for (Task task : jobAndTask.getRight()) {
                    String ipAllocationId = getIpAllocationId(task);
                    if (ipAllocationId != null) {
                        logger.error("Found existing IP allocation with task {} using {}", task.getId(), ipAllocationId);
                    }

                    logger.error("Looking at zone balance for task {}", task);
                    String zoneId = getZoneId(task);
                    if (zoneId != null) {
                        jobZoneBalancing.put(zoneId, jobZoneBalancing.getOrDefault(zoneId, 0) + 1);
                    }
                }
                result.put(jobAndTask.getLeft().getId(), jobZoneBalancing);
            }
            return result;
        }

        private String getZoneId(Task task) {
            return task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_AGENT_ZONE);
        }

        private String getIpAllocationId(Task task) {
            return task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_IP_ALLOCATION_ID);
        }
    }
}