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

package com.netflix.titus.master.integration.v3.job;

import java.util.Collections;
import java.util.List;

import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobModel;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.model.job.vpc.IpAddress;
import com.netflix.titus.api.jobmanager.model.job.vpc.IpAddressAllocation;
import com.netflix.titus.api.jobmanager.model.job.vpc.IpAddressLocation;
import com.netflix.titus.api.jobmanager.model.job.vpc.SignedIpAddressAllocation;
import com.netflix.titus.api.model.EfsMount;
import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.grpc.protogen.TaskStatus.TaskState;
import com.netflix.titus.master.integration.BaseIntegrationTest;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.ScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.TaskScenarioBuilder;
import com.netflix.titus.runtime.jobmanager.JobManagerConfiguration;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import com.netflix.titus.testkit.model.job.ContainersGenerator;
import org.apache.mesos.Protos;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.appendJobDescriptorAttribute;
import static com.netflix.titus.master.integration.v3.scenario.JobAsserts.containerWithEfsMounts;
import static com.netflix.titus.master.integration.v3.scenario.JobAsserts.containerWithResources;
import static com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCells.basicCell;
import static com.netflix.titus.testkit.junit.master.TitusStackResource.V3_ENGINE_APP_PREFIX;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskServiceJobDescriptor;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

@Category(IntegrationTest.class)
public class JobSubmitAndControlBasicTest extends BaseIntegrationTest {

    private static final JobDescriptor<BatchJobExt> ONE_TASK_BATCH_JOB = oneTaskBatchJobDescriptor().toBuilder().withApplicationName(V3_ENGINE_APP_PREFIX).build();
    private static final JobDescriptor<ServiceJobExt> ONE_TASK_SERVICE_JOB = oneTaskServiceJobDescriptor().toBuilder().withApplicationName(V3_ENGINE_APP_PREFIX).build();

    private final TitusStackResource titusStackResource = new TitusStackResource(basicCell(2));

    private final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder(titusStackResource);

    private final InstanceGroupsScenarioBuilder instanceGroupsScenarioBuilder = new InstanceGroupsScenarioBuilder(titusStackResource);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(titusStackResource).around(instanceGroupsScenarioBuilder).around(jobsScenarioBuilder);

    private JobManagerConfiguration jobConfiguration;

    @Before
    public void setUp() throws Exception {
        instanceGroupsScenarioBuilder.synchronizeWithCloud().template(InstanceGroupScenarioTemplates.basicCloudActivation());
        this.jobConfiguration = titusStackResource.getGateway().getInstance(JobManagerConfiguration.class);
    }

    /**
     * Verify batch job submit with the expected state transitions. Verify agent receives proper resources.
     */
    @Test(timeout = 30_000)
    public void testSubmitSimpleBatchJobWhichEndsOk() throws Exception {
        jobsScenarioBuilder.schedule(ONE_TASK_BATCH_JOB, jobScenarioBuilder -> jobScenarioBuilder
                .inJob(job -> assertThat(job.getJobDescriptor()).isEqualTo(
                        appendJobDescriptorAttribute(ONE_TASK_BATCH_JOB, JobAttributes.JOB_ATTRIBUTES_CREATED_BY, "embeddedFederationClient"))
                )
                .template(ScenarioTemplates.startTasksInNewJob())
                .assertEachContainer(
                        containerWithResources(ONE_TASK_BATCH_JOB.getContainer().getContainerResources(), jobConfiguration.getMinDiskSizeMB()),
                        "Container not assigned the expected amount of resources"
                )
                .allTasks(ScenarioTemplates.completeTask())
                .template(ScenarioTemplates.jobFinished())
                .expectJobEventStreamCompletes()
        );
    }

    /**
     * Verify batch job submit with the expected state transitions. Verify agent receives proper EFS mount data.
     */
    @Test(timeout = 30_000)
    public void testSubmitBatchJobWithEfsMount() throws Exception {
        EfsMount efsMount1 = ContainersGenerator.efsMounts().getValue().toBuilder().withMountPoint("/data/logs").build();
        EfsMount efsMount2 = ContainersGenerator.efsMounts().skip(1).getValue().toBuilder().withMountPoint("/data").build();
        List<EfsMount> efsMounts = asList(efsMount1, efsMount2);
        List<EfsMount> expectedOrder = asList(efsMount2, efsMount1);

        JobDescriptor<BatchJobExt> jobWithEfs = ONE_TASK_BATCH_JOB.but(jd -> jd.getContainer().but(c -> c.getContainerResources().toBuilder().withEfsMounts(efsMounts)));
        jobsScenarioBuilder.schedule(jobWithEfs, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startTasksInNewJob())
                .assertEachContainer(
                        containerWithEfsMounts(expectedOrder),
                        "Container not assigned the expected EFS mount"
                )
                .allTasks(ScenarioTemplates.completeTask())
                .template(ScenarioTemplates.jobFinished())
                .expectJobEventStreamCompletes()
        );
    }

    @Test(timeout = 30_000)
    public void testSubmitSimpleBatchJobWhichFails() throws Exception {
        jobsScenarioBuilder.schedule(ONE_TASK_BATCH_JOB, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startTasksInNewJob())
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder.transitionTo(Protos.TaskState.TASK_FAILED))
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectStateUpdates(TaskState.Finished))
                .template(ScenarioTemplates.jobFinished())
                .expectJobEventStreamCompletes()
        );
    }

    // @Test(timeout = 30_000)
    @Test
    public void testSubmitSimpleBatchJobAndKillTask() throws Exception {
        JobDescriptor<BatchJobExt> retryableJob = ONE_TASK_BATCH_JOB.but(jd -> jd.getExtensions().toBuilder()
                .withRetryPolicy(JobModel.newImmediateRetryPolicy().withRetries(1).build())
        );
        jobsScenarioBuilder.schedule(retryableJob, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startTasksInNewJob())
                .allTasks(TaskScenarioBuilder::killTask)
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectStateUpdates(TaskState.KillInitiated, TaskState.Finished))
                .expectTaskInSlot(0, 1)
                .inTask(0, 1, TaskScenarioBuilder::killTask)
                .inTask(0, 1, taskScenarioBuilder -> taskScenarioBuilder
                        .expectStateUpdateSkipOther(TaskState.KillInitiated)
                        .expectStateUpdates(TaskState.Finished)
                )
                .template(ScenarioTemplates.jobFinished())
                .expectJobEventStreamCompletes()
        );
    }

    @Test(timeout = 30_000)
    public void testSubmitSimpleBatchJobAndKillIt() throws Exception {
        JobDescriptor<BatchJobExt> retryableJob = ONE_TASK_BATCH_JOB.but(jd -> jd.getExtensions().toBuilder()
                .withRetryPolicy(JobModel.newImmediateRetryPolicy().withRetries(3).build())
        );
        jobsScenarioBuilder.schedule(retryableJob, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startTasksInNewJob())
                .template(ScenarioTemplates.killJob())
        );
    }

    @Test(timeout = 30_000)
    public void testSubmitSimpleBatchJobWithNotRunningTaskAndKillIt() throws Exception {
        JobDescriptor<BatchJobExt> queuedJob = ONE_TASK_BATCH_JOB.but(jd -> jd.getContainer().but(
                c -> c.getContainerResources().toBuilder().withCpu(64) // Prevent it from being scheduled
        ));
        jobsScenarioBuilder.schedule(queuedJob, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.jobAccepted())
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectStateUpdateSkipOther(TaskState.Accepted))
                .template(ScenarioTemplates.killJob())
        );
    }

    @Test(timeout = 30_000)
    public void submitGpuBatchJob() throws Exception {
        JobDescriptor<BatchJobExt> gpuJobDescriptor =
                ONE_TASK_BATCH_JOB.but(j -> j.getContainer().but(c -> c.getContainerResources().toBuilder().withGpu(1)));

        jobsScenarioBuilder.schedule(gpuJobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startTasksInNewJob())
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectInstanceType(AwsInstanceType.G2_2XLarge))
        );
    }

    /**
     * Verify service job submit with the expected state transitions.
     */
    @Test(timeout = 30_000)
    public void testSubmitSimpleServiceJob() throws Exception {
        jobsScenarioBuilder.schedule(ONE_TASK_SERVICE_JOB, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startTasksInNewJob())
                .template(ScenarioTemplates.killJob())
        );
    }

    @Test(timeout = 30_000)
    public void testEnableDisableServiceJob() throws Exception {
        jobsScenarioBuilder.schedule(ONE_TASK_SERVICE_JOB, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.jobAccepted())
                .updateJobStatus(false)
                .updateJobStatus(true)
        );
    }

    @Test(timeout = 30_000)
    public void testAzConstraint() throws Exception {
        JobDescriptor<ServiceJobExt> azConstraintJob =
                ONE_TASK_SERVICE_JOB.but(j -> j.getContainer().but(c -> c.toBuilder().withHardConstraints(Collections.singletonMap("availabilityzone", "value"))));


        jobsScenarioBuilder.schedule(azConstraintJob, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.jobAccepted())
                .updateJobStatus(false)
                .updateJobStatus(true)
        );
    }

    // TODO(Andrew L): @Test(timeout = 30_000)
    @Test
    public void testIpAllocationConstraint() throws Exception {
        String ipAllocationZone = "zoneB";

        List<SignedIpAddressAllocation> signedIpAddressAllocations = Collections.singletonList(SignedIpAddressAllocation.newBuilder()
                .withIpAddressAllocationSignature(new byte[0])
                .withIpAddressAllocation(IpAddressAllocation.newBuilder()
                        .withIpAddress(IpAddress.newBuilder()
                                .withAddress("100.100.100.100")
                                .build())
                        .withUuid("867-5309")
                        .withIpAddressLocation(IpAddressLocation.newBuilder()
                                .withSubnetId("subnet-5")
                                .withRegion("us-east-1")
                                .withAvailabilityZone(ipAllocationZone)
                                .build())
                        .build())
                .build()
        );

        JobDescriptor<ServiceJobExt> ipJobDescriptor =
                ONE_TASK_SERVICE_JOB.but(j -> j.getContainer().but(c -> c.getContainerResources().toBuilder()
                        .withSignedIpAddressAllocations(signedIpAddressAllocations)));

        jobsScenarioBuilder.schedule(ipJobDescriptor, jobScenarioBuilder ->
                jobScenarioBuilder
                        .template(ScenarioTemplates.startTasksInNewJob())
                        .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectZoneId(ipAllocationZone)));
    }

    // TODO(Andrew L): @Test(timeout = 30_000)
    @Test
    public void testAlreadyAssignedIpAllocationConstraint() throws Exception {
        List<SignedIpAddressAllocation> signedIpAddressAllocations = Collections.singletonList(SignedIpAddressAllocation.newBuilder()
                .withIpAddressAllocationSignature(new byte[0])
                .withIpAddressAllocation(IpAddressAllocation.newBuilder()
                        .withIpAddress(IpAddress.newBuilder()
                                .withAddress("100.100.100.100")
                                .build())
                        .withUuid("867-5309")
                        .withIpAddressLocation(IpAddressLocation.newBuilder()
                                .withSubnetId("subnet-5")
                                .withRegion("us-east-1")
                                .withAvailabilityZone("zoneB")
                                .build())
                        .build())
                .build()
        );
        JobDescriptor<ServiceJobExt> firstIpJobDescriptor =
                ONE_TASK_SERVICE_JOB.but(j -> j.getContainer().but(c -> c.getContainerResources().toBuilder()
                        .withSignedIpAddressAllocations(signedIpAddressAllocations)));

        JobDescriptor<ServiceJobExt> secondIpJobDescriptor =
                firstIpJobDescriptor.but(j -> j.getJobGroupInfo().toBuilder().withSequence("v001"));


        jobsScenarioBuilder.schedule(firstIpJobDescriptor, jobScenarioBuilder ->
                jobScenarioBuilder
                        .template(ScenarioTemplates.startTasksInNewJob())
                        .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectUnsetTaskContext(TaskAttributes.TASK_ATTRIBUTES_WAITING_FOR_IN_USE_IP_ALLOCATION)));

        jobsScenarioBuilder.schedule(secondIpJobDescriptor, jobScenarioBuilder ->
                        jobScenarioBuilder
                                .template(ScenarioTemplates.jobAccepted())
                                .expectAllTasksCreated()
                                .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectStateUpdates(TaskState.Accepted))
                                .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectTaskContext(TaskAttributes.TASK_ATTRIBUTES_WAITING_FOR_IN_USE_IP_ALLOCATION)));

        jobsScenarioBuilder
                .takeJob(0)
                .template(ScenarioTemplates.killJob());

        jobsScenarioBuilder
                .takeJob(1)
                .template(ScenarioTemplates.startTasks());

        jobsScenarioBuilder
                .takeJob(1)
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectUnsetTaskContext(TaskAttributes.TASK_ATTRIBUTES_WAITING_FOR_IN_USE_IP_ALLOCATION));
    }
}
