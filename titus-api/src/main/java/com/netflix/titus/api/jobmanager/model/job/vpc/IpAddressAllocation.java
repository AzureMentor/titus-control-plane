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

package com.netflix.titus.api.jobmanager.model.job.vpc;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

/**
 * VPC IP address allocated from Titus IP Service
 */
public class IpAddressAllocation {

    @NotNull(message = "'ipAddressLocation' is null")
    private final IpAddressLocation ipAddressLocation;

    @Size(min = 1, message = "Emtpy value not allowed")
    private final String allocationId;

    @NotNull(message = "'ipAddress' is null")
    private final IpAddress ipAddress;

    private IpAddressAllocation(Builder builder) {
        ipAddressLocation = builder.ipAddressLocation;
        allocationId = builder.allocationId;
        ipAddress = builder.ipAddress;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public IpAddressLocation getIpAddressLocation() {
        return ipAddressLocation;
    }

    public String getAllocationId() {
        return allocationId;
    }

    public IpAddress getIpAddress() {
        return ipAddress;
    }


    public static final class Builder {
        private IpAddressLocation ipAddressLocation;
        private String allocationId;
        private IpAddress ipAddress;

        private Builder() {
        }

        public Builder withIpAddressLocation(IpAddressLocation val) {
            ipAddressLocation = val;
            return this;
        }

        public Builder withUuid(String val) {
            allocationId = val;
            return this;
        }

        public Builder withIpAddress(IpAddress val) {
            ipAddress = val;
            return this;
        }

        public Builder but() {
            return newBuilder()
                    .withIpAddressLocation(ipAddressLocation)
                    .withUuid(allocationId)
                    .withIpAddress(ipAddress);
        }

        public IpAddressAllocation build() {
            return new IpAddressAllocation(this);
        }
    }
}
