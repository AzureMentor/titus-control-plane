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

import java.util.Objects;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

/**
 * IP Address to be assigned to task.
 */
public class IpAddress {

    private IpAddress(Builder builder) {
        family = builder.family;
        address = builder.address;
        prefixLength = builder.prefixLength;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public enum Family {DEFAULT, V4, V6};

    @NotNull(message = "'Family' is null")
    private final Family family;

    @Size(min = 1, message = "Emtpy value not allowed")
    private final String address;

    // TODO (Andrew L): Andrew some constraint check
    // Max of 32? Do we allocate blocks?
    private final int prefixLength;

    public Family getFamily() {
        return family;
    }

    public String getAddress() {
        return address;
    }

    public int getPrefixLength() {
        return prefixLength;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IpAddress ipAddress = (IpAddress) o;
        return prefixLength == ipAddress.prefixLength &&
                family == ipAddress.family &&
                Objects.equals(address, ipAddress.address);
    }

    @Override
    public int hashCode() {
        return Objects.hash(family, address, prefixLength);
    }

    public static final class Builder {
        private Family family;
        private String address;
        private int prefixLength;

        private Builder() {
        }

        public Builder withFamily(Family val) {
            family = val;
            return this;
        }

        public Builder withAddress(String val) {
            address = val;
            return this;
        }

        public Builder withPrefixLength(int val) {
            prefixLength = val;
            return this;
        }

        public Builder but() {
            return newBuilder()
                    .withFamily(family)
                    .withAddress(address)
                    .withPrefixLength(prefixLength);
        }

        public IpAddress build() {
            return new IpAddress(this);
        }
    }
}
