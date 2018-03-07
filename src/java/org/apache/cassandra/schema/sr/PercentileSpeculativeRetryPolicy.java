/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.schema.sr;

import java.math.BigDecimal;
import java.math.RoundingMode;

import com.google.common.base.Objects;

import com.codahale.metrics.Timer;

public class PercentileSpeculativeRetryPolicy implements SpeculativeRetryPolicy
{
    private final BigDecimal percentile;

    public PercentileSpeculativeRetryPolicy(BigDecimal percentile)
    {
        this.percentile = percentile.setScale(2, RoundingMode.UP);
    }

    @Override
    public boolean isDynamic()
    {
        return false;
    }

    @Override
    public long calculateThreshold(Timer readLatency)
    {
        return (long) (readLatency.getSnapshot().getValue(percentile.doubleValue() / 100) * 1000d);
    }

    @Override
    public Kind kind()
    {
        return Kind.PERCENTILE;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof PercentileSpeculativeRetryPolicy))
            return false;
        PercentileSpeculativeRetryPolicy rhs = (PercentileSpeculativeRetryPolicy) obj;
        return Objects.equal(percentile, rhs.percentile);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(kind(), percentile);
    }

    @Override
    public String toString()
    {
        return String.format("%.1fPERCENTILE", percentile.doubleValue() * 100);
    }
}
