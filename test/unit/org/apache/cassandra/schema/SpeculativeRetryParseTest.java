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

package org.apache.cassandra.schema;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.sr.AlwaysSpeculativeRetryPolicy;
import org.apache.cassandra.schema.sr.FixedSpeculativeRetryPolicy;
import org.apache.cassandra.schema.sr.HybridSpeculativeRetryPolicy;
import org.apache.cassandra.schema.sr.NeverSpeculativeRetryPolicy;
import org.apache.cassandra.schema.sr.PercentileSpeculativeRetryPolicy;
import org.apache.cassandra.schema.sr.SpeculativeRetryPolicy;

import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.Parameters;

import static org.apache.cassandra.schema.sr.HybridSpeculativeRetryPolicy.Function;

@RunWith(Enclosed.class)
public class SpeculativeRetryParseTest
{

    @RunWith(Parameterized.class)
    public static class SuccessfulParseTest
    {
        private final String string;
        private final SpeculativeRetryPolicy expectedValue;

        public SuccessfulParseTest(String string, SpeculativeRetryPolicy expectedValue)
        {
            this.string = string;
            this.expectedValue = expectedValue;
        }

        @Parameters
        public static Collection<Object[]> generateData()
        {
            return Arrays.asList(new Object[][]{
                                 { "NONE", NeverSpeculativeRetryPolicy.INSTANCE },
                                 { "NEVER", NeverSpeculativeRetryPolicy.INSTANCE },
                                 { "ALWAYS", AlwaysSpeculativeRetryPolicy.INSTANCE },
                                 { "10PERCENTILE", new PercentileSpeculativeRetryPolicy(new BigDecimal(0.10)) },
                                 { "121.1ms", new FixedSpeculativeRetryPolicy(121.1) },
                                 { "21.7MS", new FixedSpeculativeRetryPolicy(21.7) },
                                 { "None", NeverSpeculativeRetryPolicy.INSTANCE },
                                 { "Never", NeverSpeculativeRetryPolicy.INSTANCE },
                                 { "Always", AlwaysSpeculativeRetryPolicy.INSTANCE },
                                 { "21.1percentile", new PercentileSpeculativeRetryPolicy(new BigDecimal(0.211)) },
                                 { "78.11p", new PercentileSpeculativeRetryPolicy(new BigDecimal(0.7811)) },
                                 { "max(99p,53ms)", new HybridSpeculativeRetryPolicy(new PercentileSpeculativeRetryPolicy(new BigDecimal(0.99)),
                                                                                     new FixedSpeculativeRetryPolicy(53),
                                                                                     Function.MAX) },
                                 { "max(53ms,99p)", new HybridSpeculativeRetryPolicy(new PercentileSpeculativeRetryPolicy(new BigDecimal(0.99)),
                                                                                     new FixedSpeculativeRetryPolicy(53),
                                                                                     Function.MAX) },
                                 { "MIN(70MS,90PERCENTILE)", new HybridSpeculativeRetryPolicy(new PercentileSpeculativeRetryPolicy(new BigDecimal(0.90)),
                                                                                              new FixedSpeculativeRetryPolicy(70),
                                                                                              Function.MIN) },
                                 { "MIN(70MS,  90PERCENTILE)", new HybridSpeculativeRetryPolicy(new PercentileSpeculativeRetryPolicy(new BigDecimal(0.90)),
                                                                                                new FixedSpeculativeRetryPolicy(70),
                                                                                                Function.MIN) }
                                 }
            );
        }

        @Test
        public void testParameterParse()
        {
            assertEquals(expectedValue, SpeculativeRetryPolicy.fromString(string));
        }

        @Test
        public void testToStringRoundTrip()
        {
            assertEquals(expectedValue, SpeculativeRetryPolicy.fromString(expectedValue.toString()));
        }
    }

    @RunWith(Parameterized.class)
    public static class FailedParseTest
    {
        private final String string;

        public FailedParseTest(String string)
        {
            this.string = string;
        }

        @Parameters
        public static Collection<Object[]> generateData()
        {
            return Arrays.asList(new Object[][]{
                                 { "" },
                                 { "-0.1PERCENTILE" },
                                 { "100.1PERCENTILE" },
                                 { "xPERCENTILE" },
                                 { "xyzms" },
                                 { "X" }
                                 }
            );
        }

        @Test(expected = ConfigurationException.class)
        public void testParameterParse()
        {
            SpeculativeRetryPolicy.fromString(string);
        }
    }
}