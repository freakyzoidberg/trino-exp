/*
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
package io.trino.operator;

import io.trino.spi.predicate.BloomFilterBuilder;
import io.trino.spi.predicate.BloomFilterKind;
import io.trino.spi.type.Type;

import static java.util.Objects.requireNonNull;

/**
 * How a dynamic filter which exceeded its distinct value limit collects a bloom filter.
 * <p>
 * The sizing is the same for every driver, task and node collecting the filter, so that the collected
 * filters are compatible and can be merged.
 *
 * @param kind implementation the connectors consuming the dynamic filter are able to read
 */
public record BloomFilterOptions(BloomFilterKind kind, long expectedDistinctValues, double falsePositiveProbability)
{
    public BloomFilterOptions
    {
        requireNonNull(kind, "kind is null");
        if (expectedDistinctValues < 1) {
            throw new IllegalArgumentException("expectedDistinctValues must be at least 1: " + expectedDistinctValues);
        }
        if (!(falsePositiveProbability > 0) || !(falsePositiveProbability < 1)) {
            throw new IllegalArgumentException("falsePositiveProbability must be in the range (0, 1): " + falsePositiveProbability);
        }
    }

    public boolean supportsType(Type type)
    {
        return kind.supportsType(type);
    }

    public BloomFilterBuilder createBuilder(Type type)
    {
        return kind.createBuilder(type, expectedDistinctValues, falsePositiveProbability);
    }
}
