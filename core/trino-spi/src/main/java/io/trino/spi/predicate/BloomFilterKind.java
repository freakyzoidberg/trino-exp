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
package io.trino.spi.predicate;

import io.trino.spi.type.Type;

/**
 * Identifies a bloom filter implementation.
 * <p>
 * A connector declares which kinds it understands through
 * {@link io.trino.spi.connector.Connector#getSupportedDynamicFilterBloomFilterKinds()}, so that the engine
 * collects dynamic filters using an implementation the consuming connector is able to read.
 */
public enum BloomFilterKind
{
    /**
     * Bloom filter of the <a href="https://datasketches.apache.org/">Apache DataSketches</a> library.
     */
    DATASKETCHES {
        @Override
        public boolean supportsType(Type type)
        {
            return DataSketchesBloomFilter.isSupportedType(type);
        }

        @Override
        public BloomFilterBuilder createBuilder(Type type, long expectedDistinctValues, double falsePositiveProbability)
        {
            return DataSketchesBloomFilter.createBuilder(type, expectedDistinctValues, falsePositiveProbability);
        }
    };

    /**
     * Returns true when values of {@code type} can be added to and queried in a filter of this kind.
     */
    public abstract boolean supportsType(Type type);

    /**
     * Creates a builder collecting values of {@code type}. Filters built by two builders of the same kind,
     * type and sizing can be merged with {@link BloomFilter#union(BloomFilter)}.
     */
    public abstract BloomFilterBuilder createBuilder(Type type, long expectedDistinctValues, double falsePositiveProbability);
}
