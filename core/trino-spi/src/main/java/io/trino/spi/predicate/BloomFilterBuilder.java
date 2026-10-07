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

import io.trino.spi.block.ValueBlock;

/**
 * Collects values into a {@link BloomFilter}. Implementations are bound to a single
 * {@link io.trino.spi.type.Type} and are not thread safe.
 */
public interface BloomFilterBuilder
{
    /**
     * Adds a non-null value read from {@code block}.
     */
    void add(ValueBlock block, int position);

    /**
     * Adds a non-null value in its native representation.
     */
    void add(Object value);

    long getRetainedSizeInBytes();

    BloomFilter build();
}
