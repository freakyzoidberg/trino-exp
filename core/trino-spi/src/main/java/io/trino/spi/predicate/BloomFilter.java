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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.airlift.slice.Slice;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.Type;

import java.util.Collection;
import java.util.Optional;

/**
 * Approximate membership filter over the non-null values of a single column, carried by a
 * {@link Domain} in addition to its {@link ValueSet}.
 * <p>
 * A bloom filter never reports a false negative, so {@link #mightContain} returning {@code false} proves the
 * value is absent, while {@code true} only means that it may be present. A filter is therefore a superset of
 * the values it was built from, which makes it safe to use for filtering in the same way a min/max range is.
 * <p>
 * Consumers which do not understand the {@link #kind()} of a filter must ignore it and rely on the
 * {@link Domain#getValues()} of the domain instead.
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = DataSketchesBloomFilter.class, name = "datasketches"),
})
public interface BloomFilter
{
    BloomFilterKind kind();

    /**
     * The type of the values this filter was built from. Values of any other type cannot be queried.
     */
    Type getType();

    /**
     * @param value native representation of a non-null value of {@link #getType()}
     * @return false when {@code value} is certainly not in the filter
     */
    boolean mightContain(Object value);

    /**
     * Equivalent to {@link #mightContain(Object)} for the non-null value at {@code position}, without
     * materializing it.
     */
    boolean mightContain(ValueBlock block, int position);

    /**
     * Merges {@code other} into a new filter, or returns empty when the filters cannot be merged because
     * they are of a different kind or type, or were built with a different sizing.
     */
    Optional<BloomFilter> union(BloomFilter other);

    /**
     * Returns a new filter which additionally contains {@code values}.
     *
     * @param values native representations of non-null values of {@link #getType()}
     */
    BloomFilter addAll(Collection<Object> values);

    /**
     * Serialized form of this filter, in the format defined by its {@link #kind()}.
     */
    Slice serialize();

    long getRetainedSizeInBytes();
}
