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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.Type;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.VALUE_BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.predicate.BloomFilterKind.DATASKETCHES;
import static io.trino.spi.predicate.Utils.TUPLE_DOMAIN_TYPE_OPERATORS;
import static io.trino.spi.predicate.Utils.handleThrowable;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * {@link BloomFilter} backed by the Apache DataSketches
 * {@code org.apache.datasketches.filters.bloomfilter.BloomFilter} implementation.
 * <p>
 * Values are hashed with the {@code HASH_CODE} operator of their type, which the type guarantees to be
 * consistent with its {@code EQUAL} operator. The filter therefore accepts every value a join would match,
 * including values whose native representation differs from the one which was collected, such as
 * {@code -0.0} and {@code 0.0}.
 * <p>
 * Instances are immutable.
 */
public final class DataSketchesBloomFilter
        implements BloomFilter
{
    private static final int INSTANCE_SIZE = instanceSize(DataSketchesBloomFilter.class);

    /**
     * Fixed seed, so that filters collected independently by different drivers, tasks and nodes are
     * compatible and can be merged.
     */
    private static final long SEED = 0x5CA1AB1EL;

    private final Type type;
    private final org.apache.datasketches.filters.bloomfilter.BloomFilter sketch;
    private final long retainedSizeInBytes;

    private volatile byte[] lazySerialized;
    private volatile MethodHandle lazyHashNativeValue;
    private volatile MethodHandle lazyHashBlockValue;

    @JsonCreator
    public static DataSketchesBloomFilter fromSerialized(
            @JsonProperty("type") Type type,
            @JsonProperty("serialized") byte[] serialized)
    {
        requireNonNull(serialized, "serialized is null");
        return new DataSketchesBloomFilter(type, heapify(serialized));
    }

    private DataSketchesBloomFilter(Type type, org.apache.datasketches.filters.bloomfilter.BloomFilter sketch)
    {
        this.type = requireNonNull(type, "type is null");
        this.sketch = requireNonNull(sketch, "sketch is null");
        // the serialized image is materialized on demand and cached, so account for both representations
        this.retainedSizeInBytes = INSTANCE_SIZE + 2 * (sketch.getCapacity() / Byte.SIZE);
    }

    /**
     * Returns true when values of {@code type} can be hashed into a filter of this kind.
     */
    public static boolean isSupportedType(Type type)
    {
        return type.isComparable();
    }

    public static BloomFilterBuilder createBuilder(Type type, long expectedDistinctValues, double falsePositiveProbability)
    {
        if (!isSupportedType(type)) {
            throw new IllegalArgumentException("Type is not supported by DataSketches bloom filters: " + type);
        }
        return new Builder(
                type,
                org.apache.datasketches.filters.bloomfilter.BloomFilterBuilder.createByAccuracy(expectedDistinctValues, falsePositiveProbability, SEED));
    }

    @Override
    public BloomFilterKind kind()
    {
        return DATASKETCHES;
    }

    @Override
    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @Override
    public boolean mightContain(Object value)
    {
        return sketch.query(hash(hashNativeValue(), value));
    }

    @Override
    public boolean mightContain(ValueBlock block, int position)
    {
        long hash;
        try {
            hash = (long) hashBlockValue().invokeExact(block, position);
        }
        catch (Throwable throwable) {
            throw handleThrowable(throwable);
        }
        return sketch.query(hash);
    }

    @Override
    public Optional<BloomFilter> union(BloomFilter other)
    {
        if (!(other instanceof DataSketchesBloomFilter otherFilter) || !type.equals(otherFilter.type)) {
            return Optional.empty();
        }
        if (!sketch.isCompatible(otherFilter.sketch)) {
            return Optional.empty();
        }
        org.apache.datasketches.filters.bloomfilter.BloomFilter merged = heapify(serializedBytes());
        merged.union(otherFilter.sketch);
        return Optional.of(new DataSketchesBloomFilter(type, merged));
    }

    @Override
    public BloomFilter addAll(Collection<Object> values)
    {
        if (values.isEmpty()) {
            return this;
        }
        org.apache.datasketches.filters.bloomfilter.BloomFilter updated = heapify(serializedBytes());
        MethodHandle hashNativeValue = hashNativeValue();
        for (Object value : values) {
            updated.update(hash(hashNativeValue, value));
        }
        return new DataSketchesBloomFilter(type, updated);
    }

    @Override
    public Slice serialize()
    {
        return Slices.wrappedBuffer(serializedBytes());
    }

    @JsonProperty("serialized")
    public byte[] serializedBytes()
    {
        byte[] serialized = lazySerialized;
        if (serialized == null) {
            serialized = sketch.toByteArray();
            lazySerialized = serialized;
        }
        return serialized;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if (!(other instanceof DataSketchesBloomFilter otherFilter)) {
            return false;
        }
        return type.equals(otherFilter.type) && Arrays.equals(serializedBytes(), otherFilter.serializedBytes());
    }

    @Override
    public int hashCode()
    {
        // the serialized image starts with a header describing the sizing and holds the set bits in a stable order
        byte[] serialized = serializedBytes();
        return 31 * type.hashCode() + Arrays.hashCode(Arrays.copyOf(serialized, Math.min(serialized.length, 512)));
    }

    @Override
    public String toString()
    {
        return format("datasketches bloom filter of %s bits, %.1f%% full", sketch.getCapacity(), sketch.getFillPercentage() * 100);
    }

    private MethodHandle hashBlockValue()
    {
        MethodHandle hashBlockValue = lazyHashBlockValue;
        if (hashBlockValue == null) {
            hashBlockValue = TUPLE_DOMAIN_TYPE_OPERATORS.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, VALUE_BLOCK_POSITION_NOT_NULL));
            lazyHashBlockValue = hashBlockValue;
        }
        return hashBlockValue;
    }

    private MethodHandle hashNativeValue()
    {
        MethodHandle hashNativeValue = lazyHashNativeValue;
        if (hashNativeValue == null) {
            hashNativeValue = TUPLE_DOMAIN_TYPE_OPERATORS.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL))
                    .asType(java.lang.invoke.MethodType.methodType(long.class, Object.class));
            lazyHashNativeValue = hashNativeValue;
        }
        return hashNativeValue;
    }

    private static long hash(MethodHandle hashNativeValue, Object value)
    {
        try {
            return (long) hashNativeValue.invokeExact(value);
        }
        catch (Throwable throwable) {
            throw handleThrowable(throwable);
        }
    }

    private static org.apache.datasketches.filters.bloomfilter.BloomFilter heapify(byte[] serialized)
    {
        return org.apache.datasketches.filters.bloomfilter.BloomFilter.heapify(MemorySegment.ofArray(serialized));
    }

    private static final class Builder
            implements BloomFilterBuilder
    {
        private static final int INSTANCE_SIZE = instanceSize(Builder.class);

        private final Type type;
        private final org.apache.datasketches.filters.bloomfilter.BloomFilter sketch;
        private final MethodHandle hashBlockValue;
        private final MethodHandle hashNativeValue;
        private final long retainedSizeInBytes;

        private Builder(Type type, org.apache.datasketches.filters.bloomfilter.BloomFilter sketch)
        {
            this.type = requireNonNull(type, "type is null");
            this.sketch = requireNonNull(sketch, "sketch is null");
            this.hashBlockValue = TUPLE_DOMAIN_TYPE_OPERATORS.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, VALUE_BLOCK_POSITION_NOT_NULL));
            this.hashNativeValue = TUPLE_DOMAIN_TYPE_OPERATORS.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL))
                    .asType(java.lang.invoke.MethodType.methodType(long.class, Object.class));
            this.retainedSizeInBytes = INSTANCE_SIZE + sketch.getCapacity() / Byte.SIZE;
        }

        @Override
        public void add(ValueBlock block, int position)
        {
            long hash;
            try {
                hash = (long) hashBlockValue.invokeExact(block, position);
            }
            catch (Throwable throwable) {
                throw handleThrowable(throwable);
            }
            sketch.update(hash);
        }

        @Override
        public void add(Object value)
        {
            sketch.update(hash(hashNativeValue, value));
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return retainedSizeInBytes;
        }

        @Override
        public BloomFilter build()
        {
            // copy, so that the returned filter is not affected by further updates of this builder
            return new DataSketchesBloomFilter(type, heapify(sketch.toByteArray()));
        }
    }
}
