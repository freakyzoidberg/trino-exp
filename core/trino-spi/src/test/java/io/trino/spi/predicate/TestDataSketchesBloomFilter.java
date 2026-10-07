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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.predicate.BloomFilterKind.DATASKETCHES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToRawIntBits;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestDataSketchesBloomFilter
{
    private static final Type LONG_DECIMAL = createDecimalType(38, 0);

    private static final int VALUE_COUNT = 10_000;
    private static final double FALSE_POSITIVE_PROBABILITY = 0.01;

    @Test
    public void testSupportedTypes()
    {
        assertThat(DataSketchesBloomFilter.isSupportedType(BIGINT)).isTrue();
        assertThat(DataSketchesBloomFilter.isSupportedType(DOUBLE)).isTrue();
        assertThat(DataSketchesBloomFilter.isSupportedType(REAL)).isTrue();
        assertThat(DataSketchesBloomFilter.isSupportedType(BOOLEAN)).isTrue();
        assertThat(DataSketchesBloomFilter.isSupportedType(VARCHAR)).isTrue();
        // values are hashed with the type's HASH_CODE operator, so every comparable type is supported
        assertThat(DataSketchesBloomFilter.isSupportedType(LONG_DECIMAL)).isTrue();
        assertThat(DataSketchesBloomFilter.isSupportedType(HYPER_LOG_LOG)).isFalse();

        assertThatThrownBy(() -> DataSketchesBloomFilter.createBuilder(HYPER_LOG_LOG, 100, 0.1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not supported");
    }

    @Test
    public void testLongValues()
    {
        BloomFilterBuilder builder = createBuilder(BIGINT);
        for (long value = 0; value < VALUE_COUNT; value++) {
            builder.add(writeNativeValue(BIGINT, value), 0);
        }
        BloomFilter filter = builder.build();

        assertThat(filter.kind()).isEqualTo(DATASKETCHES);
        for (long value = 0; value < VALUE_COUNT; value++) {
            assertThat(filter.mightContain(value)).isTrue();
        }
        assertThat(falsePositiveRate(filter, VALUE_COUNT)).isLessThan(10 * FALSE_POSITIVE_PROBABILITY);
    }

    @Test
    public void testNativeValuesMatchBlockValues()
    {
        // the build side adds values read from a block while the degradation drains the collected values as
        // objects, so both must hash identically
        BloomFilterBuilder fromBlocks = createBuilder(BIGINT);
        BloomFilterBuilder fromNativeValues = createBuilder(BIGINT);
        for (long value = 0; value < 1000; value++) {
            fromBlocks.add(writeNativeValue(BIGINT, value), 0);
            fromNativeValues.add(value);
        }
        assertThat(fromBlocks.build()).isEqualTo(fromNativeValues.build());

        BloomFilterBuilder slicesFromBlocks = createBuilder(VARCHAR);
        BloomFilterBuilder slicesFromNativeValues = createBuilder(VARCHAR);
        for (int value = 0; value < 1000; value++) {
            slicesFromBlocks.add(writeNativeValue(VARCHAR, utf8Slice(value)), 0);
            slicesFromNativeValues.add(utf8Slice(value));
        }
        assertThat(slicesFromBlocks.build()).isEqualTo(slicesFromNativeValues.build());
    }

    @Test
    public void testVarcharValues()
    {
        BloomFilterBuilder builder = createBuilder(VARCHAR);
        for (int value = 0; value < VALUE_COUNT; value++) {
            builder.add(writeNativeValue(VARCHAR, utf8Slice(value)), 0);
        }
        BloomFilter filter = builder.build();

        for (int value = 0; value < VALUE_COUNT; value++) {
            assertThat(filter.mightContain(utf8Slice(value))).isTrue();
        }
        // a slice which is a view into a larger buffer must hash the same way as a standalone slice
        Slice embedded = Slices.utf8Slice("xxx" + "value 7" + "yyy").slice(3, "value 7".length());
        assertThat(filter.mightContain(embedded)).isTrue();

        int falsePositives = 0;
        for (int value = VALUE_COUNT; value < 2 * VALUE_COUNT; value++) {
            if (filter.mightContain(utf8Slice(value))) {
                falsePositives++;
            }
        }
        assertThat((double) falsePositives / VALUE_COUNT).isLessThan(10 * FALSE_POSITIVE_PROBABILITY);
    }

    @Test
    public void testFloatingPointValues()
    {
        BloomFilterBuilder doubleBuilder = createBuilder(DOUBLE);
        doubleBuilder.add(writeNativeValue(DOUBLE, 1.5), 0);
        doubleBuilder.add(-0.0);
        BloomFilter doubleFilter = doubleBuilder.build();
        assertThat(doubleFilter.mightContain(1.5)).isTrue();
        // a join matches -0.0 with 0.0, so the filter must accept both
        assertThat(doubleFilter.mightContain(0.0)).isTrue();
        assertThat(doubleFilter.mightContain(-0.0)).isTrue();

        BloomFilterBuilder realBuilder = createBuilder(REAL);
        realBuilder.add(writeNativeValue(REAL, (long) floatToRawIntBits(1.5f)), 0);
        realBuilder.add((long) floatToRawIntBits(-0.0f));
        BloomFilter realFilter = realBuilder.build();
        assertThat(realFilter.mightContain((long) floatToRawIntBits(1.5f))).isTrue();
        assertThat(realFilter.mightContain((long) floatToRawIntBits(0.0f))).isTrue();
        assertThat(realFilter.mightContain((long) floatToRawIntBits(-0.0f))).isTrue();
    }

    @Test
    public void testBooleanValues()
    {
        BloomFilterBuilder builder = createBuilder(BOOLEAN);
        builder.add(writeNativeValue(BOOLEAN, true), 0);
        BloomFilter filter = builder.build();

        assertThat(filter.mightContain(true)).isTrue();
    }

    @Test
    public void testUnion()
    {
        BloomFilter first = filterOfLongRange(0, VALUE_COUNT);
        BloomFilter second = filterOfLongRange(VALUE_COUNT, 2 * VALUE_COUNT);

        BloomFilter union = first.union(second).orElseThrow();
        for (long value = 0; value < 2 * VALUE_COUNT; value++) {
            assertThat(union.mightContain(value)).isTrue();
        }
        // union is commutative and does not modify its operands
        assertThat(second.union(first).orElseThrow()).isEqualTo(union);
        assertThat(first).isEqualTo(filterOfLongRange(0, VALUE_COUNT));
    }

    @Test
    public void testUnionOfIncompatibleFilters()
    {
        BloomFilter filter = filterOfLongRange(0, 10);
        BloomFilterBuilder differentlySized = DataSketchesBloomFilter.createBuilder(BIGINT, 10 * VALUE_COUNT, FALSE_POSITIVE_PROBABILITY);
        differentlySized.add(1L);

        assertThat(filter.union(differentlySized.build())).isEmpty();
    }

    @Test
    public void testAddAll()
    {
        BloomFilter filter = filterOfLongRange(0, 10);
        BloomFilter extended = filter.addAll(List.of(100L, 200L));

        assertThat(extended.mightContain(100L)).isTrue();
        assertThat(extended.mightContain(200L)).isTrue();
        assertThat(extended.mightContain(0L)).isTrue();
        // the original filter is not modified
        assertThat(filter).isEqualTo(filterOfLongRange(0, 10));

        assertThat(filter.addAll(List.of())).isSameAs(filter);
    }

    @Test
    public void testSerializationRoundTrip()
    {
        BloomFilter filter = filterOfLongRange(0, VALUE_COUNT);
        BloomFilter deserialized = DataSketchesBloomFilter.fromSerialized(BIGINT, filter.serialize().getBytes());

        assertThat(deserialized).isEqualTo(filter);
        assertThat(deserialized.hashCode()).isEqualTo(filter.hashCode());
        assertThat(deserialized.getRetainedSizeInBytes()).isGreaterThan(0);
        for (long value = 0; value < VALUE_COUNT; value++) {
            assertThat(deserialized.mightContain(value)).isTrue();
        }
    }

    @Test
    public void testRetainedSizeIsStable()
    {
        BloomFilter filter = filterOfLongRange(0, VALUE_COUNT);
        long retainedSize = filter.getRetainedSizeInBytes();

        // materializing the sketch and the serialized image must not change the reported size,
        // because dynamic filter accounting adds and subtracts it at different points in time
        assertThat(filter.mightContain(1L)).isTrue();
        assertThat(filter.serialize()).isNotNull();
        assertThat(filter.getRetainedSizeInBytes()).isEqualTo(retainedSize);
    }

    private static BloomFilter filterOfLongRange(long fromInclusive, long toExclusive)
    {
        BloomFilterBuilder builder = createBuilder(BIGINT);
        for (long value = fromInclusive; value < toExclusive; value++) {
            builder.add(value);
        }
        return builder.build();
    }

    private static BloomFilterBuilder createBuilder(Type type)
    {
        return DATASKETCHES.createBuilder(type, VALUE_COUNT, FALSE_POSITIVE_PROBABILITY);
    }

    private static double falsePositiveRate(BloomFilter filter, int absentValueCount)
    {
        int falsePositives = 0;
        for (long value = absentValueCount; value < 2L * absentValueCount; value++) {
            if (filter.mightContain(value)) {
                falsePositives++;
            }
        }
        return (double) falsePositives / absentValueCount;
    }

    private static Slice utf8Slice(int value)
    {
        return Slices.utf8Slice("value " + value);
    }
}
