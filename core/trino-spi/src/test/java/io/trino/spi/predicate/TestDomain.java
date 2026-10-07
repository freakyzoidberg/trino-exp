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

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonMapperProvider;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.block.TestingBlockJsonSerde;
import io.trino.spi.type.TestingTypeDeserializer;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TestingIdType.ID;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.floatToRawIntBits;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestDomain
{
    @Test
    public void testOrderableNone()
    {
        Domain domain = Domain.none(BIGINT);
        assertThat(domain.isNone()).isTrue();
        assertThat(domain.isAll()).isFalse();
        assertThat(domain.isSingleValue()).isFalse();
        assertThat(domain.isNullableSingleValue()).isFalse();
        assertThat(domain.isNullAllowed()).isFalse();
        assertThat(domain.getValues()).isEqualTo(ValueSet.none(BIGINT));
        assertThat(domain.getType()).isEqualTo(BIGINT);
        assertThat(domain.includesNullableValue(Long.MIN_VALUE)).isFalse();
        assertThat(domain.includesNullableValue(0L)).isFalse();
        assertThat(domain.includesNullableValue(Long.MAX_VALUE)).isFalse();
        assertThat(domain.includesNullableValue(null)).isFalse();
        assertThat(domain.complement()).isEqualTo(Domain.all(BIGINT));
        assertThat(domain.toString()).isEqualTo("NONE");
    }

    @Test
    public void testEquatableNone()
    {
        Domain domain = Domain.none(ID);
        assertThat(domain.isNone()).isTrue();
        assertThat(domain.isAll()).isFalse();
        assertThat(domain.isSingleValue()).isFalse();
        assertThat(domain.isNullableSingleValue()).isFalse();
        assertThat(domain.isNullAllowed()).isFalse();
        assertThat(domain.getValues()).isEqualTo(ValueSet.none(ID));
        assertThat(domain.getType()).isEqualTo(ID);
        assertThat(domain.includesNullableValue(0L)).isFalse();
        assertThat(domain.includesNullableValue(null)).isFalse();
        assertThat(domain.complement()).isEqualTo(Domain.all(ID));
        assertThat(domain.toString()).isEqualTo("NONE");
    }

    @Test
    public void testUncomparableNone()
    {
        Domain domain = Domain.none(HYPER_LOG_LOG);
        assertThat(domain.isNone()).isTrue();
        assertThat(domain.isAll()).isFalse();
        assertThat(domain.isSingleValue()).isFalse();
        assertThat(domain.isNullableSingleValue()).isFalse();
        assertThat(domain.isNullAllowed()).isFalse();
        assertThat(domain.getValues()).isEqualTo(ValueSet.none(HYPER_LOG_LOG));
        assertThat(domain.getType()).isEqualTo(HYPER_LOG_LOG);
        assertThat(domain.includesNullableValue(Slices.EMPTY_SLICE)).isFalse();
        assertThat(domain.includesNullableValue(null)).isFalse();
        assertThat(domain.complement()).isEqualTo(Domain.all(HYPER_LOG_LOG));
        assertThat(domain.toString()).isEqualTo("NONE");
    }

    @Test
    public void testOrderableAll()
    {
        Domain domain = Domain.all(BIGINT);
        assertThat(domain.isNone()).isFalse();
        assertThat(domain.isAll()).isTrue();
        assertThat(domain.isSingleValue()).isFalse();
        assertThat(domain.isNullableSingleValue()).isFalse();
        assertThat(domain.isOnlyNull()).isFalse();
        assertThat(domain.isNullAllowed()).isTrue();
        assertThat(domain.getValues()).isEqualTo(ValueSet.all(BIGINT));
        assertThat(domain.getType()).isEqualTo(BIGINT);
        assertThat(domain.includesNullableValue(Long.MIN_VALUE)).isTrue();
        assertThat(domain.includesNullableValue(0L)).isTrue();
        assertThat(domain.includesNullableValue(Long.MAX_VALUE)).isTrue();
        assertThat(domain.includesNullableValue(null)).isTrue();
        assertThat(domain.complement()).isEqualTo(Domain.none(BIGINT));
        assertThat(domain.toString()).isEqualTo("ALL");
    }

    @Test
    public void testFloatingPointOrderableAll()
    {
        Domain domain = Domain.all(REAL);
        assertThat(domain.isNone()).isFalse();
        assertThat(domain.isAll()).isTrue();
        assertThat(domain.isSingleValue()).isFalse();
        assertThat(domain.isNullableSingleValue()).isFalse();
        assertThat(domain.isOnlyNull()).isFalse();
        assertThat(domain.isNullAllowed()).isTrue();
        assertThat(domain.getValues()).isEqualTo(ValueSet.all(REAL));
        assertThat(domain.getType()).isEqualTo(REAL);
        assertThat(domain.includesNullableValue((long) floatToRawIntBits(-Float.MAX_VALUE))).isTrue();
        assertThat(domain.includesNullableValue((long) floatToRawIntBits(0.0f))).isTrue();
        assertThat(domain.includesNullableValue((long) floatToRawIntBits(Float.MAX_VALUE))).isTrue();
        assertThat(domain.includesNullableValue((long) floatToRawIntBits(Float.MIN_VALUE))).isTrue();
        assertThat(domain.includesNullableValue(null)).isTrue();
        assertThat(domain.includesNullableValue((long) floatToRawIntBits(Float.NaN))).isTrue();
        assertThat(domain.includesNullableValue((long) 0x7FC01234)).isTrue(); // different NaN representation
        assertThat(domain.complement()).isEqualTo(Domain.none(REAL));
        assertThat(domain.toString()).isEqualTo("ALL");

        domain = Domain.all(DOUBLE);
        assertThat(domain.isNone()).isFalse();
        assertThat(domain.isAll()).isTrue();
        assertThat(domain.isSingleValue()).isFalse();
        assertThat(domain.isNullableSingleValue()).isFalse();
        assertThat(domain.isOnlyNull()).isFalse();
        assertThat(domain.isNullAllowed()).isTrue();
        assertThat(domain.getValues()).isEqualTo(ValueSet.all(DOUBLE));
        assertThat(domain.getType()).isEqualTo(DOUBLE);
        assertThat(domain.includesNullableValue(-Double.MAX_VALUE)).isTrue();
        assertThat(domain.includesNullableValue(0.0)).isTrue();
        assertThat(domain.includesNullableValue(Double.MAX_VALUE)).isTrue();
        assertThat(domain.includesNullableValue(Double.MIN_VALUE)).isTrue();
        assertThat(domain.includesNullableValue(null)).isTrue();
        assertThat(domain.includesNullableValue(Double.NaN)).isTrue();
        assertThat(domain.includesNullableValue(longBitsToDouble(0x7FF8123412341234L))).isTrue(); // different NaN representation
        assertThat(domain.complement()).isEqualTo(Domain.none(DOUBLE));
        assertThat(domain.toString()).isEqualTo("ALL");
    }

    @Test
    public void testEquatableAll()
    {
        Domain domain = Domain.all(ID);
        assertThat(domain.isNone()).isFalse();
        assertThat(domain.isAll()).isTrue();
        assertThat(domain.isSingleValue()).isFalse();
        assertThat(domain.isNullableSingleValue()).isFalse();
        assertThat(domain.isOnlyNull()).isFalse();
        assertThat(domain.isNullAllowed()).isTrue();
        assertThat(domain.getValues()).isEqualTo(ValueSet.all(ID));
        assertThat(domain.getType()).isEqualTo(ID);
        assertThat(domain.includesNullableValue(0L)).isTrue();
        assertThat(domain.includesNullableValue(null)).isTrue();
        assertThat(domain.complement()).isEqualTo(Domain.none(ID));
        assertThat(domain.toString()).isEqualTo("ALL");
    }

    @Test
    public void testUncomparableAll()
    {
        Domain domain = Domain.all(HYPER_LOG_LOG);
        assertThat(domain.isNone()).isFalse();
        assertThat(domain.isAll()).isTrue();
        assertThat(domain.isSingleValue()).isFalse();
        assertThat(domain.isNullableSingleValue()).isFalse();
        assertThat(domain.isOnlyNull()).isFalse();
        assertThat(domain.isNullAllowed()).isTrue();
        assertThat(domain.getValues()).isEqualTo(ValueSet.all(HYPER_LOG_LOG));
        assertThat(domain.getType()).isEqualTo(HYPER_LOG_LOG);
        assertThat(domain.includesNullableValue(Slices.EMPTY_SLICE)).isTrue();
        assertThat(domain.includesNullableValue(null)).isTrue();
        assertThat(domain.complement()).isEqualTo(Domain.none(HYPER_LOG_LOG));
        assertThat(domain.toString()).isEqualTo("ALL");
    }

    @Test
    public void testOrderableNullOnly()
    {
        Domain domain = Domain.onlyNull(BIGINT);
        assertThat(domain.isNone()).isFalse();
        assertThat(domain.isAll()).isFalse();
        assertThat(domain.isSingleValue()).isFalse();
        assertThat(domain.isNullAllowed()).isTrue();
        assertThat(domain.isNullableSingleValue()).isTrue();
        assertThat(domain.isOnlyNull()).isTrue();
        assertThat(domain.getValues()).isEqualTo(ValueSet.none(BIGINT));
        assertThat(domain.getType()).isEqualTo(BIGINT);
        assertThat(domain.includesNullableValue(Long.MIN_VALUE)).isFalse();
        assertThat(domain.includesNullableValue(0L)).isFalse();
        assertThat(domain.includesNullableValue(Long.MAX_VALUE)).isFalse();
        assertThat(domain.includesNullableValue(null)).isTrue();
        assertThat(domain.complement()).isEqualTo(Domain.notNull(BIGINT));
        assertThat(domain.getNullableSingleValue()).isEqualTo(null);
        assertThat(domain.toString()).isEqualTo("[NULL]");
    }

    @Test
    public void testEquatableNullOnly()
    {
        Domain domain = Domain.onlyNull(ID);
        assertThat(domain.isNone()).isFalse();
        assertThat(domain.isAll()).isFalse();
        assertThat(domain.isSingleValue()).isFalse();
        assertThat(domain.isNullableSingleValue()).isTrue();
        assertThat(domain.isOnlyNull()).isTrue();
        assertThat(domain.isNullAllowed()).isTrue();
        assertThat(domain.getValues()).isEqualTo(ValueSet.none(ID));
        assertThat(domain.getType()).isEqualTo(ID);
        assertThat(domain.includesNullableValue(0L)).isFalse();
        assertThat(domain.includesNullableValue(null)).isTrue();
        assertThat(domain.complement()).isEqualTo(Domain.notNull(ID));
        assertThat(domain.getNullableSingleValue()).isEqualTo(null);
        assertThat(domain.toString()).isEqualTo("[NULL]");
    }

    @Test
    public void testUncomparableNullOnly()
    {
        Domain domain = Domain.onlyNull(HYPER_LOG_LOG);
        assertThat(domain.isNone()).isFalse();
        assertThat(domain.isAll()).isFalse();
        assertThat(domain.isSingleValue()).isFalse();
        assertThat(domain.isNullableSingleValue()).isTrue();
        assertThat(domain.isOnlyNull()).isTrue();
        assertThat(domain.isNullAllowed()).isTrue();
        assertThat(domain.getValues()).isEqualTo(ValueSet.none(HYPER_LOG_LOG));
        assertThat(domain.getType()).isEqualTo(HYPER_LOG_LOG);
        assertThat(domain.includesNullableValue(Slices.EMPTY_SLICE)).isFalse();
        assertThat(domain.includesNullableValue(null)).isTrue();
        assertThat(domain.complement()).isEqualTo(Domain.notNull(HYPER_LOG_LOG));
        assertThat(domain.getNullableSingleValue()).isEqualTo(null);
        assertThat(domain.toString()).isEqualTo("[NULL]");
    }

    @Test
    public void testOrderableNotNull()
    {
        Domain domain = Domain.notNull(BIGINT);
        assertThat(domain.isNone()).isFalse();
        assertThat(domain.isAll()).isFalse();
        assertThat(domain.isSingleValue()).isFalse();
        assertThat(domain.isNullableSingleValue()).isFalse();
        assertThat(domain.isOnlyNull()).isFalse();
        assertThat(domain.isNullAllowed()).isFalse();
        assertThat(domain.getValues()).isEqualTo(ValueSet.all(BIGINT));
        assertThat(domain.getType()).isEqualTo(BIGINT);
        assertThat(domain.includesNullableValue(Long.MIN_VALUE)).isTrue();
        assertThat(domain.includesNullableValue(0L)).isTrue();
        assertThat(domain.includesNullableValue(Long.MAX_VALUE)).isTrue();
        assertThat(domain.includesNullableValue(null)).isFalse();
        assertThat(domain.complement()).isEqualTo(Domain.onlyNull(BIGINT));
        assertThat(domain.toString()).isEqualTo("[ SortedRangeSet[type=bigint, ranges=1, {(<min>,<max>)}] ]");
    }

    @Test
    public void testEquatableNotNull()
    {
        Domain domain = Domain.notNull(ID);
        assertThat(domain.isNone()).isFalse();
        assertThat(domain.isAll()).isFalse();
        assertThat(domain.isSingleValue()).isFalse();
        assertThat(domain.isNullableSingleValue()).isFalse();
        assertThat(domain.isOnlyNull()).isFalse();
        assertThat(domain.isNullAllowed()).isFalse();
        assertThat(domain.getValues()).isEqualTo(ValueSet.all(ID));
        assertThat(domain.getType()).isEqualTo(ID);
        assertThat(domain.includesNullableValue(0L)).isTrue();
        assertThat(domain.includesNullableValue(null)).isFalse();
        assertThat(domain.complement()).isEqualTo(Domain.onlyNull(ID));
        assertThat(domain.toString()).isEqualTo("[ EquatableValueSet[type=id, values=0, EXCLUDES{}] ]");
    }

    @Test
    public void testUncomparableNotNull()
    {
        Domain domain = Domain.notNull(HYPER_LOG_LOG);
        assertThat(domain.isNone()).isFalse();
        assertThat(domain.isAll()).isFalse();
        assertThat(domain.isSingleValue()).isFalse();
        assertThat(domain.isNullableSingleValue()).isFalse();
        assertThat(domain.isOnlyNull()).isFalse();
        assertThat(domain.isNullAllowed()).isFalse();
        assertThat(domain.getValues()).isEqualTo(ValueSet.all(HYPER_LOG_LOG));
        assertThat(domain.getType()).isEqualTo(HYPER_LOG_LOG);
        assertThat(domain.includesNullableValue(Slices.EMPTY_SLICE)).isTrue();
        assertThat(domain.includesNullableValue(null)).isFalse();
        assertThat(domain.complement()).isEqualTo(Domain.onlyNull(HYPER_LOG_LOG));
        assertThat(domain.toString()).isEqualTo("[ [ALL] ]");
    }

    @Test
    public void testOrderableSingleValue()
    {
        Domain domain = Domain.singleValue(BIGINT, 0L);
        assertThat(domain.isNone()).isFalse();
        assertThat(domain.isAll()).isFalse();
        assertThat(domain.isSingleValue()).isTrue();
        assertThat(domain.isNullableSingleValue()).isTrue();
        assertThat(domain.isOnlyNull()).isFalse();
        assertThat(domain.isNullAllowed()).isFalse();
        assertThat(domain.getValues()).isEqualTo(ValueSet.ofRanges(Range.equal(BIGINT, 0L)));
        assertThat(domain.getType()).isEqualTo(BIGINT);
        assertThat(domain.includesNullableValue(Long.MIN_VALUE)).isFalse();
        assertThat(domain.includesNullableValue(0L)).isTrue();
        assertThat(domain.includesNullableValue(Long.MAX_VALUE)).isFalse();
        assertThat(domain.complement()).isEqualTo(Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 0L), Range.greaterThan(BIGINT, 0L)), true));
        assertThat(domain.getSingleValue()).isEqualTo(0L);
        assertThat(domain.getNullableSingleValue()).isEqualTo(0L);
        assertThat(domain.toString()).isEqualTo("[ SortedRangeSet[type=bigint, ranges=1, {[0]}] ]");

        assertThatThrownBy(() -> Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, true, 2L, true)), false).getSingleValue())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Domain is not a single value");
    }

    @Test
    public void testEquatableSingleValue()
    {
        Domain domain = Domain.singleValue(ID, 0L);
        assertThat(domain.isNone()).isFalse();
        assertThat(domain.isAll()).isFalse();
        assertThat(domain.isSingleValue()).isTrue();
        assertThat(domain.isNullableSingleValue()).isTrue();
        assertThat(domain.isOnlyNull()).isFalse();
        assertThat(domain.isNullAllowed()).isFalse();
        assertThat(domain.getValues()).isEqualTo(ValueSet.of(ID, 0L));
        assertThat(domain.getType()).isEqualTo(ID);
        assertThat(domain.includesNullableValue(0L)).isTrue();
        assertThat(domain.includesNullableValue(null)).isFalse();
        assertThat(domain.complement()).isEqualTo(Domain.create(ValueSet.of(ID, 0L).complement(), true));
        assertThat(domain.getSingleValue()).isEqualTo(0L);
        assertThat(domain.getNullableSingleValue()).isEqualTo(0L);
        assertThat(domain.toString()).isEqualTo("[ EquatableValueSet[type=id, values=1, {0}] ]");

        assertThatThrownBy(() -> Domain.create(ValueSet.of(ID, 0L, 1L), false).getSingleValue())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Domain is not a single value");
    }

    @Test
    public void testUncomparableSingleValue()
    {
        assertThatThrownBy(() -> Domain.singleValue(HYPER_LOG_LOG, Slices.EMPTY_SLICE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot create discrete ValueSet with non-comparable type: HyperLogLog");
    }

    @Test
    public void testOverlaps()
    {
        assertThat(Domain.all(BIGINT).overlaps(Domain.all(BIGINT))).isTrue();
        assertThat(Domain.all(BIGINT).overlaps(Domain.none(BIGINT))).isFalse();
        assertThat(Domain.all(BIGINT).overlaps(Domain.notNull(BIGINT))).isTrue();
        assertThat(Domain.all(BIGINT).overlaps(Domain.onlyNull(BIGINT))).isTrue();
        assertThat(Domain.all(BIGINT).overlaps(Domain.singleValue(BIGINT, 0L))).isTrue();

        assertThat(Domain.none(BIGINT).overlaps(Domain.all(BIGINT))).isFalse();
        assertThat(Domain.none(BIGINT).overlaps(Domain.none(BIGINT))).isFalse();
        assertThat(Domain.none(BIGINT).overlaps(Domain.notNull(BIGINT))).isFalse();
        assertThat(Domain.none(BIGINT).overlaps(Domain.onlyNull(BIGINT))).isFalse();
        assertThat(Domain.none(BIGINT).overlaps(Domain.singleValue(BIGINT, 0L))).isFalse();

        assertThat(Domain.notNull(BIGINT).overlaps(Domain.all(BIGINT))).isTrue();
        assertThat(Domain.notNull(BIGINT).overlaps(Domain.none(BIGINT))).isFalse();
        assertThat(Domain.notNull(BIGINT).overlaps(Domain.notNull(BIGINT))).isTrue();
        assertThat(Domain.notNull(BIGINT).overlaps(Domain.onlyNull(BIGINT))).isFalse();
        assertThat(Domain.notNull(BIGINT).overlaps(Domain.singleValue(BIGINT, 0L))).isTrue();

        assertThat(Domain.onlyNull(BIGINT).overlaps(Domain.all(BIGINT))).isTrue();
        assertThat(Domain.onlyNull(BIGINT).overlaps(Domain.none(BIGINT))).isFalse();
        assertThat(Domain.onlyNull(BIGINT).overlaps(Domain.notNull(BIGINT))).isFalse();
        assertThat(Domain.onlyNull(BIGINT).overlaps(Domain.onlyNull(BIGINT))).isTrue();
        assertThat(Domain.onlyNull(BIGINT).overlaps(Domain.singleValue(BIGINT, 0L))).isFalse();

        assertThat(Domain.singleValue(BIGINT, 0L).overlaps(Domain.all(BIGINT))).isTrue();
        assertThat(Domain.singleValue(BIGINT, 0L).overlaps(Domain.none(BIGINT))).isFalse();
        assertThat(Domain.singleValue(BIGINT, 0L).overlaps(Domain.notNull(BIGINT))).isTrue();
        assertThat(Domain.singleValue(BIGINT, 0L).overlaps(Domain.onlyNull(BIGINT))).isFalse();
        assertThat(Domain.singleValue(BIGINT, 0L).overlaps(Domain.singleValue(BIGINT, 0L))).isTrue();
    }

    @Test
    public void testContains()
    {
        assertThat(Domain.all(BIGINT).contains(Domain.all(BIGINT))).isTrue();
        assertThat(Domain.all(BIGINT).contains(Domain.none(BIGINT))).isTrue();
        assertThat(Domain.all(BIGINT).contains(Domain.notNull(BIGINT))).isTrue();
        assertThat(Domain.all(BIGINT).contains(Domain.onlyNull(BIGINT))).isTrue();
        assertThat(Domain.all(BIGINT).contains(Domain.singleValue(BIGINT, 0L))).isTrue();

        assertThat(Domain.none(BIGINT).contains(Domain.all(BIGINT))).isFalse();
        assertThat(Domain.none(BIGINT).contains(Domain.none(BIGINT))).isTrue();
        assertThat(Domain.none(BIGINT).contains(Domain.notNull(BIGINT))).isFalse();
        assertThat(Domain.none(BIGINT).contains(Domain.onlyNull(BIGINT))).isFalse();
        assertThat(Domain.none(BIGINT).contains(Domain.singleValue(BIGINT, 0L))).isFalse();

        assertThat(Domain.notNull(BIGINT).contains(Domain.all(BIGINT))).isFalse();
        assertThat(Domain.notNull(BIGINT).contains(Domain.none(BIGINT))).isTrue();
        assertThat(Domain.notNull(BIGINT).contains(Domain.notNull(BIGINT))).isTrue();
        assertThat(Domain.notNull(BIGINT).contains(Domain.onlyNull(BIGINT))).isFalse();
        assertThat(Domain.notNull(BIGINT).contains(Domain.singleValue(BIGINT, 0L))).isTrue();

        assertThat(Domain.onlyNull(BIGINT).contains(Domain.all(BIGINT))).isFalse();
        assertThat(Domain.onlyNull(BIGINT).contains(Domain.none(BIGINT))).isTrue();
        assertThat(Domain.onlyNull(BIGINT).contains(Domain.notNull(BIGINT))).isFalse();
        assertThat(Domain.onlyNull(BIGINT).contains(Domain.onlyNull(BIGINT))).isTrue();
        assertThat(Domain.onlyNull(BIGINT).contains(Domain.singleValue(BIGINT, 0L))).isFalse();

        assertThat(Domain.singleValue(BIGINT, 0L).contains(Domain.all(BIGINT))).isFalse();
        assertThat(Domain.singleValue(BIGINT, 0L).contains(Domain.none(BIGINT))).isTrue();
        assertThat(Domain.singleValue(BIGINT, 0L).contains(Domain.notNull(BIGINT))).isFalse();
        assertThat(Domain.singleValue(BIGINT, 0L).contains(Domain.onlyNull(BIGINT))).isFalse();
        assertThat(Domain.singleValue(BIGINT, 0L).contains(Domain.singleValue(BIGINT, 0L))).isTrue();
    }

    @Test
    public void testIntersect()
    {
        assertThat(Domain.all(BIGINT).intersect(Domain.all(BIGINT))).isEqualTo(Domain.all(BIGINT));

        assertThat(Domain.none(BIGINT).intersect(Domain.none(BIGINT))).isEqualTo(Domain.none(BIGINT));

        assertThat(Domain.all(BIGINT).intersect(Domain.none(BIGINT))).isEqualTo(Domain.none(BIGINT));

        assertThat(Domain.notNull(BIGINT).intersect(Domain.onlyNull(BIGINT))).isEqualTo(Domain.none(BIGINT));

        assertThat(Domain.singleValue(BIGINT, 0L).intersect(Domain.all(BIGINT))).isEqualTo(Domain.singleValue(BIGINT, 0L));

        assertThat(Domain.singleValue(BIGINT, 0L).intersect(Domain.onlyNull(BIGINT))).isEqualTo(Domain.none(BIGINT));

        assertThat(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L)), true).intersect(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), true))).isEqualTo(Domain.onlyNull(BIGINT));

        assertThat(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L)), true).intersect(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false))).isEqualTo(Domain.singleValue(BIGINT, 1L));
    }

    @Test
    public void testUnion()
    {
        assertUnion(Domain.all(BIGINT), Domain.all(BIGINT), Domain.all(BIGINT));
        assertUnion(Domain.none(BIGINT), Domain.none(BIGINT), Domain.none(BIGINT));
        assertUnion(Domain.all(BIGINT), Domain.none(BIGINT), Domain.all(BIGINT));
        assertUnion(Domain.notNull(BIGINT), Domain.onlyNull(BIGINT), Domain.all(BIGINT));
        assertUnion(Domain.singleValue(BIGINT, 0L), Domain.all(BIGINT), Domain.all(BIGINT));
        assertUnion(Domain.singleValue(BIGINT, 0L), Domain.notNull(BIGINT), Domain.notNull(BIGINT));
        assertUnion(Domain.singleValue(BIGINT, 0L), Domain.onlyNull(BIGINT), Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 0L)), true));

        assertUnion(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L)), true),
                Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), true),
                Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), true));

        assertUnion(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L)), true),
                Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false),
                Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), true));

        assertUnion(
                Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 20L)), true),
                Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 10L)), true),
                Domain.all(BIGINT));

        assertUnion(
                Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 20L)), false),
                Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 10L)), false),
                Domain.create(ValueSet.all(BIGINT), false));
    }

    @Test
    public void testSubtract()
    {
        assertThat(Domain.all(BIGINT).subtract(Domain.all(BIGINT))).isEqualTo(Domain.none(BIGINT));
        assertThat(Domain.all(BIGINT).subtract(Domain.none(BIGINT))).isEqualTo(Domain.all(BIGINT));
        assertThat(Domain.all(BIGINT).subtract(Domain.notNull(BIGINT))).isEqualTo(Domain.onlyNull(BIGINT));
        assertThat(Domain.all(BIGINT).subtract(Domain.onlyNull(BIGINT))).isEqualTo(Domain.notNull(BIGINT));
        assertThat(Domain.all(BIGINT).subtract(Domain.singleValue(BIGINT, 0L))).isEqualTo(Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 0L), Range.greaterThan(BIGINT, 0L)), true));

        assertThat(Domain.none(BIGINT).subtract(Domain.all(BIGINT))).isEqualTo(Domain.none(BIGINT));
        assertThat(Domain.none(BIGINT).subtract(Domain.none(BIGINT))).isEqualTo(Domain.none(BIGINT));
        assertThat(Domain.none(BIGINT).subtract(Domain.notNull(BIGINT))).isEqualTo(Domain.none(BIGINT));
        assertThat(Domain.none(BIGINT).subtract(Domain.onlyNull(BIGINT))).isEqualTo(Domain.none(BIGINT));
        assertThat(Domain.none(BIGINT).subtract(Domain.singleValue(BIGINT, 0L))).isEqualTo(Domain.none(BIGINT));

        assertThat(Domain.notNull(BIGINT).subtract(Domain.all(BIGINT))).isEqualTo(Domain.none(BIGINT));
        assertThat(Domain.notNull(BIGINT).subtract(Domain.none(BIGINT))).isEqualTo(Domain.notNull(BIGINT));
        assertThat(Domain.notNull(BIGINT).subtract(Domain.notNull(BIGINT))).isEqualTo(Domain.none(BIGINT));
        assertThat(Domain.notNull(BIGINT).subtract(Domain.onlyNull(BIGINT))).isEqualTo(Domain.notNull(BIGINT));
        assertThat(Domain.notNull(BIGINT).subtract(Domain.singleValue(BIGINT, 0L))).isEqualTo(Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 0L), Range.greaterThan(BIGINT, 0L)), false));

        assertThat(Domain.onlyNull(BIGINT).subtract(Domain.all(BIGINT))).isEqualTo(Domain.none(BIGINT));
        assertThat(Domain.onlyNull(BIGINT).subtract(Domain.none(BIGINT))).isEqualTo(Domain.onlyNull(BIGINT));
        assertThat(Domain.onlyNull(BIGINT).subtract(Domain.notNull(BIGINT))).isEqualTo(Domain.onlyNull(BIGINT));
        assertThat(Domain.onlyNull(BIGINT).subtract(Domain.onlyNull(BIGINT))).isEqualTo(Domain.none(BIGINT));
        assertThat(Domain.onlyNull(BIGINT).subtract(Domain.singleValue(BIGINT, 0L))).isEqualTo(Domain.onlyNull(BIGINT));

        assertThat(Domain.singleValue(BIGINT, 0L).subtract(Domain.all(BIGINT))).isEqualTo(Domain.none(BIGINT));
        assertThat(Domain.singleValue(BIGINT, 0L).subtract(Domain.none(BIGINT))).isEqualTo(Domain.singleValue(BIGINT, 0L));
        assertThat(Domain.singleValue(BIGINT, 0L).subtract(Domain.notNull(BIGINT))).isEqualTo(Domain.none(BIGINT));
        assertThat(Domain.singleValue(BIGINT, 0L).subtract(Domain.onlyNull(BIGINT))).isEqualTo(Domain.singleValue(BIGINT, 0L));
        assertThat(Domain.singleValue(BIGINT, 0L).subtract(Domain.singleValue(BIGINT, 0L))).isEqualTo(Domain.none(BIGINT));

        assertThat(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L)), true).subtract(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), true))).isEqualTo(Domain.singleValue(BIGINT, 1L));

        assertThat(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L)), true).subtract(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false))).isEqualTo(Domain.onlyNull(BIGINT));
    }

    @Test
    public void testJsonSerialization()
            throws Exception
    {
        JsonMapper mapper = new JsonMapperProvider()
                .withJsonDeserializers(Map.of(
                        Type.class, new TestingTypeDeserializer(new TestingTypeManager()),
                        Block.class, new TestingBlockJsonSerde.Deserializer(new TestingBlockEncodingSerde())))
                .withJsonSerializers(Map.of(
                        Block.class, new TestingBlockJsonSerde.Serializer(new TestingBlockEncodingSerde())))
                .get();

        Domain domain = Domain.all(BIGINT);
        assertThat(domain).isEqualTo(mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.none(DOUBLE);
        assertThat(domain).isEqualTo(mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.notNull(BOOLEAN);
        assertThat(domain).isEqualTo(mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.notNull(HYPER_LOG_LOG);
        assertThat(domain).isEqualTo(mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.onlyNull(VARCHAR);
        assertThat(domain).isEqualTo(mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.onlyNull(HYPER_LOG_LOG);
        assertThat(domain).isEqualTo(mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.singleValue(BIGINT, Long.MIN_VALUE);
        assertThat(domain).isEqualTo(mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.singleValue(ID, Long.MIN_VALUE);
        assertThat(domain).isEqualTo(mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 0L), Range.equal(BIGINT, 1L), Range.range(BIGINT, 2L, true, 3L, true)), true);
        assertThat(domain).isEqualTo(mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = bloomFilterDomain(1L, 2L, 3L);
        Domain deserialized = mapper.readValue(mapper.writeValueAsString(domain), Domain.class);
        assertThat(deserialized).isEqualTo(domain);
        assertThat(deserialized.getBloomFilter().orElseThrow().mightContain(1L)).isTrue();

        // domains without a bloom filter keep their serialized form unchanged
        assertThat(mapper.writeValueAsString(Domain.singleValue(BIGINT, 1L)))
                .doesNotContain("bloomFilter");
    }

    @Test
    public void testBloomFilterIsAdditive()
    {
        Domain domain = bloomFilterDomain(1L, 2L, 3L);

        // the value set keeps working exactly as it does without a bloom filter
        assertThat(domain.getValues()).isEqualTo(ValueSet.ofRanges(Range.range(BIGINT, 1L, true, 3L, true)));
        assertThat(domain.getValues().getRanges().getRangeCount()).isEqualTo(1);
        assertThat(domain.includesNullableValue(2L)).isTrue();
        assertThat(domain.isNullAllowed()).isFalse();
        assertThat(domain.getRetainedSizeInBytes()).isGreaterThan(Domain.singleValue(BIGINT, 1L).getRetainedSizeInBytes());
        assertThat(domain.toString()).contains("datasketches bloom filter");

        BloomFilter filter = domain.getBloomFilter().orElseThrow();
        assertThat(filter.mightContain(1L)).isTrue();
        assertThat(filter.mightContain(4L)).isFalse();
    }

    @Test
    public void testBloomFilterUnion()
    {
        Domain first = bloomFilterDomain(1L, 2L);
        Domain second = bloomFilterDomain(30L, 40L);

        Domain union = first.union(second);
        BloomFilter unionFilter = union.getBloomFilter().orElseThrow();
        assertThat(unionFilter.mightContain(1L)).isTrue();
        assertThat(unionFilter.mightContain(40L)).isTrue();
        assertThat(unionFilter.mightContain(4L)).isFalse();
        assertThat(Domain.union(ImmutableList.of(first, second))).isEqualTo(union);

        // a domain which did not degrade contributes its discrete values to the filter
        Domain discrete = Domain.multipleValues(BIGINT, ImmutableList.of(70L, 80L));
        BloomFilter absorbed = first.union(discrete).getBloomFilter().orElseThrow();
        assertThat(absorbed.mightContain(1L)).isTrue();
        assertThat(absorbed.mightContain(70L)).isTrue();
        assertThat(absorbed.mightContain(4L)).isFalse();
        assertThat(Domain.union(ImmutableList.of(discrete, first)).getBloomFilter()).contains(absorbed);

        // values which cannot be enumerated would be missing from the filter, so it is dropped
        Domain range = Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 100L, true, 200L, true)), false);
        assertThat(first.union(range).getBloomFilter()).isEmpty();
        assertThat(Domain.union(ImmutableList.of(first, range)).getBloomFilter()).isEmpty();
        assertThat(first.union(Domain.all(BIGINT)).getBloomFilter()).isEmpty();
    }

    @Test
    public void testBloomFilterOnDerivedDomains()
    {
        Domain domain = bloomFilterDomain(1L, 2L, 3L);
        Domain other = Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 2L, true, 100L, true)), false);

        // intersection and difference are subsets, so the filter remains a superset of the result
        assertThat(domain.intersect(other).getBloomFilter()).isEqualTo(domain.getBloomFilter());
        assertThat(other.intersect(domain).getBloomFilter()).isEqualTo(domain.getBloomFilter());
        assertThat(domain.subtract(other).getBloomFilter()).isEqualTo(domain.getBloomFilter());
        // the complement of a superset is not a superset of the complement
        assertThat(domain.complement().getBloomFilter()).isEmpty();

        // simplification is used to shrink a domain which exceeds a size limit, and the bloom filter is its
        // largest part, so it is dropped
        Domain manyRanges = Domain.create(
                ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 3L), Range.equal(BIGINT, 5L)),
                false,
                domain.getBloomFilter());
        assertThat(manyRanges.simplify(1).getBloomFilter()).isEmpty();
        assertThat(manyRanges.simplify(1).getValues()).isEqualTo(ValueSet.ofRanges(Range.range(BIGINT, 1L, true, 5L, true)));
        assertThat(domain.simplify(1).getBloomFilter()).isEmpty();
        assertThat(domain.simplify(1).getValues()).isEqualTo(domain.getValues());
        // a domain without a bloom filter is unaffected
        Domain withoutFilter = Domain.singleValue(BIGINT, 1L);
        assertThat(withoutFilter.simplify(1)).isSameAs(withoutFilter);
    }

    private static Domain bloomFilterDomain(long... values)
    {
        BloomFilterBuilder builder = BloomFilterKind.DATASKETCHES.createBuilder(BIGINT, 1000, 0.01);
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (long value : values) {
            builder.add(value);
            min = Math.min(min, value);
            max = Math.max(max, value);
        }
        return Domain.create(
                ValueSet.ofRanges(Range.range(BIGINT, min, true, max, true)),
                false,
                Optional.of(builder.build()));
    }

    private void assertUnion(Domain first, Domain second, Domain expected)
    {
        assertThat(first.union(second)).isEqualTo(expected);
        assertThat(Domain.union(ImmutableList.of(first, second))).isEqualTo(expected);
    }
}
