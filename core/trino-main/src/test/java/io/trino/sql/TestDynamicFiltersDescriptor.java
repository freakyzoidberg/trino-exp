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
package io.trino.sql;

import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.sql.DynamicFilters.Descriptor;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.plan.DynamicFilterId;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN_OR_EQUAL;
import static org.assertj.core.api.Assertions.assertThat;

class TestDynamicFiltersDescriptor
{
    private static final DynamicFilterId FILTER_ID = new DynamicFilterId("df");
    private static final Reference INPUT = new Reference(BIGINT, "probe");

    @Test
    public void testApplyComparisonToRange()
    {
        Domain collected = Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, true, 10L, true)), false);

        assertThat(descriptor(EQUAL).applyComparison(collected)).isEqualTo(collected);
        assertThat(descriptor(LESS_THAN).applyComparison(collected))
                .isEqualTo(Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 10L)), false));
        assertThat(descriptor(LESS_THAN_OR_EQUAL).applyComparison(collected))
                .isEqualTo(Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 10L)), false));
        assertThat(descriptor(GREATER_THAN).applyComparison(collected))
                .isEqualTo(Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 1L)), false));
        assertThat(descriptor(GREATER_THAN_OR_EQUAL).applyComparison(collected))
                .isEqualTo(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 1L)), false));
    }

    @Test
    public void testApplyComparisonToUnboundedDomain()
    {
        // a dynamic filter which only excludes nulls, as collected when values degrade to a bloom filter alone,
        // has no bound to compare against
        Domain notNull = Domain.notNull(BIGINT);

        assertThat(descriptor(EQUAL).applyComparison(notNull)).isEqualTo(notNull);
        assertThat(descriptor(LESS_THAN).applyComparison(notNull)).isEqualTo(Domain.all(BIGINT));
        assertThat(descriptor(LESS_THAN_OR_EQUAL).applyComparison(notNull)).isEqualTo(Domain.all(BIGINT));
        assertThat(descriptor(GREATER_THAN).applyComparison(notNull)).isEqualTo(Domain.all(BIGINT));
        assertThat(descriptor(GREATER_THAN_OR_EQUAL).applyComparison(notNull)).isEqualTo(Domain.all(BIGINT));
    }

    @Test
    public void testApplyComparisonToHalfBoundedDomain()
    {
        Domain lowBounded = Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 1L)), false);

        assertThat(descriptor(GREATER_THAN).applyComparison(lowBounded))
                .isEqualTo(Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 1L)), false));
        assertThat(descriptor(LESS_THAN).applyComparison(lowBounded)).isEqualTo(Domain.all(BIGINT));
    }

    private static Descriptor descriptor(ComparisonOperator operator)
    {
        return new Descriptor(FILTER_ID, INPUT, operator);
    }
}
