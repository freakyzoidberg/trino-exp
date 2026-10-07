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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_ABSENT;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Defines the possible values of a single variable in terms of its valid scalar values and nullability.
 * <p>
 * For example:
 * <ul>
 * <li>Domain.none() => no scalar values allowed, NULL not allowed
 * <li>Domain.all() => all scalar values allowed, NULL allowed
 * <li>Domain.onlyNull() => no scalar values allowed, NULL allowed
 * <li>Domain.notNull() => all scalar values allowed, NULL not allowed
 * </ul>
 */
public final class Domain
{
    private static final int INSTANCE_SIZE = instanceSize(Domain.class);

    public static final int DEFAULT_COMPACTION_THRESHOLD = 32;

    private final ValueSet values;
    private final boolean nullAllowed;
    private final Optional<BloomFilter> bloomFilter;

    private Domain(ValueSet values, boolean nullAllowed)
    {
        this(values, nullAllowed, Optional.empty());
    }

    private Domain(ValueSet values, boolean nullAllowed, Optional<BloomFilter> bloomFilter)
    {
        this.values = requireNonNull(values, "values is null");
        this.nullAllowed = nullAllowed;
        this.bloomFilter = requireNonNull(bloomFilter, "bloomFilter is null");
    }

    public static Domain create(ValueSet values, boolean nullAllowed)
    {
        return new Domain(values, nullAllowed, Optional.empty());
    }

    /**
     * Creates a domain which, in addition to {@code values}, carries an approximate membership filter over the
     * non-null values of the column. The filter is a superset of the values of the column, so consumers which
     * do not understand its {@link BloomFilter#kind()} can safely ignore it and use {@code values} alone.
     */
    @JsonCreator
    public static Domain create(
            @JsonProperty("values") ValueSet values,
            @JsonProperty("nullAllowed") boolean nullAllowed,
            @JsonProperty("bloomFilter") Optional<BloomFilter> bloomFilter)
    {
        return new Domain(values, nullAllowed, bloomFilter);
    }

    public static Domain none(Type type)
    {
        return new Domain(ValueSet.none(type), false);
    }

    public static Domain all(Type type)
    {
        return new Domain(ValueSet.all(type), true);
    }

    public static Domain onlyNull(Type type)
    {
        return new Domain(ValueSet.none(type), true);
    }

    public static Domain notNull(Type type)
    {
        return new Domain(ValueSet.all(type), false);
    }

    public static Domain singleValue(Type type, Object value)
    {
        return singleValue(type, value, false);
    }

    public static Domain singleValue(Type type, Object value, boolean nullAllowed)
    {
        return new Domain(ValueSet.of(type, value), nullAllowed);
    }

    public static Domain multipleValues(Type type, List<?> values)
    {
        return multipleValues(type, values, false);
    }

    public static Domain multipleValues(Type type, List<?> values, boolean nullAllowed)
    {
        if (values.isEmpty()) {
            throw new IllegalArgumentException("values cannot be empty");
        }
        if (values.size() == 1) {
            return singleValue(type, values.get(0), nullAllowed);
        }
        return new Domain(ValueSet.of(type, values.get(0), values.subList(1, values.size()).toArray()), nullAllowed);
    }

    public Type getType()
    {
        return values.getType();
    }

    @JsonProperty
    public ValueSet getValues()
    {
        return values;
    }

    @JsonProperty
    public boolean isNullAllowed()
    {
        return nullAllowed;
    }

    /**
     * An optional approximate membership filter over the non-null values of the column, in addition to
     * {@link #getValues()}. It never reports a false negative, so a value rejected by the filter is certainly
     * not in the domain, while a value accepted by it may still be absent.
     */
    @JsonProperty
    @JsonInclude(NON_ABSENT)
    public Optional<BloomFilter> getBloomFilter()
    {
        return bloomFilter;
    }

    public boolean isNone()
    {
        return values.isNone() && !nullAllowed;
    }

    public boolean isAll()
    {
        return values.isAll() && nullAllowed;
    }

    public boolean isSingleValue()
    {
        return !nullAllowed && values.isSingleValue();
    }

    public boolean isNullableSingleValue()
    {
        if (nullAllowed) {
            return values.isNone();
        }
        return values.isSingleValue();
    }

    public boolean isOnlyNull()
    {
        return values.isNone() && nullAllowed;
    }

    public Object getSingleValue()
    {
        if (!isSingleValue()) {
            throw new IllegalStateException("Domain is not a single value");
        }
        return values.getSingleValue();
    }

    public Object getNullableSingleValue()
    {
        if (!isNullableSingleValue()) {
            throw new IllegalStateException("Domain is not a nullable single value");
        }

        if (nullAllowed) {
            return null;
        }
        return values.getSingleValue();
    }

    public boolean includesNullableValue(Object value)
    {
        return value == null ? nullAllowed : values.containsValue(value);
    }

    public boolean isNullableDiscreteSet()
    {
        return values.isNone() ? nullAllowed : values.isDiscreteSet();
    }

    public DiscreteSet getNullableDiscreteSet()
    {
        if (!isNullableDiscreteSet()) {
            throw new IllegalStateException("Domain is not a nullable discrete set");
        }

        return new DiscreteSet(
                values.isNone() ? List.of() : values.getDiscreteSet(),
                nullAllowed);
    }

    public boolean overlaps(Domain other)
    {
        checkCompatibility(other);
        if (this.isNullAllowed() && other.isNullAllowed()) {
            return true;
        }
        return values.overlaps(other.getValues());
    }

    public boolean contains(Domain other)
    {
        checkCompatibility(other);
        if (!this.isNullAllowed() && other.isNullAllowed()) {
            return false;
        }
        return values.contains(other.getValues());
    }

    public Domain intersect(Domain other)
    {
        checkCompatibility(other);
        // the intersection is a subset of this domain, so this bloom filter remains a superset of the result
        return new Domain(values.intersect(other.getValues()), this.isNullAllowed() && other.isNullAllowed(), bloomFilter.or(() -> other.bloomFilter));
    }

    public Domain union(Domain other)
    {
        checkCompatibility(other);
        return new Domain(values.union(other.getValues()), this.isNullAllowed() || other.isNullAllowed(), unionBloomFilters(List.of(this, other)));
    }

    public static Domain union(List<Domain> domains)
    {
        if (domains.isEmpty()) {
            throw new IllegalArgumentException("domains cannot be empty for union");
        }
        if (domains.size() == 1) {
            return domains.get(0);
        }

        boolean nullAllowed = false;
        List<ValueSet> valueSets = new ArrayList<>(domains.size());
        for (Domain domain : domains) {
            valueSets.add(domain.getValues());
            nullAllowed = nullAllowed || domain.nullAllowed;
        }

        ValueSet unionedValues = valueSets.get(0).union(valueSets.subList(1, valueSets.size()));

        return new Domain(unionedValues, nullAllowed, unionBloomFilters(domains));
    }

    public Domain complement()
    {
        // the complement of a superset is not a superset of the complement, so the bloom filter is dropped
        return new Domain(values.complement(), !nullAllowed);
    }

    public Domain subtract(Domain other)
    {
        checkCompatibility(other);
        // the difference is a subset of this domain, so this bloom filter remains a superset of the result
        return new Domain(values.subtract(other.getValues()), this.isNullAllowed() && !other.isNullAllowed(), bloomFilter);
    }

    /**
     * Merges the bloom filters of {@code domains} into a filter which is a superset of their union, or returns
     * empty when a domain contributes values which cannot be added to a filter.
     */
    private static Optional<BloomFilter> unionBloomFilters(List<Domain> domains)
    {
        int firstWithFilter = -1;
        for (int i = 0; i < domains.size(); i++) {
            if (domains.get(i).bloomFilter.isPresent()) {
                firstWithFilter = i;
                break;
            }
        }
        if (firstWithFilter < 0) {
            return Optional.empty();
        }

        Optional<BloomFilter> result = domains.get(firstWithFilter).bloomFilter;
        for (int i = 0; i < domains.size() && result.isPresent(); i++) {
            if (i != firstWithFilter) {
                result = mergeBloomFilter(result.get(), domains.get(i));
            }
        }
        return result;
    }

    private static Optional<BloomFilter> mergeBloomFilter(BloomFilter filter, Domain domain)
    {
        if (domain.bloomFilter.isPresent()) {
            return filter.union(domain.bloomFilter.get());
        }
        ValueSet otherValues = domain.getValues();
        if (otherValues.isNone()) {
            return Optional.of(filter);
        }
        if (!otherValues.isDiscreteSet()) {
            // the values of the other domain cannot be enumerated, so the merged filter would miss them
            return Optional.empty();
        }
        return Optional.of(filter.addAll(otherValues.getDiscreteSet()));
    }

    private void checkCompatibility(Domain domain)
    {
        if (!getType().equals(domain.getType())) {
            throw new IllegalArgumentException(format("Mismatched Domain types: %s vs %s", getType(), domain.getType()));
        }
        if (values.getClass() != domain.values.getClass()) {
            throw new IllegalArgumentException(format("Mismatched Domain value set classes: %s vs %s", values.getClass(), domain.values.getClass()));
        }
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(values, nullAllowed, bloomFilter);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Domain other = (Domain) obj;
        return Objects.equals(this.values, other.values) &&
                this.nullAllowed == other.nullAllowed &&
                Objects.equals(this.bloomFilter, other.bloomFilter);
    }

    /**
     * Reduces the number of discrete components in the Domain if there are too many.
     */
    public Domain simplify()
    {
        return simplify(DEFAULT_COMPACTION_THRESHOLD);
    }

    /**
     * Reduces the size of the domain, dropping the bloom filter and the discrete components which exceed
     * {@code threshold}.
     */
    public Domain simplify(int threshold)
    {
        Optional<ValueSet> simplifiedValueSet = values.getValuesProcessor().transform(
                ranges -> {
                    if (ranges.getRangeCount() <= threshold) {
                        return Optional.empty();
                    }
                    return Optional.of(ValueSet.ofRanges(ranges.getSpan()));
                },
                discreteValues -> {
                    if (discreteValues.getValuesCount() <= threshold) {
                        return Optional.empty();
                    }
                    return Optional.of(ValueSet.all(values.getType()));
                },
                _ -> Optional.empty());
        if (simplifiedValueSet.isEmpty()) {
            if (bloomFilter.isEmpty()) {
                return this;
            }
            // the bloom filter is the largest part of a domain which carries one, and cannot be made smaller
            return new Domain(values, nullAllowed, Optional.empty());
        }
        return new Domain(simplifiedValueSet.get(), nullAllowed, Optional.empty());
    }

    @Override
    public String toString()
    {
        return toString(10);
    }

    public String toString(int limit)
    {
        if (isAll()) {
            return "ALL";
        }
        if (isNone()) {
            return "NONE";
        }
        if (isOnlyNull()) {
            return "[NULL]";
        }
        String bloomFilterDescription = bloomFilter.map(filter -> ", " + filter).orElse("");
        return "[ " + (nullAllowed ? "NULL, " : "") + values.toString(limit) + bloomFilterDescription + " ]";
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + values.getRetainedSizeInBytes() + bloomFilter.map(BloomFilter::getRetainedSizeInBytes).orElse(0L);
    }

    public static class DiscreteSet
    {
        private final List<Object> nonNullValues;
        private final boolean containsNull;

        DiscreteSet(List<Object> values, boolean containsNull)
        {
            this.nonNullValues = requireNonNull(values, "values is null");
            this.containsNull = containsNull;
            if (!containsNull && values.isEmpty()) {
                throw new IllegalArgumentException("Discrete set cannot be empty");
            }
        }

        public List<Object> getNonNullValues()
        {
            return nonNullValues;
        }

        public boolean containsNull()
        {
            return containsNull;
        }
    }
}
