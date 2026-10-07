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
package io.trino.sql.gen.columnar;

import io.trino.operator.project.SelectedPositions;
import io.trino.spi.block.Block;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.BloomFilter;

import static io.trino.operator.project.SelectedPositions.positionsList;
import static io.trino.operator.project.SelectedPositions.positionsRange;
import static java.util.Objects.requireNonNull;

/**
 * Applies the bloom filter of a dynamic filter to a single column of a page.
 * <p>
 * A bloom filter has no expression form, so unlike the rest of the dynamic filter it is not compiled but
 * probed directly. It is registered as its own conjunct of
 * {@link io.trino.sql.gen.columnar.DynamicPageFilter.DynamicFilterEvaluator}, so the selectivity profiler
 * disables it independently of the range filter collected for the same column.
 */
public final class BloomFilterEvaluator
        implements FilterEvaluator
{
    private final int channel;
    private final BloomFilter bloomFilter;
    private final boolean nullAllowed;

    private int[] outputPositions = new int[0];

    public BloomFilterEvaluator(int channel, BloomFilter bloomFilter, boolean nullAllowed)
    {
        this.channel = channel;
        this.bloomFilter = requireNonNull(bloomFilter, "bloomFilter is null");
        this.nullAllowed = nullAllowed;
    }

    @Override
    public SelectionResult evaluate(ConnectorSession session, SelectedPositions activePositions, SourcePage page)
    {
        if (activePositions.isEmpty()) {
            return new SelectionResult(activePositions, 0);
        }
        if (outputPositions.length < activePositions.size()) {
            outputPositions = new int[activePositions.size()];
        }

        long start = System.nanoTime();
        Block block = page.getBlock(channel);
        ValueBlock valueBlock = block.getUnderlyingValueBlock();
        int outputPositionsCount = 0;
        if (activePositions.isList()) {
            int[] positions = activePositions.getPositions();
            int offset = activePositions.getOffset();
            for (int index = offset; index < offset + activePositions.size(); index++) {
                int position = positions[index];
                if (mightContain(block, valueBlock, position)) {
                    outputPositions[outputPositionsCount++] = position;
                }
            }
        }
        else {
            int offset = activePositions.getOffset();
            for (int position = offset; position < offset + activePositions.size(); position++) {
                if (mightContain(block, valueBlock, position)) {
                    outputPositions[outputPositionsCount++] = position;
                }
            }
            if (outputPositionsCount == activePositions.size()) {
                // full range was selected
                return new SelectionResult(positionsRange(offset, outputPositionsCount), System.nanoTime() - start);
            }
        }
        return new SelectionResult(positionsList(outputPositions, 0, outputPositionsCount), System.nanoTime() - start);
    }

    private boolean mightContain(Block block, ValueBlock valueBlock, int position)
    {
        int valuePosition = block.getUnderlyingValuePosition(position);
        if (valueBlock.isNull(valuePosition)) {
            return nullAllowed;
        }
        return bloomFilter.mightContain(valueBlock, valuePosition);
    }
}
