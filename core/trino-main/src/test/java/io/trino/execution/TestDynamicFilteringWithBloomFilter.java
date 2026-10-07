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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.DynamicFilterService.DynamicFilterDomainStats;
import io.trino.spi.Plugin;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.predicate.BloomFilterKind;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import io.trino.testing.StandaloneQueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static io.trino.SystemSessionProperties.DYNAMIC_FILTERING_BLOOM_FILTER_ENABLED;
import static io.trino.SystemSessionProperties.DYNAMIC_ROW_FILTERING_BLOOM_FILTER_ENABLED;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * End to end coverage of collecting a bloom filter for a dynamic filter which exceeds the distinct values limit.
 * The catalog declaring bloom filter support is a TPCH catalog wrapped in a connector which declares the
 * {@link BloomFilterKind#DATASKETCHES} capability; the plain {@code tpch} catalog declares nothing and is used to
 * verify that such a connector is never handed a bloom filter.
 */
@TestInstance(PER_CLASS)
public class TestDynamicFilteringWithBloomFilter
{
    private static final String BLOOM_FILTER_CATALOG = "bloom_tpch";
    private static final String PLAIN_CATALOG = "tpch";

    @Language("SQL")
    private static final String JOIN_WITH_LARGE_BUILD_SIDE =
            """
            SELECT count(*)
            FROM %s.tiny.lineitem l JOIN %s.tiny.orders o ON l.orderkey = o.orderkey
            WHERE o.totalprice > 1000
            """;

    private QueryRunner queryRunner;

    @BeforeAll
    public void setUp()
    {
        queryRunner = new StandaloneQueryRunner(
                testSessionBuilder().build(),
                server -> server.setProperties(ImmutableMap.of(
                        // degrade well before the tiny tables are exhausted
                        "dynamic-filtering.max-distinct-values-per-driver", "10",
                        "dynamic-filtering.partitioned.max-distinct-values-per-driver", "10",
                        "dynamic-filtering.bloom-filter.expected-distinct-values", "10000")));
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.installPlugin(new BloomFilterAwareTpchPlugin());
        queryRunner.createCatalog(BLOOM_FILTER_CATALOG, "bloom_filter_tpch", ImmutableMap.of());
        queryRunner.createCatalog(PLAIN_CATALOG, "tpch", ImmutableMap.of());
    }

    @AfterAll
    public void tearDown()
    {
        closeAllRuntimeException(queryRunner);
        queryRunner = null;
    }

    @Test
    public void testBloomFilterCollectedForSupportedConnector()
    {
        String domain = collectedDynamicFilterDomain(bloomFilterSession(true), BLOOM_FILTER_CATALOG);

        // the min/max range is still collected for consumers which cannot read the bloom filter
        assertThat(domain).contains("SortedRangeSet[type=bigint, ranges=1, {[1,60000]}]");
        assertThat(domain).contains("datasketches bloom filter");
    }

    @Test
    public void testBloomFilterNotCollectedWhenDisabled()
    {
        String domain = collectedDynamicFilterDomain(bloomFilterSession(false), BLOOM_FILTER_CATALOG);

        assertThat(domain).contains("SortedRangeSet[type=bigint, ranges=1, {[1,60000]}]");
        assertThat(domain).doesNotContain("bloom filter");
    }

    @Test
    public void testBloomFilterNotCollectedForUnsupportedConnector()
    {
        String domain = collectedDynamicFilterDomain(bloomFilterSession(true), PLAIN_CATALOG);

        assertThat(domain).contains("SortedRangeSet[type=bigint, ranges=1, {[1,60000]}]");
        assertThat(domain).doesNotContain("bloom filter");
    }

    @Test
    public void testBloomFilterCollectedForEngineRowFiltering()
    {
        // the engine reads the bloom filter itself when it filters rows with it, so no connector has to declare support
        Session session = Session.builder(bloomFilterSession(true))
                .setSystemProperty(DYNAMIC_ROW_FILTERING_BLOOM_FILTER_ENABLED, "true")
                .build();
        String domain = collectedDynamicFilterDomain(session, PLAIN_CATALOG);

        assertThat(domain).contains("SortedRangeSet[type=bigint, ranges=1, {[1,60000]}]");
        assertThat(domain).contains("datasketches bloom filter");
    }

    @Test
    public void testEngineRowFilteringAloneCollectsNothing()
    {
        // the row filtering flag has no effect while collection is disabled
        Session session = Session.builder(bloomFilterSession(false))
                .setSystemProperty(DYNAMIC_ROW_FILTERING_BLOOM_FILTER_ENABLED, "true")
                .build();
        String domain = collectedDynamicFilterDomain(session, PLAIN_CATALOG);

        assertThat(domain).doesNotContain("bloom filter");
    }

    private static Session bloomFilterSession(boolean bloomFilterEnabled)
    {
        return testSessionBuilder()
                // the probe side scan must run in the same stage as the join for the consuming connector to be known
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "BROADCAST")
                .setSystemProperty(DYNAMIC_FILTERING_BLOOM_FILTER_ENABLED, Boolean.toString(bloomFilterEnabled))
                .build();
    }

    private String collectedDynamicFilterDomain(Session session, String catalog)
    {
        MaterializedResultWithPlan result = queryRunner.executeWithPlan(session, JOIN_WITH_LARGE_BUILD_SIDE.formatted(catalog, catalog));
        List<DynamicFilterDomainStats> domainStats = queryRunner.getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(result.queryId())
                .getQueryStats()
                .getDynamicFiltersStats()
                .getDynamicFilterDomainStats();

        assertThat(domainStats).hasSize(1);
        return domainStats.getFirst().getSimplifiedDomain();
    }

    private static class BloomFilterAwareTpchPlugin
            implements Plugin
    {
        @Override
        public Iterable<ConnectorFactory> getConnectorFactories()
        {
            return ImmutableList.of(new BloomFilterAwareTpchConnectorFactory());
        }
    }

    private static class BloomFilterAwareTpchConnectorFactory
            implements ConnectorFactory
    {
        private final ConnectorFactory delegate = new TpchConnectorFactory();

        @Override
        public String getName()
        {
            return "bloom_filter_tpch";
        }

        @Override
        public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
        {
            return new BloomFilterAwareConnector(delegate.create(catalogName, config, context));
        }
    }

    private record BloomFilterAwareConnector(Connector delegate)
            implements Connector
    {
        @Override
        public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
        {
            return delegate.beginTransaction(isolationLevel, readOnly, autoCommit);
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
        {
            return delegate.getMetadata(session, transactionHandle);
        }

        @Override
        public ConnectorSplitManager getSplitManager()
        {
            return delegate.getSplitManager();
        }

        @Override
        public ConnectorPageSourceProvider getPageSourceProvider()
        {
            return delegate.getPageSourceProvider();
        }

        @Override
        public ConnectorNodePartitioningProvider getNodePartitioningProvider()
        {
            return delegate.getNodePartitioningProvider();
        }

        @Override
        public Set<BloomFilterKind> getSupportedDynamicFilterBloomFilterKinds()
        {
            return Set.of(BloomFilterKind.DATASKETCHES);
        }

        @Override
        public void shutdown()
        {
            delegate.shutdown();
        }
    }
}
