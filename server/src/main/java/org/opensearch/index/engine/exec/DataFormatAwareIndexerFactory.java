/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.Nullable;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.DataFormatAwareNRTReplicationEngine;
import org.opensearch.index.engine.DataFormatAwareReadOnlyEngine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.plugins.DocumentLookupProvider;

/**
 * {@link IndexerFactory} that creates a {@link DataFormatAwareEngine} for primaries
 * or a {@link DataFormatAwareNRTReplicationEngine} for read-only replicas,
 * used when the pluggable data format feature is enabled.
 *
 * @opensearch.internal
 */
public class DataFormatAwareIndexerFactory implements IndexerFactory {

    @Nullable
    private DocumentLookupProvider documentLookupProvider;

    /** Wires the optional {@link DocumentLookupProvider} used by {@link DataFormatAwareEngine#getById}. */
    public void setGetByIdPlugin(@Nullable DocumentLookupProvider documentLookupProvider) {
        this.documentLookupProvider = documentLookupProvider;
    }

    @Override
    public Indexer createIndexer(EngineConfig config) {
        if (config.isReadOnlyReplica()) {
            return new DataFormatAwareNRTReplicationEngine(config);
        } else if (config.getIndexSettings().isWarmIndex()) {
            return new DataFormatAwareReadOnlyEngine(config, documentLookupProvider);
        }
        return new DataFormatAwareEngine(config, documentLookupProvider);
    }
}
