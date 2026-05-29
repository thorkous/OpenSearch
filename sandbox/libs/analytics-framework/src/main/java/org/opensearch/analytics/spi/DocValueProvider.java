/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.get.DocumentLookupResult;

import java.io.IOException;
import java.util.List;

/**
 * SPI for accessing document values from a backend's storage layer.
 * Supports both random point lookups (by document id) and batch range scans
 * (by sequence number range).
 *
 * <p>Backends implement this interface to expose doc-value access to the
 * analytics core layer. The engine-level {@code DocumentLookupProvider}
 * bridges to this SPI for internal operations (version resolution, restore).
 *
 * @opensearch.internal
 */
public interface DocValueProvider {

    /**
     * Fetch a single document by its {@code _id}.
     *
     * @param id the document id
     * @param reader a point-in-time reader snapshot
     * @param indexName the index name used as the scan table
     * @return the lookup result; never null
     */
    DocumentLookupResult getById(String id, IndexReaderProvider.Reader reader, String indexName) throws IOException;

    /**
     * Fetch all documents with {@code _seq_no > fromSeqNoExclusive}.
     *
     * @param fromSeqNoExclusive the exclusive lower bound on sequence number
     * @param reader a point-in-time reader snapshot
     * @param indexName the index name used as the scan table
     * @return list of matching documents; empty if none
     */
    List<DocumentLookupResult> getDocsBySeqNoRange(long fromSeqNoExclusive, IndexReaderProvider.Reader reader, String indexName)
        throws IOException;
}
