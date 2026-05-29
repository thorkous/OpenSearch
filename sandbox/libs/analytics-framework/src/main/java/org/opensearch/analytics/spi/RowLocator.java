/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.index.engine.exec.IndexReaderProvider;

import java.io.IOException;

/**
 * Resolves a document id to its physical storage location (rowId + writerGeneration).
 * Implementations use the index's secondary structure (e.g., Lucene) to perform this mapping.
 *
 * @opensearch.internal
 */
public interface RowLocator {

    /**
     * Resolve the physical row location for a document id.
     *
     * @param reader the point-in-time reader snapshot
     * @param id the document id to locate
     * @return {@code long[]{rowId, writerGeneration}} or {@code null} if not found
     */
    long[] resolveRowLocation(IndexReaderProvider.Reader reader, String id) throws IOException;
}
