/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Backend-specific execution contract for doc-value reads. Backends implement this
 * to perform the actual storage read (e.g., DataFusion native parquet scan).
 *
 * @opensearch.internal
 */
public interface DocValueExecutor {

    /**
     * Execute a single-row fetch from the given parquet file at the specified row offset.
     *
     * @param directory the directory containing the parquet file
     * @param parquetFile the parquet file name
     * @param tableName the logical table name for the query
     * @param rowId the row offset to fetch
     * @param writerGeneration the writer generation (for table aliasing)
     * @return the row as a field-name → value map, or null if not found
     */
    Map<String, Object> executeSingleRow(String directory, String parquetFile, String tableName, long rowId, long writerGeneration)
        throws IOException;

    /**
     * Execute a range query returning all rows matching the predicate.
     *
     * @param directory the directory containing parquet files
     * @param parquetFile the parquet file name
     * @param tableName the logical table name
     * @param fromSeqNoExclusive the exclusive lower bound on _seq_no
     * @param writerGeneration the writer generation
     * @return list of rows as field-name → value maps
     */
    List<Map<String, Object>> executeRangeQuery(
        String directory,
        String parquetFile,
        String tableName,
        long fromSeqNoExclusive,
        long writerGeneration
    ) throws IOException;
}
