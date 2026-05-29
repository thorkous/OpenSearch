/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.get.DocumentLookupResult;
import org.opensearch.index.seqno.SequenceNumbers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Core orchestrator for doc-value access. Coordinates row location resolution,
 * backend-specific execution, and result building. Lives in the analytics core
 * layer so any backend can reuse the orchestration logic.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DocValueService implements DocValueProvider {

    private static final Logger logger = LogManager.getLogger(DocValueService.class);
    private static final String PARQUET_FORMAT = "parquet";

    private final RowLocator rowLocator;
    private final DocValueExecutor executor;

    public DocValueService(RowLocator rowLocator, DocValueExecutor executor) {
        this.rowLocator = rowLocator;
        this.executor = executor;
    }

    @Override
    public DocumentLookupResult getById(String id, IndexReaderProvider.Reader reader, String indexName) throws IOException {
        long[] rowLocation = rowLocator.resolveRowLocation(reader, id);
        if (rowLocation == null) {
            return DocumentLookupResult.notFound(id);
        }

        long rowId = rowLocation[0];
        long writerGeneration = rowLocation[1];
        WriterFileSet parquetSet = findParquetSet(reader.catalogSnapshot(), writerGeneration);

        String parquetFile = parquetSet.files().iterator().next();
        Map<String, Object> row = executor.executeSingleRow(parquetSet.directory(), parquetFile, indexName, rowId, writerGeneration);
        if (row == null) {
            logger.debug("get-by-id hit in locator but empty fetch for id=[{}] gen=[{}] rowId=[{}]", id, writerGeneration, rowId);
            return DocumentLookupResult.notFound(id);
        }

        return buildResultFromRow(id, row);
    }

    @Override
    public List<DocumentLookupResult> getDocsBySeqNoRange(long fromSeqNoExclusive, IndexReaderProvider.Reader reader, String indexName)
        throws IOException {
        CatalogSnapshot snapshot = reader.catalogSnapshot();
        List<DocumentLookupResult> results = new ArrayList<>();
        for (Segment segment : snapshot.getSegments()) {
            WriterFileSet parquetSet = segment.dfGroupedSearchableFiles().get(PARQUET_FORMAT);
            if (parquetSet == null || parquetSet.files().isEmpty()) continue;
            String parquetFile = parquetSet.files().iterator().next();
            List<Map<String, Object>> rows = executor.executeRangeQuery(
                parquetSet.directory(),
                parquetFile,
                indexName,
                fromSeqNoExclusive,
                parquetSet.writerGeneration()
            );
            for (Map<String, Object> row : rows) {
                String id = row.get("_id") != null ? row.get("_id").toString() : null;
                if (id == null) continue;
                results.add(buildResultFromRow(id, row));
            }
        }
        return results;
    }

    /** Builds a DocumentLookupResult from a raw row, filtering reserved/internal fields. */
    public static DocumentLookupResult buildResultFromRow(String id, Map<String, Object> row) throws IOException {
        long seqNo = extractLong(row, "_seq_no", SequenceNumbers.UNASSIGNED_SEQ_NO);
        long primaryTerm = extractLong(row, "_primary_term", SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
        long version = extractLong(row, "_version", Versions.NOT_FOUND);

        Map<String, Object> filtered = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : row.entrySet()) {
            if (DocValueFields.RESERVED.contains(e.getKey()) || e.getKey().startsWith("_")) continue;
            filtered.put(e.getKey(), e.getValue());
        }

        BytesReference source;
        try (XContentBuilder xcb = XContentFactory.jsonBuilder()) {
            xcb.map(filtered);
            source = BytesReference.bytes(xcb);
        }

        return new DocumentLookupResult(id, version, true, source, seqNo, primaryTerm, Map.of(), Map.of());
    }

    public static long extractLong(Map<String, Object> row, String key, long fallback) {
        Object v = row.get(key);
        if (v == null) return fallback;
        if (v instanceof Number) return ((Number) v).longValue();
        try {
            return Long.parseLong(v.toString());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static WriterFileSet findParquetSet(CatalogSnapshot snapshot, long writerGeneration) {
        for (Segment segment : snapshot.getSegments()) {
            WriterFileSet candidate = segment.dfGroupedSearchableFiles().get(PARQUET_FORMAT);
            if (candidate == null) continue;
            if (candidate.writerGeneration() != writerGeneration) continue;
            if (candidate.files().isEmpty()) continue;
            return candidate;
        }
        throw new IllegalStateException("No parquet file-set for writer_generation=" + writerGeneration + " in snapshot");
    }
}
