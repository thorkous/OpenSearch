/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.opensearch.analytics.spi.DocValueExecutor;
import org.opensearch.analytics.spi.DocValueService;
import org.opensearch.analytics.spi.RowLocator;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.ReaderHandle;
import org.opensearch.be.datafusion.nativelib.StreamHandle;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.engine.exec.MonoFileWriterSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import io.substrait.proto.FetchRel;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelRoot;
import io.substrait.proto.Type;

/**
 * Non-Lucene get-by-id implementation for DataFusion-backed indexes.
 *
 * <p>Resolves the {@code _id} term via the sibling Lucene backend's {@code DirectoryReader},
 * maps the matching global docId to a {@code (writerGeneration, rowId)} pair, locates the
 * parquet file in the {@link CatalogSnapshot}, and issues a Substrait
 * {@code Fetch(offset=rowId, count=1) → Read(NamedTable=[indexName])} plan through the
 * DataFusion native runtime. The single returned Arrow record batch is flattened into a
 * JSON source with reserved columns stripped.
 */
@ExperimentalApi
public class GetService {

    private static final Logger logger = LogManager.getLogger(GetService.class);

    private static final String PARQUET_FORMAT = "parquet";

    private final LuceneReaderAccessor luceneReaderAccessor;
    private final SubstraitPlanFactory planFactory;
    private final NativeExecutor executor;

    /** Production constructor. */
    public GetService(DataFusionPlugin dfPlugin) {
        this(new RegistryLuceneReaderAccessor(requireRegistry(dfPlugin)), new SubstraitPlanFactory(), new NativeBridgeExecutor(dfPlugin));
    }

    private static DataFormatRegistry requireRegistry(DataFusionPlugin dfPlugin) {
        DataFormatRegistry registry = dfPlugin.getDataFormatRegistry();
        if (registry == null) {
            throw new IllegalStateException("DataFormatRegistry not initialized on DataFusionPlugin");
        }
        return registry;
    }

    /** Test constructor accepting seam interfaces. */
    GetService(LuceneReaderAccessor luceneReaderAccessor, SubstraitPlanFactory planFactory, NativeExecutor executor) {
        this.luceneReaderAccessor = luceneReaderAccessor;
        this.planFactory = planFactory;
        this.executor = executor;
    }

    /** Returns a core-layer DocValueService wired with this backend's executor and row locator. */
    public DocValueService toDocValueService() {
        return new DocValueService(luceneReaderAccessor, executor);
    }

    /**
     * Strategy for locating a document's physical row in the storage layer.
     * Production code resolves via the Lucene secondary index; tests supply a direct override.
     */
    interface LuceneReaderAccessor extends RowLocator {
        DirectoryReader directoryReader(IndexReaderProvider.Reader reader) throws IOException;

        /**
         * Resolves the physical row location for a document id.
         * @return {@code long[]{rowId, writerGeneration}} or {@code null} if not found
         */
        default long[] resolveRowLocation(IndexReaderProvider.Reader reader, String id) throws IOException {
            DirectoryReader luceneReader = directoryReader(reader);
            if (luceneReader == null) {
                return null;
            }
            BytesRef idBytes = Uid.encodeId(id);
            TopDocs topDocs = new IndexSearcher(luceneReader).search(new TermQuery(new Term(IdFieldMapper.NAME, idBytes)), 1);
            if (topDocs.scoreDocs.length == 0) {
                return null;
            }
            int docId = topDocs.scoreDocs[0].doc;
            int leafOrd = ReaderUtil.subIndex(docId, luceneReader.leaves());
            LeafReaderContext leafCtx = luceneReader.leaves().get(leafOrd);
            int localDocId = docId - leafCtx.docBase;

            SortedNumericDocValues rowIdDv = leafCtx.reader().getSortedNumericDocValues(DocumentInput.ROW_ID_FIELD);
            if (rowIdDv == null || !rowIdDv.advanceExact(localDocId)) {
                throw new IllegalStateException("Leaf segment missing " + DocumentInput.ROW_ID_FIELD + " doc values");
            }
            long rowId = rowIdDv.nextValue();

            LeafReader unwrapped = FilterLeafReader.unwrap(leafCtx.reader());
            if (!(unwrapped instanceof SegmentReader)) {
                throw new IllegalStateException("Expected SegmentReader leaf, got " + unwrapped.getClass());
            }
            String genAttr = ((SegmentReader) unwrapped).getSegmentInfo().info.getAttribute("writer_generation");
            if (genAttr == null) {
                throw new IllegalStateException("Leaf segment missing writer_generation attribute");
            }
            long writerGeneration;
            try {
                writerGeneration = Long.parseLong(genAttr);
            } catch (NumberFormatException e) {
                throw new IllegalStateException("Invalid writer_generation attribute: [" + genAttr + "]", e);
            }
            return new long[] { rowId, writerGeneration };
        }
    }

    /**
     * Registry-based {@link LuceneReaderAccessor}. Looks up the Lucene {@link DataFormat} by name
     * on the {@link DataFormatRegistry}. Avoids a compile-time dep on the Lucene backend module
     * (which would invert the module graph: DF is downstream of analytics-engine, not of
     * analytics-backend-lucene) and avoids reflection across plugin classloaders (each plugin has
     * its own isolated classloader at runtime).
     */
    static final class RegistryLuceneReaderAccessor implements LuceneReaderAccessor {
        private static final String LUCENE_FORMAT_NAME = "lucene";
        private final DataFormatRegistry registry;

        RegistryLuceneReaderAccessor(DataFormatRegistry registry) {
            this.registry = registry;
        }

        @Override
        public DirectoryReader directoryReader(IndexReaderProvider.Reader reader) {
            DataFormat luceneFormat;
            DataFormatPlugin plugin = registry.getPlugin("lucene");
            if (plugin == null) {
                return null;
            }
            try {
                luceneFormat = registry.format(LUCENE_FORMAT_NAME);
            } catch (IllegalArgumentException e) {
                return null;
            }
            Object readerObj = reader.getReader(luceneFormat, Object.class);

            if (readerObj instanceof DirectoryReader dr) {
                return dr;
            }
            // LuceneReader is a record in analytics-backend-lucene with a directoryReader() accessor.
            Class<?> readerClass = readerObj.getClass();
            if (!"org.opensearch.be.lucene.LuceneReader".equals(readerClass.getName())) {
                throw new IllegalStateException("Reader for format [lucene] has unexpected type: " + readerClass.getName());
            }
            // Verify the object was loaded by the same classloader as the lucene plugin.
            ClassLoader pluginLoader = plugin.getClass().getClassLoader();
            ClassLoader readerLoader = readerClass.getClassLoader();
            if (readerLoader == null || readerLoader != pluginLoader) {
                throw new IllegalStateException(
                    "Reader for format [lucene] loaded from unexpected classloader — expected plugin classloader"
                );
            }
            try {
                var method = readerObj.getClass().getMethod("directoryReader");
                Object result = method.invoke(readerObj);
                if (!(result instanceof DirectoryReader)) {
                    throw new IllegalStateException(
                        "directoryReader() returned " + (result == null ? "null" : result.getClass().getName())
                    );
                }
                return (DirectoryReader) result;
            } catch (Exception e) {
                throw new IllegalStateException(
                    "Reader for format [lucene] is " + readerObj.getClass().getName() + ", cannot extract DirectoryReader",
                    e
                );
            }
        }
    }

    /**
     * Builds the narrow Substrait "fetch-one-row-from-named-table" plan. Exposed as a seam for
     * unit tests that want to assert plan shape without running the native runtime.
     */
    static final class SubstraitPlanFactory {
        byte[] buildFetchPlan(String tableName, long offset) {
            // Empty NamedStruct satisfies the required-presence contract; DataFusion's listing
            // table resolves the actual parquet schema from the file footer at read time.
            NamedStruct baseSchema = NamedStruct.newBuilder().setStruct(Type.Struct.newBuilder().build()).build();
            Rel read = Rel.newBuilder()
                .setRead(
                    ReadRel.newBuilder()
                        .setBaseSchema(baseSchema)
                        .setNamedTable(ReadRel.NamedTable.newBuilder().addNames(tableName).build())
                        .build()
                )
                .build();
            Rel fetch = Rel.newBuilder().setFetch(FetchRel.newBuilder().setInput(read).setOffset(offset).setCount(1L).build()).build();
            RelRoot root = RelRoot.newBuilder().setInput(fetch).build();
            Plan plan = Plan.newBuilder().addRelations(PlanRel.newBuilder().setRoot(root).build()).build();
            return plan.toByteArray();
        }
    }

    /**
     * Executes a {@code SELECT * FROM tableName LIMIT 1 OFFSET rowId} query over a single parquet
     * file via the DataFusion native bridge and returns the first row as a map. Uses
     * {@link NativeBridge#sqlToSubstrait} to let the native planner project the parquet file's
     * true schema — passing a hand-rolled Substrait plan with an empty {@code base_schema} yields
     * zero-column projection.
     */
    interface NativeExecutor extends DocValueExecutor {
        Map<String, Object> executeSingleRow(String parquetDir, String parquetFile, String tableName, long rowId, long writerGeneration)
            throws IOException;

        List<Map<String, Object>> executeRangeQuery(
            String parquetDir,
            String parquetFile,
            String tableName,
            long seqNoFloor,
            long writerGeneration
        ) throws IOException;
    }

    /**
     * Production {@link NativeExecutor} driving {@link NativeBridge}. Spins up a short-lived
     * reader scoped to one parquet file, executes the plan, imports the single resulting batch
     * via the Arrow C Data Interface, and flattens the first row into a Java map.
     */
    static final class NativeBridgeExecutor implements NativeExecutor {

        private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC);
        private static final String GET_BY_ID_TABLE_ALIAS = "_t";

        private final DataFusionPlugin dfPlugin;

        NativeBridgeExecutor(DataFusionPlugin dfPlugin) {
            this.dfPlugin = dfPlugin;
        }

        @Override
        public Map<String, Object> executeSingleRow(
            String parquetDir,
            String parquetFile,
            String tableName,
            long rowId,
            long writerGeneration
        ) throws IOException {
            long runtimePtr = dfPlugin.getDataFusionService().getNativeRuntime().get();
            // ReaderHandle registers the native pointer with NativeHandle so downstream
            // validatePointer() calls in executeQueryAsync() find it in the live set.
            MonoFileWriterSet segment = MonoFileWriterSet.of(parquetDir, writerGeneration, parquetFile, 0L);
            try (ReaderHandle readerHandle = new ReaderHandle(parquetDir, List.of(segment), null)) {
                long readerPtr = readerHandle.getPointer();
                if (rowId < 0) {
                    throw new IllegalArgumentException("rowId must be non-negative, got: " + rowId);
                }
                String sql = "SELECT * FROM \"" + GET_BY_ID_TABLE_ALIAS + "\" LIMIT 1 OFFSET " + Long.toUnsignedString(rowId);
                byte[] substraitPlan = NativeBridge.sqlToSubstrait(readerPtr, GET_BY_ID_TABLE_ALIAS, sql, runtimePtr);
                WireConfigSnapshot configSnapshot = WireConfigSnapshot.builder(dfPlugin.getDatafusionSettings().getSnapshot())
                    .queryStrategy(1) // ListingTable — bypass indexed executor routing
                    .build();
                long streamPtr = executeNativeQuery(
                    readerPtr,
                    substraitPlan,
                    runtimePtr,
                    configSnapshot,
                    "DataFusion get-by-id query failed"
                );
                return readSingleRow(streamPtr);
            }
        }

        @Override
        public List<Map<String, Object>> executeRangeQuery(
            String parquetDir,
            String parquetFile,
            String tableName,
            long seqNoFloor,
            long writerGeneration
        ) throws IOException {
            long runtimePtr = dfPlugin.getDataFusionService().getNativeRuntime().get();
            MonoFileWriterSet segment = MonoFileWriterSet.of(parquetDir, writerGeneration, parquetFile, 0L);
            try (ReaderHandle readerHandle = new ReaderHandle(parquetDir, List.of(segment), null)) {
                long readerPtr = readerHandle.getPointer();
                String sql = "SELECT \"_id\", \"_seq_no\", \"_primary_term\", \"_version\" FROM \""
                    + GET_BY_ID_TABLE_ALIAS
                    + "\" WHERE \"_seq_no\" > "
                    + seqNoFloor;
                byte[] substraitPlan = NativeBridge.sqlToSubstrait(readerPtr, GET_BY_ID_TABLE_ALIAS, sql, runtimePtr);
                WireConfigSnapshot configSnapshot = dfPlugin.getDatafusionSettings().getSnapshot();
                long streamPtr = executeNativeQuery(readerPtr, substraitPlan, runtimePtr, configSnapshot, "DataFusion range query failed");
                return readAllRows(streamPtr);
            }
        }

        private Map<String, Object> readSingleRow(long streamPtr) throws IOException {
            try (
                BufferAllocator allocator = new RootAllocator(16 * 1024 * 1024);
                StreamHandle streamHandle = new StreamHandle(streamPtr, dfPlugin.getDataFusionService().getNativeRuntime());
                DatafusionResultStream stream = new DatafusionResultStream(streamHandle, allocator)
            ) {
                var iter = stream.iterator();
                if (!iter.hasNext()) return null;
                var batch = iter.next();
                try (VectorSchemaRoot root = batch.getArrowRoot()) {
                    if (root.getRowCount() == 0) return null;
                    return rowToMap(root, 0);
                }
            }
        }

        private List<Map<String, Object>> readAllRows(long streamPtr) throws IOException {
            List<Map<String, Object>> results = new ArrayList<>();
            try (
                BufferAllocator allocator = new RootAllocator(64 * 1024 * 1024);
                StreamHandle streamHandle = new StreamHandle(streamPtr, dfPlugin.getDataFusionService().getNativeRuntime());
                DatafusionResultStream stream = new DatafusionResultStream(streamHandle, allocator)
            ) {
                var iter = stream.iterator();
                while (iter.hasNext()) {
                    var batch = iter.next();
                    try (VectorSchemaRoot root = batch.getArrowRoot()) {
                        FieldVector idVec = root.getVector(IdFieldMapper.NAME);
                        for (int i = 0; i < root.getRowCount(); i++) {
                            Map<String, Object> row = rowToMap(root, i);
                            if (idVec != null && !idVec.isNull(i)) {
                                row.put(IdFieldMapper.NAME, Uid.decodeId((byte[]) idVec.getObject(i)));
                            }
                            results.add(row);
                        }
                    }
                }
            }
            return results;
        }

        private long executeNativeQuery(
            long readerPtr,
            byte[] substraitPlan,
            long runtimePtr,
            WireConfigSnapshot configSnapshot,
            String errorMessage
        ) throws IOException {
            CompletableFuture<Long> future = new CompletableFuture<>();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment configSegment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
                configSnapshot.writeTo(configSegment);
                NativeBridge.executeQueryAsync(
                    readerPtr,
                    GET_BY_ID_TABLE_ALIAS,
                    substraitPlan,
                    runtimePtr,
                    0L,
                    configSegment.address(),
                    new ActionListener<>() {
                        @Override
                        public void onResponse(Long v) {
                            future.complete(v);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            future.completeExceptionally(e);
                        }
                    }
                );
                try {
                    return future.join();
                } catch (Exception e) {
                    throw new IOException(errorMessage, e);
                }
            }
        }

        private static Map<String, Object> rowToMap(VectorSchemaRoot root, int rowIdx) {
            Map<String, Object> out = new LinkedHashMap<>();
            for (Field field : root.getSchema().getFields()) {
                String name = field.getName();
                FieldVector vec = root.getVector(name);
                Object converted = convert(vec, rowIdx);
                if (converted != null) {
                    out.put(name, converted);
                }
            }
            return out;
        }

        private static Object convert(FieldVector vec, int idx) {
            if (vec == null || vec.isNull(idx)) return null;
            ArrowType type = vec.getField().getType();
            ArrowType.ArrowTypeID id = type.getTypeID();
            switch (id) {
                case Binary:
                case LargeBinary:
                case FixedSizeBinary:
                case BinaryView:
                    return null;
                default:
                    break;
            }
            Object raw = vec.getObject(idx);
            switch (id) {
                case Utf8:
                case LargeUtf8:
                case Utf8View, Date:
                    return raw == null ? null : raw.toString();
                case Int:
                    return raw instanceof Number ? ((Number) raw).longValue() : raw;
                case FloatingPoint:
                    return raw instanceof Number ? ((Number) raw).doubleValue() : raw;
                case Bool:
                    return raw;
                case Timestamp:
                    if (raw instanceof Number) {
                        ArrowType.Timestamp ts = (ArrowType.Timestamp) type;
                        return ISO_FORMATTER.format(toInstant(((Number) raw).longValue(), ts.getUnit()));
                    }
                    return raw == null ? null : raw.toString();
                default:
                    // TODO type coverage (list, struct, decimal)
                    return null;
            }
        }

        private static Instant toInstant(long v, TimeUnit unit) {
            switch (unit) {
                case SECOND:
                    return Instant.ofEpochSecond(v);
                case MILLISECOND:
                    return Instant.ofEpochMilli(v);
                case MICROSECOND:
                    return Instant.ofEpochSecond(v / 1_000_000L, (v % 1_000_000L) * 1_000L);
                case NANOSECOND:
                default:
                    return Instant.ofEpochSecond(v / 1_000_000_000L, v % 1_000_000_000L);
            }
        }
    }

}
