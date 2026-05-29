/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.analytics.spi.DocValueFields;
import org.opensearch.analytics.spi.DocValueService;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.get.DocumentLookupResult;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.substrait.proto.Plan;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GetServiceTests extends OpenSearchTestCase {

    private GetService.NativeExecutor mockExecutor;
    private GetService.SubstraitPlanFactory planFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockExecutor = mock(GetService.NativeExecutor.class);
        planFactory = new GetService.SubstraitPlanFactory();
    }

    public void testGetById_nullReader_returnsNotFound() throws IOException {
        DocValueService service = new GetService(reader -> null, planFactory, mockExecutor).toDocValueService();
        IndexReaderProvider.Reader mockReader = mock(IndexReaderProvider.Reader.class);

        DocumentLookupResult result = service.getById("doc1", mockReader, "idx");
        assertFalse(result.exists());
        assertEquals("doc1", result.id());
    }

    public void testGetById_emptyReader_returnsNotFound() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
        writer.commit();
        DirectoryReader emptyReader = DirectoryReader.open(dir);

        DocValueService service = new GetService(r -> emptyReader, planFactory, mockExecutor).toDocValueService();
        IndexReaderProvider.Reader mockReader = mock(IndexReaderProvider.Reader.class);

        DocumentLookupResult result = service.getById("doc1", mockReader, "idx");
        assertFalse(result.exists());

        emptyReader.close();
        writer.close();
        dir.close();
    }

    public void testGetById_idNotInIndex_returnsNotFound() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
        Document doc = new Document();
        doc.add(new StringField(IdFieldMapper.NAME, "other_id_value", org.apache.lucene.document.Field.Store.NO));
        writer.addDocument(doc);
        writer.commit();
        DirectoryReader luceneReader = DirectoryReader.open(dir);

        DocValueService service = new GetService(r -> luceneReader, planFactory, mockExecutor).toDocValueService();
        IndexReaderProvider.Reader mockReader = mock(IndexReaderProvider.Reader.class);

        DocumentLookupResult result = service.getById("missing", mockReader, "idx");
        assertFalse(result.exists());

        luceneReader.close();
        writer.close();
        dir.close();
    }

    public void testGetDocsBySeqNoRange_emptySnapshot() throws IOException {
        DocValueService service = new GetService(r -> null, planFactory, mockExecutor).toDocValueService();
        CatalogSnapshot snapshot = mock(CatalogSnapshot.class);
        when(snapshot.getSegments()).thenReturn(Collections.emptyList());
        IndexReaderProvider.Reader mockReader = mock(IndexReaderProvider.Reader.class);
        when(mockReader.catalogSnapshot()).thenReturn(snapshot);

        List<DocumentLookupResult> results = service.getDocsBySeqNoRange(-1, mockReader, "idx");
        assertTrue(results.isEmpty());
    }

    public void testGetDocsBySeqNoRange_segmentWithoutParquet() throws IOException {
        DocValueService service = new GetService(r -> null, planFactory, mockExecutor).toDocValueService();
        Segment segment = new Segment(1L, Map.of());

        CatalogSnapshot snapshot = mock(CatalogSnapshot.class);
        when(snapshot.getSegments()).thenReturn(List.of(segment));
        IndexReaderProvider.Reader mockReader = mock(IndexReaderProvider.Reader.class);
        when(mockReader.catalogSnapshot()).thenReturn(snapshot);

        List<DocumentLookupResult> results = service.getDocsBySeqNoRange(-1, mockReader, "idx");
        assertTrue(results.isEmpty());
    }

    public void testGetDocsBySeqNoRange_returnsResults() throws IOException {
        GetService.NativeExecutor executor = mock(GetService.NativeExecutor.class);
        when(executor.executeRangeQuery("/dir", "f.parquet", "idx", -1L, 5L)).thenReturn(
            List.of(
                Map.of("_id", "d1", "_seq_no", 1L, "_primary_term", 1L, "_version", 1L, "name", "alice"),
                Map.of("_id", "d2", "_seq_no", 2L, "_primary_term", 1L, "_version", 2L, "name", "bob")
            )
        );

        DocValueService service = new GetService(r -> null, planFactory, executor).toDocValueService();

        WriterFileSet pset = new WriterFileSet("/dir", 5L, Set.of("f.parquet"), 10L, 100L);
        Segment segment = new Segment(1L, Map.of("parquet", pset));

        CatalogSnapshot snapshot = mock(CatalogSnapshot.class);
        when(snapshot.getSegments()).thenReturn(List.of(segment));
        IndexReaderProvider.Reader mockReader = mock(IndexReaderProvider.Reader.class);
        when(mockReader.catalogSnapshot()).thenReturn(snapshot);

        List<DocumentLookupResult> results = service.getDocsBySeqNoRange(-1, mockReader, "idx");
        assertEquals(2, results.size());
        assertEquals("d1", results.get(0).id());
        assertEquals(1L, results.get(0).seqNo());
        assertTrue(results.get(0).exists());
        assertEquals("d2", results.get(1).id());
    }

    public void testGetDocsBySeqNoRange_skipsNullId() throws IOException {
        GetService.NativeExecutor executor = mock(GetService.NativeExecutor.class);
        when(executor.executeRangeQuery("/dir", "f.parquet", "idx", -1L, 5L)).thenReturn(
            List.of(Map.of("_seq_no", 1L, "_primary_term", 1L, "_version", 1L))
        );

        DocValueService service = new GetService(r -> null, planFactory, executor).toDocValueService();

        WriterFileSet pset = new WriterFileSet("/dir", 5L, Set.of("f.parquet"), 10L, 100L);
        Segment segment = new Segment(1L, Map.of("parquet", pset));

        CatalogSnapshot snapshot = mock(CatalogSnapshot.class);
        when(snapshot.getSegments()).thenReturn(List.of(segment));
        IndexReaderProvider.Reader mockReader = mock(IndexReaderProvider.Reader.class);
        when(mockReader.catalogSnapshot()).thenReturn(snapshot);

        List<DocumentLookupResult> results = service.getDocsBySeqNoRange(-1, mockReader, "idx");
        assertTrue(results.isEmpty());
    }

    // --- SubstraitPlanFactory ---

    public void testBuildFetchPlan_validProtobuf() throws Exception {
        byte[] bytes = planFactory.buildFetchPlan("my_table", 42L);
        assertNotNull(bytes);
        assertTrue(bytes.length > 0);

        Plan plan = Plan.parseFrom(bytes);
        assertEquals(1, plan.getRelationsCount());
        assertTrue(plan.getRelations(0).hasRoot());
        assertTrue(plan.getRelations(0).getRoot().getInput().hasFetch());
        assertEquals(42L, plan.getRelations(0).getRoot().getInput().getFetch().getOffset());
        assertEquals(1L, plan.getRelations(0).getRoot().getInput().getFetch().getCount());
    }

    // --- extractLong (via reflection) ---

    public void testExtractLong_number() throws Exception {
        assertEquals(42L, invokeExtractLong(Map.of("k", 42), "k", -1L));
        assertEquals(42L, invokeExtractLong(Map.of("k", 42L), "k", -1L));
        assertEquals(42L, invokeExtractLong(Map.of("k", 42.9), "k", -1L));
    }

    public void testExtractLong_string() throws Exception {
        assertEquals(99L, invokeExtractLong(Map.of("k", "99"), "k", -1L));
    }

    public void testExtractLong_missing_returnsFallback() throws Exception {
        assertEquals(-1L, invokeExtractLong(Map.of(), "k", -1L));
    }

    public void testExtractLong_invalidString_returnsFallback() throws Exception {
        assertEquals(-1L, invokeExtractLong(Map.of("k", "abc"), "k", -1L));
    }

    public void testReservedFields() {
        Set<String> reserved = DocValueFields.RESERVED;
        assertTrue(reserved.contains("_id"));
        assertTrue(reserved.contains("_seq_no"));
        assertTrue(reserved.contains("_primary_term"));
        assertTrue(reserved.contains("_version"));
    }

    public void testGetById_docFoundInLucene_returnsFromParquet() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        DirectoryReader luceneReader = createLuceneIndexWithDoc(dir, "1");

        GetService.NativeExecutor executor = mock(GetService.NativeExecutor.class);
        when(executor.executeSingleRow("/parquet", "data.parquet", "idx", 0L, 1L)).thenReturn(
            Map.of("_id", "doc1", "_seq_no", 5L, "_primary_term", 1L, "_version", 2L, "field1", "value1")
        );

        WriterFileSet pset = new WriterFileSet("/parquet", 1L, Set.of("data.parquet"), 1L, 100L);
        Segment segment = new Segment(1L, Map.of("parquet", pset));
        CatalogSnapshot snapshot = mock(CatalogSnapshot.class);
        when(snapshot.getSegments()).thenReturn(List.of(segment));
        IndexReaderProvider.Reader mockReader = mock(IndexReaderProvider.Reader.class);
        when(mockReader.catalogSnapshot()).thenReturn(snapshot);

        DocValueService service = new GetService(r -> luceneReader, planFactory, executor).toDocValueService();
        DocumentLookupResult result = service.getById("doc1", mockReader, "idx");

        assertTrue("doc should be found", result.exists());
        assertEquals("doc1", result.id());
        assertEquals(5L, result.seqNo());
        assertEquals(2L, result.version());

        luceneReader.close();
        dir.close();
    }

    /** Creates a Lucene index with one doc (id="doc1", __row_id__=0, writer_generation=genValue). */
    private DirectoryReader createLuceneIndexWithDoc(Directory dir, String genValue) throws IOException {
        Codec baseCodec = Codec.getDefault();
        Codec codec = new FilterCodec(baseCodec.getName(), baseCodec) {
            @Override
            public SegmentInfoFormat segmentInfoFormat() {
                final SegmentInfoFormat delegate = baseCodec.segmentInfoFormat();
                return new SegmentInfoFormat() {
                    @Override
                    public org.apache.lucene.index.SegmentInfo read(Directory d, String s, byte[] id, org.apache.lucene.store.IOContext ctx)
                        throws IOException {
                        return delegate.read(d, s, id, ctx);
                    }

                    @Override
                    public void write(Directory d, org.apache.lucene.index.SegmentInfo info, org.apache.lucene.store.IOContext ctx)
                        throws IOException {
                        info.putAttribute("writer_generation", genValue);
                        delegate.write(d, info, ctx);
                    }
                };
            }
        };
        IndexWriterConfig iwc = new IndexWriterConfig();
        iwc.setCodec(codec);
        IndexWriter writer = new IndexWriter(dir, iwc);
        Document doc = new Document();
        doc.add(new StringField(IdFieldMapper.NAME, Uid.encodeId("doc1"), org.apache.lucene.document.Field.Store.NO));
        doc.add(new org.apache.lucene.document.SortedNumericDocValuesField("__row_id__", 0L));
        writer.addDocument(doc);
        writer.commit();
        writer.close();
        return DirectoryReader.open(dir);
    }

    public void testGetById_findParquetSetThrows_whenGenerationMismatch() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        DirectoryReader luceneReader = createLuceneIndexWithDoc(dir, "1");

        // CatalogSnapshot has generation=99, but segment has generation=1 → no match → throws
        WriterFileSet pset = new WriterFileSet("/parquet", 99L, Set.of("data.parquet"), 1L, 100L);
        Segment segment = new Segment(99L, Map.of("parquet", pset));
        CatalogSnapshot snapshot = mock(CatalogSnapshot.class);
        when(snapshot.getSegments()).thenReturn(List.of(segment));
        IndexReaderProvider.Reader mockReader = mock(IndexReaderProvider.Reader.class);
        when(mockReader.catalogSnapshot()).thenReturn(snapshot);

        DocValueService service = new GetService(r -> luceneReader, planFactory, mockExecutor).toDocValueService();
        expectThrows(IllegalStateException.class, () -> service.getById("doc1", mockReader, "idx"));

        luceneReader.close();
        dir.close();
    }

    public void testGetById_returnsNotFound_whenExecutorReturnsNull() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        DirectoryReader luceneReader = createLuceneIndexWithDoc(dir, "1");

        GetService.NativeExecutor executor = mock(GetService.NativeExecutor.class);
        when(executor.executeSingleRow("/parquet", "data.parquet", "idx", 0L, 1L)).thenReturn(null);

        WriterFileSet pset = new WriterFileSet("/parquet", 1L, Set.of("data.parquet"), 1L, 100L);
        Segment segment = new Segment(1L, Map.of("parquet", pset));
        CatalogSnapshot snapshot = mock(CatalogSnapshot.class);
        when(snapshot.getSegments()).thenReturn(List.of(segment));
        IndexReaderProvider.Reader mockReader = mock(IndexReaderProvider.Reader.class);
        when(mockReader.catalogSnapshot()).thenReturn(snapshot);

        DocValueService service = new GetService(r -> luceneReader, planFactory, executor).toDocValueService();
        DocumentLookupResult result = service.getById("doc1", mockReader, "idx");
        assertFalse("should return notFound when executor returns null", result.exists());

        luceneReader.close();
        dir.close();
    }

    public void testGetById_throws_whenWriterGenerationMissing() throws IOException {
        // Use default codec (no writer_generation attribute set)
        Directory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
        Document doc = new Document();
        doc.add(new StringField(IdFieldMapper.NAME, Uid.encodeId("doc1"), org.apache.lucene.document.Field.Store.NO));
        doc.add(new org.apache.lucene.document.SortedNumericDocValuesField("__row_id__", 0L));
        writer.addDocument(doc);
        writer.commit();
        DirectoryReader luceneReader = DirectoryReader.open(dir);

        IndexReaderProvider.Reader mockReader = mock(IndexReaderProvider.Reader.class);
        DocValueService service = new GetService(r -> luceneReader, planFactory, mockExecutor).toDocValueService();
        expectThrows(IllegalStateException.class, () -> service.getById("doc1", mockReader, "idx"));

        luceneReader.close();
        writer.close();
        dir.close();
    }

    public void testGetById_throws_whenWriterGenerationInvalid() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        DirectoryReader luceneReader = createLuceneIndexWithDoc(dir, "not_a_number");

        IndexReaderProvider.Reader mockReader = mock(IndexReaderProvider.Reader.class);
        DocValueService service = new GetService(r -> luceneReader, planFactory, mockExecutor).toDocValueService();
        expectThrows(IllegalStateException.class, () -> service.getById("doc1", mockReader, "idx"));

        luceneReader.close();
        dir.close();
    }

    public void testGetById_throws_whenRowIdDocValuesMissing() throws IOException {
        // Index a doc with _id but WITHOUT __row_id__ doc values
        Directory dir = new ByteBuffersDirectory();
        Codec baseCodec = Codec.getDefault();
        Codec codec = new FilterCodec(baseCodec.getName(), baseCodec) {
            @Override
            public SegmentInfoFormat segmentInfoFormat() {
                final SegmentInfoFormat delegate = baseCodec.segmentInfoFormat();
                return new SegmentInfoFormat() {
                    @Override
                    public org.apache.lucene.index.SegmentInfo read(Directory d, String s, byte[] id, org.apache.lucene.store.IOContext ctx)
                        throws IOException {
                        return delegate.read(d, s, id, ctx);
                    }

                    @Override
                    public void write(Directory d, org.apache.lucene.index.SegmentInfo info, org.apache.lucene.store.IOContext ctx)
                        throws IOException {
                        info.putAttribute("writer_generation", "1");
                        delegate.write(d, info, ctx);
                    }
                };
            }
        };
        IndexWriterConfig iwc = new IndexWriterConfig();
        iwc.setCodec(codec);
        IndexWriter writer = new IndexWriter(dir, iwc);
        Document doc = new Document();
        doc.add(new StringField(IdFieldMapper.NAME, Uid.encodeId("doc1"), org.apache.lucene.document.Field.Store.NO));
        // Intentionally NO __row_id__ field
        writer.addDocument(doc);
        writer.commit();
        writer.close();
        DirectoryReader luceneReader = DirectoryReader.open(dir);

        IndexReaderProvider.Reader mockReader = mock(IndexReaderProvider.Reader.class);
        DocValueService service = new GetService(r -> luceneReader, planFactory, mockExecutor).toDocValueService();
        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> service.getById("doc1", mockReader, "idx"));
        assertTrue(ex.getMessage().contains("__row_id__"));

        luceneReader.close();
        dir.close();
    }

    private static long invokeExtractLong(Map<String, Object> row, String key, long fallback) {
        return DocValueService.extractLong(row, key, fallback);
    }
}
