package org.opensearch.be.lucene.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.Term;
import org.opensearch.be.lucene.LuceneDataFormat;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DeleteExecutionEngine;
import org.opensearch.index.engine.dataformat.DeleteInput;
import org.opensearch.index.engine.dataformat.DeleteResult;
import org.opensearch.index.engine.dataformat.Deleter;
import org.opensearch.index.engine.dataformat.DeleterImpl;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.RefreshResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.commit.Committer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Lucene-based implementation of {@link DeleteExecutionEngine} that buffers all deletes
 * and applies them at controlled points during the refresh cycle.
 *
 * <p>All deletes are buffered — never applied immediately. During refresh:
 * <ul>
 *   <li>Deletes for child writers are applied before flush (baked into flushed segment)</li>
 *   <li>Deletes for parent are applied after addIndexes (resolved by NRT reader open)</li>
 * </ul>
 *
 * <p>Routing logic in {@link #deleteDocument}: if the generation is -1 or has been marked
 * as flushed, to delete targets the parent. Otherwise, it targets the child writer.
 *
 * @opensearch.experimental
 */
public class LuceneDeleteExecutionEngine implements DeleteExecutionEngine<DataFormat> {

    private static final Logger logger = LogManager.getLogger(LuceneDeleteExecutionEngine.class);

    /** Deletes targeting child writers, keyed by generation. Applied before flush. */
    private final BufferedDeletes bufferedDeletes = new BufferedDeletes();

    /** Maps generation to deleter (holds writer reference for applying deletes) */
    private final Map<Long, Deleter> generationToDeleterMap = new ConcurrentHashMap<>();

    private final DataFormat dataFormat;
    private final LuceneCommitter committer;

    public LuceneDeleteExecutionEngine(DataFormat dataFormat, Committer committer) {
        this.dataFormat = dataFormat;
        this.committer = (LuceneCommitter) committer;
    }

    @Override
    public Deleter createDeleter(Writer<?> writer) {
        LuceneWriter luceneWriter = writer.getWriterForFormat(LuceneDataFormat.LUCENE_FORMAT_NAME)
            .map(w -> (LuceneWriter) w)
            .orElseThrow(
                () -> new IllegalArgumentException("Cannot create deleter: no Lucene writer found for generation=" + writer.generation())
            );
        Deleter deleter = new DeleterImpl<>(luceneWriter);
        generationToDeleterMap.put(writer.generation(), deleter);
        return deleter;
    }

    /**
     * Buffers a deleted. Routes to parent or child writer buffer based on generation and flush state.
     */
    @Override
    public DeleteResult deleteDocument(DeleteInput input) throws IOException {
        bufferedDeletes.add(input);
        return new DeleteResult.Success(1L, 1L, 1L);
    }

    /**
     * Applies buffered deletes for a specific generation to the child writer via its deleter.
     * Called by the refresh thread BEFORE flush. Marks the generation as flushed so that
     * any late-arriving deletes route to parent.
     *
     * @param generation the writer generation
     */
    public void applyDeletesForGeneration(long generation) throws IOException {
        bufferedDeletes.markFlushed(generation);
        Queue<DeleteInput> deletes = bufferedDeletes.removeWriterDeletes(generation);
        if (deletes == null) {
            return;
        }
        try (Deleter deleter = generationToDeleterMap.remove(generation)) {
            if (deleter == null) {
                bufferedDeletes.addAllToParent(deletes);
                return;
            }
            for (DeleteInput input : deletes) {
                deleter.deleteDoc(input);
            }
        }
    }

    /**
     * Applies parent deletes for docs already in parent (gen=-1).
     * Called BEFORE addIndexes. Resolved by addIndexes internal flush.
     */
    public void applyParentDeletesBeforeAddIndexes() throws IOException {
        List<Term> terms = new ArrayList<>();
        DeleteInput input;
        while ((input = bufferedDeletes.pollParentDeleteBeforeAddIndexes()) != null) {
            terms.add(new Term(input.fieldName(), input.value()));
        }
        if (!terms.isEmpty()) {
            committer.getIndexWriter().deleteDocuments(terms.toArray(new Term[0]));
        }
    }

    /**
     * Applies parent deletes for docs that arrived via addIndexes (flushed gens).
     * Called AFTER addIndexes. Resolved by DirectoryReader.open().
     * Clears flushedGenerations for next cycle.
     */
    public void applyParentDeletesAfterAddIndexes() throws IOException {
        List<Term> terms = new ArrayList<>();
        DeleteInput input;
        while ((input = bufferedDeletes.pollParentDeleteAfterAddIndexes()) != null) {
            terms.add(new Term(input.fieldName(), input.value()));
        }
        if (!terms.isEmpty()) {
            committer.getIndexWriter().deleteDocuments(terms.toArray(new Term[0]));
        }
        bufferedDeletes.clearFlushedGenerations();
    }

    @Override
    public void beforeRefresh() throws IOException {
        // No-op: deletes are applied explicitly via applyDeletesForGeneration/applyParentDeletes
    }

    @Override
    public void afterRefresh(boolean didRefresh) throws IOException {
        // No-op: pendingWriterDeletes may still have Case 4 entries for next cycle
        bufferedDeletes.clear();
    }

    @Override
    public RefreshResult refresh(RefreshInput refreshInput) throws IOException {
        return null;
    }

    @Override
    public DataFormat getDataFormat() {
        return this.dataFormat;
    }

    @Override
    public void close() throws IOException {
        bufferedDeletes.clear();
        for (Deleter deleter : generationToDeleterMap.values()) {
            deleter.close();
        }
        generationToDeleterMap.clear();
    }
}
