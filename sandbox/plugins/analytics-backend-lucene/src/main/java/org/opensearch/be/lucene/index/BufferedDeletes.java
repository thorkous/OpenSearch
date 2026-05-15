/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.opensearch.index.engine.dataformat.DeleteInput;

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Thread-safe buffer for pending delete operations, partitioned into writer-level
 * and parent-level deletes.
 *
 * <p>Routing logic: if a generation is -1 or has been marked as flushed via
 * {@link #markFlushed(long)}, to delete targets the parent. Otherwise, it targets
 * the child writer for that generation.
 *
 * @opensearch.experimental
 */
public class BufferedDeletes {

    /** Deletes targeting child writers, keyed by generation. */
    private final ConcurrentHashMap<Long, Queue<DeleteInput>> pendingWriterDeletes = new ConcurrentHashMap<>();

    /** Deletes for docs already in parent (gen=-1). Applied BEFORE addIndexes. */
    private final Queue<DeleteInput> pendingParentDeletesBeforeAddIndexes = new ConcurrentLinkedQueue<>();

    /** Deletes for docs arriving via addIndexes (flushed gen). Applied AFTER addIndexes. */
    private final Queue<DeleteInput> pendingParentDeletesAfterAddIndexes = new ConcurrentLinkedQueue<>();

    /** Generations flushed in the current refresh cycle. Routes late deletes to after-addIndexes queue. */
    private final Set<Long> flushedGenerations = ConcurrentHashMap.newKeySet();

    /**
     * Buffers a delete, routing based on generation and flush state.
     */
    public void add(DeleteInput input) {
        long gen = input.generation();
        if (gen == -1L) {
            pendingParentDeletesBeforeAddIndexes.add(input);
        } else if (flushedGenerations.contains(gen)) {
            pendingParentDeletesAfterAddIndexes.add(input);
        } else {
            pendingWriterDeletes.computeIfAbsent(gen, k -> new ConcurrentLinkedQueue<>()).add(input);
        }
    }

    /**
     * Marks a generation as flushed. Late-arriving deletes for this generation
     * will route to parent instead of the writer queue.
     */
    public void markFlushed(long generation) {
        flushedGenerations.add(generation);
    }

    /**
     * Removes and returns all buffered deletes for the given generation.
     * Returns null if no deletes are buffered for that generation.
     */
    public Queue<DeleteInput> removeWriterDeletes(long generation) {
        return pendingWriterDeletes.remove(generation);
    }

    /**
     * Polls the next pre-addIndexes parent delete. Returns null if empty.
     */
    public DeleteInput pollParentDeleteBeforeAddIndexes() {
        return pendingParentDeletesBeforeAddIndexes.poll();
    }

    /**
     * Polls the next post-addIndexes parent delete. Returns null if empty.
     */
    public DeleteInput pollParentDeleteAfterAddIndexes() {
        return pendingParentDeletesAfterAddIndexes.poll();
    }

    /**
     * Moves a collection of deletes to the after-addIndexes parent queue.
     */
    public void addAllToParent(Queue<DeleteInput> deletes) {
        pendingParentDeletesAfterAddIndexes.addAll(deletes);
    }

    /**
     * Clears flushed generations tracking. Called after parent deletes are applied.
     */
    public void clearFlushedGenerations() {
        flushedGenerations.clear();
    }

    /**
     * Clears all state.
     */
    public void clear() {
        pendingParentDeletesBeforeAddIndexes.clear();
        pendingParentDeletesAfterAddIndexes.clear();
        flushedGenerations.clear();
    }
}
