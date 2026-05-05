/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.apache.lucene.index.Term;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.queue.Lockable;

import java.io.Closeable;
import java.io.IOException;

/**
 * Handles document deletion for a specific data format. Each deleter is paired with a
 * {@link Writer} and shares its generation. Implements {@link Lockable} for thread-safe
 * pooling via {@link org.opensearch.common.queue.LockablePool}.
 *
 * <p>For Parquet-only format, the deleter holds a per-generation Lucene writer for
 * indexing identity documents. For Lucene-only format, the deleter is a no-op wrapper
 * since Lucene natively tracks live docs.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface Deleter extends Closeable, Lockable {

    /**
     * Returns the generation number of this deleter, matching its paired writer.
     *
     * @return the generation number
     */
    long generation();

    /**
     * Registers a document for future delete tracking by indexing a minimal identity document.
     * For Lucene-only format this is a no-op (Lucene already has the full document).
     * For Parquet-only format this writes {@code _id} to the per-gen Lucene writer.
     *
     * @param uid the term identifying the document
     * @throws IOException if an I/O error occurs
     */
    void registerForDelete(Term uid) throws IOException;

    /**
     * Deletes a document from the underlying format-specific storage.
     *
     * @param uid the term identifying the document
     * @return the result of the delete operation
     * @throws IOException if an I/O error occurs
     */
    DeleteResult deleteDoc(Term uid) throws IOException;
}
