# LLD: Update & Delete Flow for DataFormatAwareEngine

## Overview

Implement update and delete operations in `DataFormatAwareEngine`. All deletes are buffered and applied at controlled points during the refresh cycle — never immediately.

---

## Design Decision: All Deletes Buffered

### Why buffer ALL deletes (even for open child writers)?

1. **No race conditions.** Applying deletes immediately to a child writer risks the writer being flushed/closed concurrently by the refresh thread. Buffering eliminates AlreadyClosedException.
2. **Controlled application point.** Deletes are applied at a well-defined moment in the refresh cycle — before flush for child writers, after addIndexes for parent.
3. **Simple mental model.** `deleteDocument()` always buffers. The refresh thread is the only one that applies deletes.

### Why NOT immediate parent writer delete?

If `parentWriter.deleteDocuments(term)` is called immediately, the pending delete gets consumed/resolved by `addIndexes()` internal `flush(false,true)` before the doc arrives in parent (if the doc is in a child writer being flushed in the same cycle). Result: delete is a no-op.

### Why NOT per-writer buffer inside LuceneWriter?

Writer closes its IndexWriter during `flush()`. No parent IndexWriter reference at writer level. No clean drain point since `LuceneIndexingExecutionEngine.refresh()` only receives `RefreshInput` (file paths), not writer objects.

---

## Delete Cases

A delete arrives with a `generation` field indicating which writer the document lives in:

| Case | Generation | Doc Location | Action |
|------|-----------|--------------|--------|
| 1. Parent delete | gen = -1 | Parent segments (prior refresh) | Buffer → apply to parent after addIndexes |
| 2. Flushed writer | gen matches checked-out writer (already flushed) | Child segment on disk, about to be added to parent | Buffer → apply to parent after addIndexes |
| 3. Current writer (pre-flush) | gen matches checked-out writer (not yet flushed) | Child writer IndexWriter | Buffer → apply to child writer before flush |
| 4. New writer | gen matches writer created after checkout | New writer still in pool | Stays in buffer → handled next refresh cycle |

**Key invariant:** After `checkoutAll()`, no new indexing can happen (pool is empty). Only deletes arrive concurrently during refresh.

---

## Refresh Cycle — Applying Deletes

```
DataFormatAwareEngine.refresh():
  1. writerPool.checkoutAll()              ← take writers from pool (no more indexing after this)
  2. versionMap.beforeRefresh()            ← rotate version map
  
  3. For each checked-out writer:
     a. flushedGenerations.add(writer.gen)         ← mark flushed FIRST (routes late deletes to parent)
     b. applyDeletesForGeneration(writer.gen)      ← drain pendingWriterDeletes for this gen → child IndexWriter
     c. writer.flush()                             ← deletes baked into flushed segment
     d. writer.close()
  
  4. addIndexes(dirs)                      ← child segments merged into parent
  
  5. applyParentDeletes()                  ← drain pendingParentDeletes → parent IndexWriter
                                              + clear flushedGenerations
  
  6. DirectoryReader.open(parentWriter)    ← NRT reader resolves pending parent deletes
  
  7. catalogSnapshotManager.commitNewSnapshot()
  8. versionMap.afterRefresh()
  9. IOUtils.close(writers)
```

### Why this ordering works:

- **Step 3a:** Deletes for a child writer are applied BEFORE flush. The flushed segment already has the doc marked deleted. When `addIndexes()` copies this segment to parent, the delete is baked in.
- **Step 5:** Deletes targeting docs in parent's existing segments (from prior refreshes) or docs that just arrived via `addIndexes()` (Case 2) are applied as pending deletes on the parent writer.
- **Step 6:** `DirectoryReader.open()` calls `IndexWriter.getReader()` which resolves all pending deletes against all parent segments.

---

## Data Structures

### LuceneDeleteExecutionEngine

```java
// Deletes targeting child writers (applied before flush)
private final ConcurrentHashMap<Long, Queue<DeleteInput>> pendingWriterDeletes = new ConcurrentHashMap<>();

// Deletes targeting parent segments (applied after addIndexes)
private final Queue<DeleteInput> pendingParentDeletes = new ConcurrentLinkedQueue<>();

// Tracks which generations have been flushed in the current refresh cycle
// Used by deleteDocument() to route late-arriving deletes to parent
private final Set<Long> flushedGenerations = ConcurrentHashMap.newKeySet();

private final LuceneCommitter committer;
```

### deleteDocument() — Routes Based on Flush State

```java
@Override
public DeleteResult deleteDocument(DeleteInput input) throws IOException {
    long gen = input.generation();
    if (gen == -1L || flushedGenerations.contains(gen)) {
        // Doc in parent (committed segment or just-flushed writer)
        pendingParentDeletes.add(input);
    } else {
        // Doc in child writer (not yet flushed)
        pendingWriterDeletes.computeIfAbsent(gen, k -> new ConcurrentLinkedQueue<>()).add(input);
    }
    return new DeleteResult.Success(1L, 1L, 1L);
}
```

### applyDeletesForGeneration(long gen, IndexWriter childWriter)

Called by refresh thread for each checked-out writer, BEFORE flush.
**Order: mark flushed → remove from buffer → apply → flush → close.**

```java
public void applyDeletesForGeneration(long generation, IndexWriter childWriter) throws IOException {
    flushedGenerations.add(generation);  // Mark FIRST — late deletes route to parent
    Queue<DeleteInput> deletes = pendingWriterDeletes.remove(generation);
    if (deletes == null) return;
    for (DeleteInput input : deletes) {
        Term uid = new Term(input.fieldName(), input.value());
        childWriter.deleteDocuments(uid);
    }
}
```

### applyParentDeletes()

Called by refresh thread AFTER addIndexes, drains parent deletes:

```java
public void applyParentDeletes() throws IOException {
    DeleteInput input;
    while ((input = pendingParentDeletes.poll()) != null) {
        Term uid = new Term(input.fieldName(), input.value());
        committer.getIndexWriter().deleteDocuments(uid);
    }
    flushedGenerations.clear();  // Reset for next cycle
}
```

---

## Delete Operation (Engine Layer)

```
DataFormatAwareEngine.delete(Engine.Delete):
  1. Acquire readLock + versionMap keyed lock
  2. DeletionStrategyPlanner.planOperationAsPrimary(delete)
     → resolves version from LiveVersionMap
  3. Assign seqNo, advanceMaxSeqNoOfUpdatesOrDeletes
  4. If plan.executeOpOnEngine:
     a. Get VersionValue from versionMap
     b. If DataFormatVersionValue → deleteExecutionEngine.deleteDocument(uid, gen)
     c. If not DataFormatVersionValue (committed segment) → deleteExecutionEngine.deleteDocument(uid, -1)
  5. Record DeleteVersionValue tombstone in LiveVersionMap
  6. Write to translog
  7. Track seqNo in localCheckpointTracker
```

## Update Operation (Engine Layer)

```
DataFormatAwareEngine.indexIntoEngine(Engine.Index, IndexingStrategy plan):
  1. If plan.useUpdateDocument:
     a. Get DataFormatVersionValue from versionMap → has writerGeneration
     b. If found → deleteExecutionEngine.deleteDocument(uid, gen)
     c. If not found (committed segment) → deleteExecutionEngine.deleteDocument(uid, -1)
  2. Write new doc via currentWriter.addDoc()
  3. Record new DataFormatVersionValue in versionMap (new gen, new version)
```

---

## Concurrency Model

- `index()` and `delete()` acquire shared `readLock` — can run concurrently with each other
- `refresh()` also acquires shared `readLock` + `refreshLock` (exclusive among refreshes)
- After `checkoutAll()`, pool is empty → no new `index()` can get a writer (blocks on pool)
- `delete()` CAN run concurrently during refresh → routes via `flushedGenerations` check:
  - If gen already flushed → `pendingParentDeletes` (safe, ConcurrentLinkedQueue)
  - If gen not flushed → `pendingWriterDeletes` (safe, ConcurrentHashMap + ConcurrentLinkedQueue)
- `applyDeletesForGeneration()` and `applyParentDeletes()` run on refresh thread only
- `flushedGenerations` is a ConcurrentHashMap.newKeySet() — thread-safe for concurrent reads during `deleteDocument()` and writes during refresh

---

## Edge Cases

1. **Delete arrives after flushedGenerations.add() but before remove():**
   - `deleteDocument()` sees `flushedGenerations.contains(gen)` → true → routes to `pendingParentDeletes` ✅
   - Doc will be in parent after addIndexes → parent delete resolves it

2. **Delete arrives during applyParentDeletes (step 5):**
   - Added to `pendingParentDeletes` tail. `poll()` loop may not see it.
   - Stays in queue for next refresh cycle. ✅ (one-cycle delay, matches NRT contract)

3. **Delete for generation created after checkout (Case 4):**
   - Gen not in `flushedGenerations` → routes to `pendingWriterDeletes`
   - Not matched by any `applyDeletesForGeneration()` call (writer not checked out)
   - Stays in `pendingWriterDeletes` → handled next refresh when that writer is checked out ✅

4. **Pure delete (no new docs in this refresh cycle):**
   - `checkoutAll()` returns empty list. No writers to flush. No `addIndexes()`.
   - `applyParentDeletes()` still runs → sends deletes to parent.
   - Need to call `DirectoryReader.open()` even when no new segments, to resolve parent deletes.
