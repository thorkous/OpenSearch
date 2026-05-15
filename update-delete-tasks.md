# Implementation Tasks: Update & Delete Flow for DataFormatAwareEngine

**Repo**: `/Volumes/workplace/thorkous-changes/OpenSearch`
**LLD**: `/Volumes/workplace/thorkous-changes/OpenSearch/update-delete-lld.md`
**Build**: `./gradlew spotlessApply -Dsandbox.enabled=true && ./gradlew :server:build :sandbox:plugins:build -Dsandbox.enabled=true -x forbiddenApisInternalClusterTest -x forbiddenApisMain -x forbiddenApisTest`

---

## Task 1: DeleteExecutionEngine extends RefreshListener

**File**: `server/src/main/java/org/opensearch/index/engine/dataformat/DeleteExecutionEngine.java`

**What**: Add `ReferenceManager.RefreshListener` as a super-interface.

**Change**:
```java
// Before:
public interface DeleteExecutionEngine<T extends DataFormat> extends Closeable {

// After:
import org.apache.lucene.search.ReferenceManager;

public interface DeleteExecutionEngine<T extends DataFormat> extends Closeable, ReferenceManager.RefreshListener {
```

**Why**: Mirrors InternalEngine where `CompositeIndexWriter` implements `RefreshListener` and is registered on `internalReaderManager`. This lets `DataFormatAwareEngine` add `deleteExecutionEngine` to its `refreshListeners` list so `beforeRefresh()` (drain buffered deletes) and `afterRefresh()` (cleanup) are called automatically during refresh.

**Verification**: Build compiles. All implementations (`LuceneDeleteExecutionEngine`, `MockDeleteExecutionEngine`) will fail until they implement `beforeRefresh()`/`afterRefresh()` — that's expected (fixed in T3/T4).

---

## Task 2: Implement buffered deletes + double-buffer in LuceneDeleteExecutionEngine

**File**: `sandbox/plugins/analytics-backend-lucene/src/main/java/org/opensearch/be/lucene/index/LuceneDeleteExecutionEngine.java`

**What**:
- Replace single `generationToDeleterMap` with a double-buffered pattern (current/old)
- Add `ConcurrentLinkedQueue<DeleteInput>` for buffered deletes
- Modify `deleteDocument()` to buffer when generation not in current map
- Implement `beforeRefresh()`: rotate map + drain buffer into parent writer
- Implement `afterRefresh()`: close old deleters, clear old map

**Changes**:

1. Replace field:
```java
// Before:
private final Map<Long, Deleter> generationToDeleterMap;

// After:
private volatile Map<Long, Deleter> generationToDeleterMap;
private volatile Map<Long, Deleter> oldGenerationToDeleterMap = Collections.emptyMap();
private final Queue<DeleteInput> bufferedDeletes = new ConcurrentLinkedQueue<>();
```

2. Modify `deleteDocument()`:
```java
@Override
public DeleteResult deleteDocument(DeleteInput deleteInput) throws IOException {
    Deleter deleter = generationToDeleterMap.get(deleteInput.generation());
    if (deleter != null) {
        return deleter.deleteDoc(deleteInput);
    }
    // Generation not in current map — buffer for next refresh
    bufferedDeletes.add(deleteInput);
    return new DeleteResult.Success(1L, 1L, 1L);
}
```

3. Implement `beforeRefresh()`:
```java
@Override
public void beforeRefresh() throws IOException {
    // 1. Rotate: current → old, fresh → current
    oldGenerationToDeleterMap = generationToDeleterMap;
    generationToDeleterMap = new ConcurrentHashMap<>();

    // 2. Drain buffered deletes into parent writer
    DeleteInput input;
    while ((input = bufferedDeletes.poll()) != null) {
        Term uid = new Term(input.fieldName(), input.value());
        committer.getIndexWriter().deleteDocuments(uid);
    }
}
```

4. Implement `afterRefresh()`:
```java
@Override
public void afterRefresh(boolean didRefresh) throws IOException {
    for (Deleter deleter : oldGenerationToDeleterMap.values()) {
        deleter.close();
    }
    oldGenerationToDeleterMap = Collections.emptyMap();
}
```

5. **Remove** the existing `else` branch in `deleteDocument()` that directly calls `committer.getIndexWriter().deleteDocuments(uid)`.

**Why double-buffer instead of isOpen()**:
- After `beforeRefresh()` rotates the map, any delete for a flushed generation won't find it in the new (empty) map → goes to buffer automatically
- No `isOpen()` method needed on Deleter interface
- New writers created by the pool supplier after refresh register in the fresh current map via `createDeleter()`
- Mirrors `LiveVersionMap` and `CompositeIndexWriter.LiveIndexWriterDeletesMap` patterns

**Verification**: Build compiles. Existing tests pass.

---

## Task 3: Update MockDeleteExecutionEngine

**File**: `test/framework/src/main/java/org/opensearch/index/engine/dataformat/stub/MockDeleteExecutionEngine.java`

**What**: Add no-op implementations of `beforeRefresh()` and `afterRefresh()`.

**Changes**:
```java
@Override
public void beforeRefresh() throws IOException {
    // No-op in mock
}

@Override
public void afterRefresh(boolean didRefresh) throws IOException {
    // No-op in mock
}
```

**Verification**: Build compiles. All existing tests pass.

---

## Task 4: Refactor refresh() listener wiring in DataFormatAwareEngine

**File**: `server/src/main/java/org/opensearch/index/engine/DataFormatAwareEngine.java`

**What**:
1. Add `versionMap` and `deleteExecutionEngine` to the `refreshListeners` list in the constructor
2. Move `notifyRefreshListenersBefore()` to BEFORE `writerPool.checkoutAll()` (currently it's after writer flush)
3. Remove explicit `versionMap.beforeRefresh()` call (now handled via listener)
4. Remove explicit `versionMap.afterRefresh(refreshed)` call from finally block (now handled via listener)
5. Keep `pruneTombstones()` call — it's not part of the listener contract

**Constructor change** (after building `refreshListeners` list from engineConfig):
```java
// Add versionMap first (must rotate before deleteExecutionEngine drains)
refreshListeners.add(versionMap);
refreshListeners.add((ReferenceManager.RefreshListener) deleteExecutionEngine);
```

**refresh() method changes**:

Before (current):
```java
versionMap.beforeRefresh();
try {
    List<DefaultLockableHolder<Writer<?>>> writers = writerPool.checkoutAll();
    // ... flush writers, build segments ...
    notifyRefreshListenersBefore();
    // ... refresh, commit snapshot ...
    notifyRefreshListenersAfter(refreshed);
} finally {
    store.decRef();
    versionMap.afterRefresh(refreshed);
}
```

After:
```java
try {
    notifyRefreshListenersBefore();  // ← moved here: triggers versionMap.beforeRefresh() + deleteExecutionEngine.beforeRefresh()
    List<DefaultLockableHolder<Writer<?>>> writers = writerPool.checkoutAll();
    // ... flush writers, build segments ...
    // ... refresh, commit snapshot ...
    notifyRefreshListenersAfter(refreshed);  // ← triggers versionMap.afterRefresh() + deleteExecutionEngine.afterRefresh()
} finally {
    store.decRef();
    // versionMap.afterRefresh() removed — handled by listener
}
```

**Ripple effects checked**:
- Existing listeners (`RefreshMetricUpdater`, `ReplicationCheckpointUpdater`, etc.) have no-op `beforeRefresh()` — moving the call earlier has no impact
- `versionMap.afterRefresh()` was in a finally block for safety — now it's in `notifyRefreshListenersAfter()` which is also in the try block. If refresh fails, `afterRefresh()` won't be called. Check if this is acceptable or if you need to keep it in finally. InternalEngine relies on Lucene's ReferenceManager which calls afterRefresh even on failure — consider wrapping `notifyRefreshListenersAfter()` in finally too.

**Verification**: Build compiles. Existing `DataFormatAwareEngineTests` pass.

---

## Task 5: Implement prepareDelete() + advanceMaxSeqNoOfUpdatesOrDeletes()

**File**: `server/src/main/java/org/opensearch/index/engine/DataFormatAwareEngine.java`

**What**: Replace the `throw UnsupportedOperationException` stubs with real implementations.

**prepareDelete()**:
```java
@Override
public Engine.Delete prepareDelete(
    String id, long seqNo, long primaryTerm, long version,
    VersionType versionType, Engine.Operation.Origin origin,
    long ifSeqNo, long ifPrimaryTerm
) {
    Term uid = new Term(IdFieldMapper.NAME, Uid.encodeId(id));
    return new Engine.Delete(id, uid, seqNo, primaryTerm, version, versionType,
        origin, System.nanoTime(), ifSeqNo, ifPrimaryTerm);
}
```

**advanceMaxSeqNoOfUpdatesOrDeletes()**:
```java
@Override
public void advanceMaxSeqNoOfUpdatesOrDeletes(long maxSeqNoOfUpdatesOnPrimary) {
    maxSeqNoOfUpdatesOrDeletes.updateAndGet(curr -> Math.max(curr, maxSeqNoOfUpdatesOnPrimary));
}
```

**Verification**: Build compiles.

---

## Task 6: Wire DeletionStrategyPlanner + implement delete()

**File**: `server/src/main/java/org/opensearch/index/engine/DataFormatAwareEngine.java`

**What**: Add `DeletionStrategyPlanner` field, initialize in constructor, implement full `delete(Engine.Delete)` method.

**Add field**:
```java
private final DeletionStrategyPlanner deletionStrategyPlanner;
```

**Initialize in constructor** (after `indexingStrategyPlanner` initialization):
```java
this.deletionStrategyPlanner = new DeletionStrategyPlanner(
    engineConfig.getIndexSettings(),
    engineConfig.getShardId(),
    this.versionMap,
    localCheckpointTracker::getProcessedCheckpoint,
    this::hasBeenProcessedBefore,
    op -> OpVsEngineDocStatus.OP_NEWER,
    this::resolveDocVersion
);
```
Note: Check `DeletionStrategyPlanner` constructor signature — it may differ. Read the existing class first.

**Implement delete()**:
```java
@Override
public Engine.DeleteResult delete(Engine.Delete delete) throws IOException {
    assert Objects.equals(delete.uid().field(), IdFieldMapper.NAME);
    try (ReleasableLock ignored = readLock.acquire()) {
        ensureOpen();
        lastWriteNanos = delete.startTime();

        final DeletionStrategy plan = deletionStrategyPlanner.planOperationAsPrimary(delete);
        final Engine.DeleteResult deleteResult;

        if (plan.earlyResultOnPreFlightError.isPresent()) {
            deleteResult = (Engine.DeleteResult) plan.earlyResultOnPreFlightError.get();
        } else {
            // Assign seqNo for primary
            long seqNo = generateSeqNoForOperationOnPrimary(delete);

            if (plan.executeOpOnEngine) {
                deleteResult = deleteFromEngine(delete, plan, seqNo);
            } else {
                deleteResult = new Engine.DeleteResult(
                    plan.version, delete.primaryTerm(), seqNo, plan.currentlyDeleted
                );
            }
        }

        if (deleteResult.getSeqNo() != UNASSIGNED_SEQ_NO) {
            // Record tombstone in versionMap
            versionMap.putDeleteUnderLock(
                delete.uid().bytes(),
                new DeleteVersionValue(plan.version, deleteResult.getSeqNo(), delete.primaryTerm(),
                    engineConfig.getThreadPool().relativeTimeInMillis())
            );

            // Write to translog
            if (!delete.origin().isFromTranslog()) {
                Translog.Location location = translogManager.add(new Translog.Delete(delete, deleteResult));
                deleteResult.setTranslogLocation(location);
            }

            // Track seqNo
            localCheckpointTracker.markSeqNoAsProcessed(deleteResult.getSeqNo());
            if (deleteResult.getTranslogLocation() == null) {
                localCheckpointTracker.markSeqNoAsPersisted(deleteResult.getSeqNo());
            }

            // Advance maxSeqNoOfUpdatesOrDeletes
            maxSeqNoOfUpdatesOrDeletes.updateAndGet(curr -> Math.max(curr, deleteResult.getSeqNo()));
        }

        deleteResult.setTook(System.nanoTime() - delete.startTime());
        deleteResult.freeze();
        return deleteResult;
    } catch (RuntimeException | IOException e) {
        maybeFailEngine("delete id[" + delete.id() + "]", e);
        throw e;
    }
}
```

**Implement deleteFromEngine()**:
```java
private Engine.DeleteResult deleteFromEngine(Engine.Delete delete, DeletionStrategy plan, long seqNo) throws IOException {
    // Check if doc is in versionMap (current writer, not yet flushed)
    VersionValue versionValue = versionMap.getUnderLock(delete.uid().bytes());

    if (versionValue instanceof DataFormatVersionValue) {
        DataFormatVersionValue dfvv = (DataFormatVersionValue) versionValue;
        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, delete.uid().bytes(), dfvv.writerGeneration);
        deleteExecutionEngine.deleteDocument(deleteInput);
    } else if (!(versionValue instanceof DeleteVersionValue)) {
        // Doc exists in committed segment (resolved via searcher) — use generation -1 to signal "not in current writer"
        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, delete.uid().bytes(), -1L);
        deleteExecutionEngine.deleteDocument(deleteInput);
    }
    // If DeleteVersionValue — doc already deleted, no physical delete needed

    return new Engine.DeleteResult(plan.version, delete.primaryTerm(), seqNo, plan.currentlyDeleted);
}
```

**Important**: Read `DeletionStrategyPlanner.java`, `DeletionStrategy.java`, `DeleteVersionValue.java`, and `DataFormatVersionValue.java` before implementing. The planner's constructor signature and the `plan` fields may differ from what's shown here.

**Verification**: Build compiles. Write a test that indexes a doc, deletes it, and verifies the delete result.

---

## Task 7: Modify index() for update path

**File**: `server/src/main/java/org/opensearch/index/engine/DataFormatAwareEngine.java`

**What**: In `indexIntoEngine()`, before writing the new doc, detect if this is an update and delete the old version. After writing, record the new `DataFormatVersionValue` in versionMap.

**In `indexIntoEngine()`, before `currentWriter.addDoc()`**:
```java
// If this is an update (doc already exists), delete old version first
if (!plan.currentNotFoundOrDeleted) {
    VersionValue oldVersion = versionMap.getUnderLock(index.uid().bytes());
    if (oldVersion instanceof DataFormatVersionValue) {
        DataFormatVersionValue dfvv = (DataFormatVersionValue) oldVersion;
        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, index.uid().bytes(), dfvv.writerGeneration);
        deleteExecutionEngine.deleteDocument(deleteInput);
    } else if (!(oldVersion instanceof DeleteVersionValue)) {
        // In committed segment
        DeleteInput deleteInput = new DeleteInput(IdFieldMapper.NAME, index.uid().bytes(), -1L);
        deleteExecutionEngine.deleteDocument(deleteInput);
    }
}
```

**After successful write, before translog**:
```java
if (indexResult.getResultType() == Engine.Result.Type.SUCCESS) {
    // Record new version in LiveVersionMap with writer generation
    versionMap.putIndexUnderLock(
        index.uid().bytes(),
        new DataFormatVersionValue(
            null,  // translogLocation — set after translog write
            plan.version,
            index.seqNo(),
            index.primaryTerm(),
            currentWriter.generation()
        )
    );
}
```

**Important**:
- `versionMap.putIndexUnderLock()` and `versionMap.getUnderLock()` require the keyedLock to be held. Check how InternalEngine acquires it (usually via `try (Releasable ignored = versionMap.acquireLock(uid.bytes()))`)
- `plan.currentNotFoundOrDeleted` — verify this field exists on `IndexingStrategy`. It's set by `processNormally(currentNotFoundOrDeleted, ...)`.
- `currentWriter.generation()` — the writer from the pool. Verify `Writer` interface has `generation()` method.
- Advance `maxSeqNoOfUpdatesOrDeletes` for updates too (when `!plan.currentNotFoundOrDeleted`).

**Verification**: Build compiles. Test: index doc, update doc (same ID), verify old version deleted and new version in versionMap. After refresh, search returns only new version.

---

## Build & Test

After all tasks:
```bash
cd /Volumes/workplace/thorkous-changes/OpenSearch
./gradlew spotlessApply -Dsandbox.enabled=true
./gradlew :server:build :sandbox:plugins:build -Dsandbox.enabled=true -x forbiddenApisInternalClusterTest -x forbiddenApisMain -x forbiddenApisTest
```

Run existing tests:
```bash
./gradlew :server:test --tests "org.opensearch.index.engine.DataFormatAwareEngineTests" -Dsandbox.enabled=true
```
