// Model of the windowed-Kafka offset commit. Toward #418. Two invariants:
//
//   commitsOnlyOwned       -- a worker commits offsets only for partitions it
//                             currently owns (the adversarial review's
//                             Finding 1: a plain windowed pipeline kept
//                             committing a revoked partition's position).
//   committedBelowRetained -- the committed offset is at or below the lowest
//                             offset a retained bucket still needs, so a
//                             restart replays everything the open windows hold
//                             (the low-watermark commit).
//
// FIX = false -- commit the processed offset, and keep a revoked partition's
//                uncommitted work.
// FIX = true  -- commit the low watermark, and forget a revoked partition's
//                work on handoff.
module commit_offsets {
  pure val Partitions = Set(0, 1)
  pure val Workers = Set(0, 1)
  pure val MaxOffset = 4
  pure val FIX = false

  var owner: int -> int           // partition -> owner
  var processed: int -> int       // partition -> highest offset processed
  var retainedLow: int -> int     // partition -> lowest offset a retained bucket needs
  var hasWork: (int, int) -> bool // (worker, partition) -> has uncommitted work
  var committedOffset: int -> int // partition -> last committed offset (-1 none)
  var violatedOwn: bool
  var violatedRetained: bool

  pure def pairs: Set[(int, int)] = tuples(Workers, Partitions)

  // What a worker commits for p: the low watermark under the fix, else the
  // processed offset.
  def commitTarget(p: int): int =
    if (FIX) retainedLow.get(p) else processed.get(p)

  action init = all {
    owner' = Partitions.mapBy(p => p),
    processed' = Partitions.mapBy(p => 0),
    retainedLow' = Partitions.mapBy(p => 0),
    hasWork' = pairs.mapBy(wp => false),
    committedOffset' = Partitions.mapBy(p => -1),
    violatedOwn' = false,
    violatedRetained' = false,
  }

  // The owner processes p's next offset; it now has uncommitted work.
  action process(p: int): bool = all {
    processed.get(p) < MaxOffset,
    processed' = processed.set(p, processed.get(p) + 1),
    hasWork' = hasWork.set((owner.get(p), p), true),
    owner' = owner, retainedLow' = retainedLow, committedOffset' = committedOffset,
    violatedOwn' = violatedOwn, violatedRetained' = violatedRetained,
  }

  // A bucket closes; the low watermark slides up, never past processed.
  action closeBucket(p: int): bool = all {
    retainedLow.get(p) < processed.get(p),
    retainedLow' = retainedLow.set(p, retainedLow.get(p) + 1),
    owner' = owner, processed' = processed, hasWork' = hasWork,
    committedOffset' = committedOffset, violatedOwn' = violatedOwn, violatedRetained' = violatedRetained,
  }

  // Worker w commits its position for p.
  action commitStep(w: int, p: int): bool = all {
    hasWork.get((w, p)),
    violatedOwn' = (w != owner.get(p)),
    violatedRetained' = (commitTarget(p) > retainedLow.get(p)),
    committedOffset' = committedOffset.set(p, commitTarget(p)),
    hasWork' = hasWork.set((w, p), false),
    owner' = owner, processed' = processed, retainedLow' = retainedLow,
  }

  // Reassign p. The fix forgets the old owner's uncommitted work; the plain
  // design keeps it, so the old owner can still commit a partition it lost.
  action reassign(p: int, w: int): bool = all {
    owner.get(p) != w,
    owner' = owner.set(p, w),
    hasWork' = if (FIX) hasWork.set((owner.get(p), p), false) else hasWork,
    processed' = processed, retainedLow' = retainedLow, committedOffset' = committedOffset,
    violatedOwn' = violatedOwn, violatedRetained' = violatedRetained,
  }

  action step = {
    nondet p = oneOf(Partitions)
    nondet w = oneOf(Workers)
    any { process(p), closeBucket(p), commitStep(w, p), reassign(p, w) }
  }

  val commitsOnlyOwned = not(violatedOwn)
  val committedBelowRetained = not(violatedRetained)
}
