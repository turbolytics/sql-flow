// Model of partition_owned's single-writer guarantee for the window-commit
// protocol. Toward #418.
//
// The guarantee: one process writes each (bucket, partition) key. Across a
// rebalance the old owner must stop writing a partition's buckets before the
// new owner starts, so a stale partial count never races the new owner's
// recount. partition_owned achieves this by deleting the revoked partition's
// unpublished rows -- not publishing them -- atomically with the handoff,
// under the window pass lock.
//
// Invariant noNonOwnerPublish: a worker never publishes a bucket for a
// partition it does not currently own.
//
// FIX = true  -- partition_owned: the drop happens with the handoff.
// FIX = false -- a plain windowed pipeline: the old owner keeps its
//                unpublished rows and an in-flight pass can still publish
//                them after the partition has moved.
module single_writer {
  pure val Partitions = Set(0, 1)
  pure val Workers = Set(0, 1)
  pure val MaxBucket = 3
  pure val FIX = false

  // owner of each partition
  var owner: int -> int
  // buckets each (worker, partition) has accumulated but not yet published
  var unpublished: (int, int) -> Set[int]
  // a non-owner published a bucket
  var violated: bool

  pure def pairs: Set[(int, int)] =
    tuples(Workers, Partitions)

  action init = all {
    owner' = Partitions.mapBy(p => p),
    unpublished' = pairs.mapBy(wp => Set()),
    violated' = false,
  }

  // The owner of p accumulates its next bucket (data arrives).
  action accumulate(p: int, b: int): bool = all {
    unpublished.get((owner.get(p), p)).contains(b) == false,
    b < MaxBucket,
    unpublished' = unpublished.set((owner.get(p), p), unpublished.get((owner.get(p), p)).union(Set(b))),
    owner' = owner,
    violated' = violated,
  }

  // Worker w runs a pass and publishes what it holds for p. If w is not p's
  // current owner and it has rows to publish, that is a non-owner write.
  action publishPass(w: int, p: int): bool = all {
    unpublished.get((w, p)).size() > 0,
    violated' = (w != owner.get(p)),
    unpublished' = unpublished.set((w, p), Set()),
    owner' = owner,
  }

  // The group hands p to another worker. partition_owned drops the old
  // owner's unpublished rows atomically with the handoff (under the pass
  // lock); a plain pipeline leaves them, so a later pass can still publish
  // them for a partition the worker no longer owns.
  action reassign(p: int, w: int): bool = all {
    owner.get(p) != w,
    owner' = owner.set(p, w),
    unpublished' =
      if (FIX) unpublished.set((owner.get(p), p), Set())
      else unpublished,
    violated' = violated,
  }

  action step = {
    nondet p = oneOf(Partitions)
    nondet w = oneOf(Workers)
    nondet b = oneOf(0.to(MaxBucket))
    any {
      accumulate(p, b),
      publishPass(w, p),
      reassign(p, w),
    }
  }

  val noNonOwnerPublish = not(violated)
}
