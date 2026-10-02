// Model of SQLFlow's windowed-Kafka watermark/assignment sub-protocol, to
// reproduce issue #417 and to check that a per-partition watermark removes it.
//
// Abstraction: each Kafka partition carries an event-time "progress" measured
// in whole buckets (minutes). A worker owns a set of partitions. When a
// record's bucket has ended more than `Lateness` before the watermark it is
// judged against, the engine refuses it as late (core.Watermarks.Classify).
//
// Current design (FIX = false): one watermark per window per worker, the
// minimum over the worker's *active* partitions' progress -- a partition the
// worker is still replaying after a reassignment is excluded (it has placed
// nothing yet and idle-closes out of the minimum), so a fast partition drags
// the watermark up and the replayed partition's own open buckets are refused.
//
// Fixed design (FIX = true): the watermark is judged per partition, against
// that partition's own progress, so a partition is never refused for how far
// another partition has run.
module window_watermark {
  pure val Partitions = Set(0, 1)
  pure val Workers = Set(0, 1)
  pure val Lateness = 2
  pure val MaxBucket = 8
  pure val FIX = false

  pure def min2(a: int, b: int): int = if (a < b) a else b
  pure def max2(a: int, b: int): int = if (a > b) a else b

  var progress: int -> int   // partition -> newest bucket its data has reached
  var owner: int -> int      // partition -> owning worker
  var replaying: int -> bool // partition -> owner is still replaying it
  var replayAt: int -> int   // partition -> bucket the replay is at
  var violated: bool         // an open bucket was refused as late

  // Partitions worker w currently owns.
  def ownedBy(w: int): Set[int] =
    Partitions.filter(p => owner.get(p) == w)

  // The watermark partition p's records are judged against.
  def judgeWatermark(p: int): int =
    if (FIX) {
      progress.get(p)
    } else {
      val active = ownedBy(owner.get(p)).filter(q => not(replaying.get(q)))
      if (active.size() == 0) 0
      else active.fold(MaxBucket + 1, (acc, q) => min2(acc, progress.get(q)))
    }

  action init = all {
    progress' = Partitions.mapBy(p => 0),
    owner' = Partitions.mapBy(p => p),
    replaying' = Partitions.mapBy(p => false),
    replayAt' = Partitions.mapBy(p => 0),
    violated' = false,
  }

  // A partition's data advances one bucket while its owner actively consumes it.
  action advance(p: int): bool = all {
    not(replaying.get(p)),
    progress.get(p) < MaxBucket,
    progress' = progress.set(p, progress.get(p) + 1),
    owner' = owner, replaying' = replaying, replayAt' = replayAt, violated' = violated,
  }

  // The group hands p to another worker, which must replay p's retained
  // buckets (back to progress - Lateness, bounded at 0).
  action reassign(p: int, w: int): bool = all {
    owner.get(p) != w,
    not(replaying.get(p)),
    owner' = owner.set(p, w),
    replaying' = replaying.set(p, true),
    replayAt' = replayAt.set(p, max2(0, progress.get(p) - Lateness)),
    progress' = progress, violated' = violated,
  }

  // The new owner replays p's next retained bucket (bucket_end = b + 1). A
  // bucket p still needs -- within Lateness of p's own progress -- must not
  // be refused as late (bucket_end + Lateness <= the watermark it is judged
  // against).
  action replayStep(p: int): bool = all {
    replaying.get(p),
    // refused as late (bucket_end + Lateness <= watermark) AND still needed
    // (bucket_end within Lateness of p's own progress).
    violated' = (
      ((replayAt.get(p) + 1 + Lateness) <= judgeWatermark(p))
      and ((replayAt.get(p) + 1 + Lateness) > progress.get(p))
    ),
    replayAt' = replayAt.set(p, replayAt.get(p) + 1),
    replaying' = if ((replayAt.get(p) + 1) >= progress.get(p)) replaying.set(p, false) else replaying,
    progress' = progress,
    owner' = owner,
  }

  action step = {
    nondet p = oneOf(Partitions)
    nondet w = oneOf(Workers)
    any {
      advance(p),
      reassign(p, w),
      replayStep(p),
    }
  }

  // The property: no bucket a partition still needs is refused as late.
  val noOpenBucketRefused = not(violated)
}
