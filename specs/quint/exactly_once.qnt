// Model of the exactly-once count under retries and crash-replay. Toward #418.
//
// An event has an id. The SDK retries and a crash replays from the committed
// offset, so the same id can be delivered more than once. The count must still
// attribute each id once: dedup by id_hash (a set) makes it idempotent.
//
// This is the no-double-count half of exactly-once; the no-loss half is
// commit_offsets.qnt's committedBelowRetained (the committed offset never
// passes a row a retained bucket still needs).
//
// Invariant exactlyOnce: the published count equals the number of distinct ids
// delivered -- never more, however many times an id is redelivered.
//
// FIX = true  -- dedup: the window holds a set of ids.
// FIX = false -- no dedup: the window counts every delivery.
module exactly_once {
  pure val Ids = Set(0, 1)
  pure val FIX = false

  var consumed: Set[int]   // ids whose offset is durably committed (not replayed)
  var inflight: Set[int]   // delivered since the last commit (lost on crash, replayed)
  var windowSet: Set[int]  // distinct ids accumulated (the dedup view)
  var windowCount: int     // raw deliveries accumulated (the no-dedup view)
  var violated: bool

  // The published count under the active design.
  def counted: int = if (FIX) windowSet.size() else windowCount

  action init = all {
    consumed' = Set(),
    inflight' = Set(),
    windowSet' = Set(),
    windowCount' = 0,
    violated' = false,
  }

  // Deliver an id the group has not durably committed. A retry or a replay is
  // just another deliver of an id not yet in `consumed`.
  action deliver(id: int): bool = all {
    not(consumed.contains(id)),
    windowSet' = windowSet.union(Set(id)),
    windowCount' = windowCount + 1,
    inflight' = inflight.union(Set(id)),
    // over-count iff the published count exceeds the distinct ids delivered
    violated' = ((if (FIX) windowSet.union(Set(id)).size() else windowCount + 1) != windowSet.union(Set(id)).size()),
    consumed' = consumed,
  }

  // Commit: the inflight deliveries become durable; Kafka will not replay them.
  action commit: bool = all {
    inflight.size() > 0,
    consumed' = consumed.union(inflight),
    inflight' = Set(),
    windowSet' = windowSet, windowCount' = windowCount, violated' = violated,
  }

  // Crash with the disk kept: uncommitted (inflight) deliveries are not
  // durable, so Kafka replays them -- they leave `inflight` and can be
  // delivered again. The window's accumulated state survives.
  action crash: bool = all {
    inflight.size() > 0,
    inflight' = Set(),
    consumed' = consumed, windowSet' = windowSet, windowCount' = windowCount, violated' = violated,
  }

  action step = any {
    nondet id = oneOf(Ids)
    deliver(id),
    commit,
    crash,
  }

  val exactlyOnce = not(violated)
}
