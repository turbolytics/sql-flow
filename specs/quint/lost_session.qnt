// Model of what a worker's watermark minimum holds across a failed group
// session, to reproduce issue #436 and to check its fix.
//
// Abstraction: the group owns each partition to one worker (`owner`). Each
// worker's watermark tracker keeps the partitions in its minimum (`held`)
// and marks some of them lost. A worker's watermark is the minimum over
// `held`, so a partition in `held` that no data will ever reach -- because
// another worker owns it -- pins the watermark for good, and the worker
// stops closing windows while it goes on consuming. That is the stall in
// #436: in the stack, count-2 kept partition 0 after a rejoin gave it to
// count, and its watermark stayed at partition 0's last position.
//
// Tracker calls, as core.Watermarks implements them:
//   Assigned(S)  holds S; FIX also drops every lost partition not in S
//   Released(S)  drops S
//   Lost         marks everything held as lost, keeps it held
//
// A session fails all at once, and the first assignment after it is the
// member's whole new assignment (franz-go's cooperative assignor gives a
// member that owns nothing everything it is given). LagTable already relies
// on this.
//
// Current design (FIX = false): a lost partition stays held until it is
// assigned back here. If the rejoin gives it to another member, it never is.
//
// Fixed design (FIX = true): the rejoin's assignment settles every lost
// partition; one not in it leaves `held`, as a revoked one does.
module lost_session {
  pure val Partitions = Set(0, 1, 2)
  pure val Workers = Set(0, 1)
  pure val FIX = false

  var owner: int -> int          // partition -> the worker the group gave it to
  var up: int -> bool            // worker -> its group session is alive
  var held: int -> Set[int]      // worker -> partitions in its watermark minimum
  var lost: int -> Set[int]      // worker -> partitions it marked lost

  def ownedBy(w: int): Set[int] = Partitions.filter(p => owner.get(p) == w)

  action init = all {
    owner' = Map(0 -> 0, 1 -> 0, 2 -> 1),
    up' = Workers.mapBy(w => true),
    held' = Map(0 -> Set(0, 1), 1 -> Set(2)),
    lost' = Workers.mapBy(w => Set()),
  }

  // A rebalance between live members moves p from its owner to `to`: the old
  // owner is told Released, the new one Assigned (incrementally).
  action move(p: int, dest: int): bool = {
    val from = owner.get(p)
    all {
      from != dest, up.get(from), up.get(dest),
      owner' = owner.set(p, dest),
      held' = held.set(from, held.get(from).exclude(Set(p))).set(dest, held.get(dest).union(Set(p))),
      lost' = lost,
      up' = up,
    }
  }

  // w's session fails: the tracker marks everything it holds as lost.
  action loseSession(w: int): bool = all {
    up.get(w),
    up' = up.set(w, false),
    lost' = lost.set(w, held.get(w)),
    held' = held,
    owner' = owner,
  }

  // While w is gone the group gives its partition p to a live member.
  action takeOver(p: int, dest: int): bool = {
    val from = owner.get(p)
    all {
      not(up.get(from)), up.get(dest), from != dest,
      owner' = owner.set(p, dest),
      held' = held.set(dest, held.get(dest).union(Set(p))),
      lost' = lost,
      up' = up,
    }
  }

  // w rejoins and the group gives it S: partitions it still owns, plus any
  // taken from live members (who are told Released). Its tracker is told
  // Assigned(S), the whole of its new assignment.
  action rejoin(w: int, S: Set[int]): bool = {
    val keep = held.get(w).exclude(lost.get(w).exclude(S))
    val mine = if (FIX) keep.union(S) else held.get(w).union(S)
    val others = Workers.exclude(Set(w))
    all {
      not(up.get(w)),
      // Only partitions the group can give: w's own, or a live member's.
      S.forall(p => owner.get(p) == w or up.get(owner.get(p))),
      up' = up.set(w, true),
      owner' = Partitions.mapBy(p => if (S.contains(p)) w else owner.get(p)),
      held' = Workers.mapBy(x =>
        if (x == w) mine
        else held.get(x).exclude(S)),
      lost' = lost.set(w, Set()),
    }
  }

  action step = {
    nondet p = oneOf(Partitions)
    nondet w = oneOf(Workers)
    nondet S = oneOf(Partitions.powerset())
    any {
      move(p, w),
      loseSession(w),
      takeOver(p, w),
      rejoin(w, S),
    }
  }

  // The property: a live worker's watermark minimum holds only partitions
  // it owns. A partition another member owns brings no more data here, so
  // holding it pins the watermark and the worker stops closing windows.
  val liveMinimumOnlyOwned = Workers.forall(w =>
    not(up.get(w)) or held.get(w).forall(p => owner.get(p) == w))
}
