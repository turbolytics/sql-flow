-- One row: which install this is, and which of its two telemetry events have
-- been sent. bin/telemetry.sh reads and writes it. Nothing else does.
--
-- install_id names this database. It is made here, from nothing about the
-- deployer: not a hostname, not a Render id, not an address. It survives
-- restarts and redeploys, so an install is counted once however often it
-- starts.
--
-- Each event has two columns. sent_at is set once our collector has answered
-- 2xx, and the event is never sent again. claimed_at is a lease: a process
-- takes it before it sends, so that two pipeline instances starting together
-- do not both report one install, and gives it back if the send fails. A
-- lease older than two minutes is up for grabs, so a process that died between
-- claiming and sending does not lose the event for good.
CREATE TABLE install (
  singleton                boolean     PRIMARY KEY DEFAULT true CHECK (singleton),
  install_id               uuid        NOT NULL DEFAULT gen_random_uuid(),
  created_at               timestamptz NOT NULL DEFAULT now(),
  deployed_claimed_at      timestamptz,
  deployed_sent_at         timestamptz,
  first_request_claimed_at timestamptz,
  first_request_sent_at    timestamptz
);

INSERT INTO install DEFAULT VALUES;
