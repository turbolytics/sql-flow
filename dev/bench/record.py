# Record real Jetstream post events to gzipped NDJSON.
#
#   uv run --with websockets python dev/bench/record.py <hours_back> <events> <out.ndjson.gz>
#
# Jetstream replays history from a cursor as fast as the client reads, about
# 5,000 posts a second from here against roughly 30 to 50 live, so two million
# real posts take about seven minutes rather than a day. The capture feeds
# dev/bench/replay and SQLFLOW_LEAK_JETSTREAM. Posts are public user content:
# keep captures out of the repository.
import asyncio, gzip, json, sys, time
import websockets

hours_back, target, out = float(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
BASE = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"

async def main():
    cursor = int((time.time() - hours_back * 3600) * 1_000_000)
    n, start, last_report = 0, time.time(), time.time()
    with gzip.open(out, "wt", compresslevel=3) as f:
        while n < target:
            try:
                async with websockets.connect(f"{BASE}&cursor={cursor}", max_size=2**22) as ws:
                    async for raw in ws:
                        f.write(raw if raw.endswith("\n") else raw + "\n")
                        n += 1
                        cursor = json.loads(raw)["time_us"]
                        if time.time() - last_report > 30:
                            last_report = time.time()
                            print(f"{n} events, {n/(time.time()-start):.0f}/s, stream at {time.strftime('%H:%M', time.gmtime(cursor/1e6))} UTC", flush=True)
                        if n >= target:
                            break
            except Exception as e:
                print(f"reconnect after {n} events: {e!r}", flush=True)
                await asyncio.sleep(2)
    print(f"done: {n} events in {time.time()-start:.0f}s -> {out}", flush=True)

asyncio.run(main())
