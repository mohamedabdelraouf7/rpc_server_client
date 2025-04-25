#!/usr/bin/env python3
import os, json, redis, time

REDIS_HOST   = os.getenv("REDIS_HOST", "127.0.0.1")
REQ_QUEUE    = "rpc_queue"          # incoming requests
REPLY_PREFIX = "rpc_response_"      # per-call reply queue

# ---- RPC methods -----------------------------------------------------------
def add(a: int, b: int) -> int:
    """Simple demo RPC that adds two numbers."""
    return a + b

FUNC_MAP = {
    "add": add,
    # "sub": sub, "mul": mul ...  # extend here
}
# ---------------------------------------------------------------------------

def main() -> None:
    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    print(f"[SERVER] Awaiting RPC requests on list '{REQ_QUEUE}' (Redis {REDIS_HOST})")

    while True:
        # BLPOP returns (queue_name, payload) or blocks indefinitely
        _, msg = r.blpop(REQ_QUEUE)
        req = json.loads(msg)

        method = req.get("method")
        params = req.get("params", [])
        corr_id = req.get("correlation_id")
        reply_queue = req.get("reply_queue")

        print(f"[SERVER] {method}{tuple(params)}  (corr_id={corr_id})")

        if method not in FUNC_MAP:
            result = {"error": f"unknown method '{method}'", "correlation_id": corr_id}
        else:
            try:
                result = {"result": FUNC_MAP[method](*params), "correlation_id": corr_id}
            except Exception as e:
                result = {"error": str(e), "correlation_id": corr_id}

        r.rpush(reply_queue, json.dumps(result))

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[SERVER] Shutting down.")
