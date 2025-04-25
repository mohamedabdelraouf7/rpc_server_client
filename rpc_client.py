#!/usr/bin/env python3
import os, json, uuid, time, redis

REDIS_HOST   = os.getenv("REDIS_HOST", "127.0.0.1")
REQ_QUEUE    = "rpc_queue"
REPLY_PREFIX = "rpc_response_"

r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

def call(method: str, *params, timeout: int = 5):
    """
    Send an RPC request and wait (blocking) for the reply.
    Raises TimeoutError if no response arrives within <timeout> seconds.
    """
    corr_id = str(uuid.uuid4())
    reply_q = f"{REPLY_PREFIX}{corr_id}"

    request = {
        "method": method,
        "params": list(params),
        "correlation_id": corr_id,
        "reply_queue": reply_q,
    }
    # 1) enqueue the request
    r.rpush(REQ_QUEUE, json.dumps(request))

    # 2) wait for single response
    start = time.time()
    while True:
        # Use BLPOP with a 1-second step so we can check timeout manually
        res = r.blpop(reply_q, timeout=1)
        if res:
            _, msg = res
            response = json.loads(msg)
            if response.get("correlation_id") == corr_id:
                if "error" in response:
                    raise RuntimeError(response["error"])
                return response["result"]
        if time.time() - start > timeout:
            raise TimeoutError("RPC call timed out")

def main():
    print("[CLIENT] Type two integers; Ctrl-C to quit.")
    while True:
        try:
            a = int(input("a = "))
            b = int(input("b = "))
            result = call("add", a, b)
            print(f"add({a}, {b}) = {result}\n")
        except (ValueError, RuntimeError, TimeoutError) as e:
            print("Error:", e)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[CLIENT] Bye!")
