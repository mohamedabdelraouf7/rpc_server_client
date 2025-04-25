#!/usr/bin/env python3
import redis
import json

def add(a, b):
    return a + b

def main():
    r = redis.Redis(host='192.168.1.19', port=6379, db=0)
    print("[x] Awaiting RPC requests")
    while True:
        _, msg = r.blpop("rpc_queue")          # blocks until a request arrives
        request = json.loads(msg)

        method       = request["method"]
        params       = request["params"]
        corr_id      = request["correlation_id"]
        reply_queue  = request["reply_queue"]

        print(f"[.] {method}{tuple(params)}")

        if method == "add":
            result = add(*params)
        else:
            result = f"Error:unknowns method '{method}'"

        response = {
            "result":         result,
            "correlation_id": corr_id
        }
        r.rpush(reply_queue, json.dumps(response))

if __name__ == "__main__":
    main()
