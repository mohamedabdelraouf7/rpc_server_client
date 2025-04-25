#!/usr/bin/env python3
import os, json, redis, uuid

REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REQUEST_CH = "rpc:req"
RESPONSE_CH = "rpc:res:"

def add(a, b):     
    return a + b

def main():
    r = redis.Redis(host=REDIS_HOST, decode_responses=True)
    sub = r.pubsub()
    sub.subscribe(REQUEST_CH)
    print(f"[SERVER] Waiting for calls on {REQUEST_CH} …")
    for msg in sub.listen():
        if msg["type"] != "message":
            continue
        req = json.loads(msg["data"])
        call_id = req["id"]
        method  = req["method"]
        params  = req.get("params", [])
        print(f"[SERVER] {method}{tuple(params)}")
        # simple function dispatcher
        result = globals()[method](*params)
        r.publish(RESPONSE_CH + call_id, json.dumps({"result": result}))

if __name__ == "__main__":
    main()
