#!/usr/bin/env python3
import os, json, uuid, redis

REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REQUEST_CH = "rpc:req"
RESPONSE_CH = "rpc:res:"

def call(method, *params):
    r = redis.Redis(host=REDIS_HOST, decode_responses=True)
    call_id = str(uuid.uuid4())
    payload = {"id": call_id, "method": method, "params": list(params)}
    r.publish(REQUEST_CH, json.dumps(payload))
    sub = r.pubsub()
    sub.subscribe(RESPONSE_CH + call_id)
    for msg in sub.listen():
        if msg["type"] == "message":
            return json.loads(msg["data"])["result"]

def main():
    while True:
        a = int(input("a = "))
        b = int(input("b = "))
        res = call("add", a, b)
        print(f"add({a}, {b}) = {res}\n")

if __name__ == "__main__":
    main()
