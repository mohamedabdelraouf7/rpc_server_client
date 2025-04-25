#!/usr/bin/env python3
import redis
import json
import uuid

def main():
    r = redis.Redis(host='192.168.1.19', port=6379, db=0)

    corr_id     = str(uuid.uuid4())
    reply_queue = f"rpc_response_{corr_id}"

    request = {
        "method":         "add",
        "params":         [7, 5],
        "correlation_id": corr_id,
        "reply_queue":    reply_queue,
    }

    r.rpush("rpc_queue", json.dumps(request))

    _, msg   = r.blpop(reply_queue)            # waits for the server reply
    response = json.loads(msg)

    if response.get("correlation_id") == corr_id:
        print("result:", response["result"])
    else:
        print("Mismatched response ID:", response)

if __name__ == "__main__":
    main()
