#!/usr/bin/env python3
import os
import uuid
import json
import time
import redis

def call(r, function, *args, **kwargs):
    corr_id = str(uuid.uuid4())
    reply_q = f"response_queue:{corr_id}"

    # build and send request
    req = {
        'id': corr_id,
        'function': function,
        'args': args,
        'kwargs': kwargs,
        'reply_to': reply_q
    }
    r.rpush('rpc_queue', json.dumps(req))

    # wait for a response
    _, msg = r.blpop(reply_q)
    resp = json.loads(msg)

    if resp.get('status') == 'ok':
        return resp['result']
    else:
        raise RuntimeError(resp.get('error'))

def main():
    # Connect to Redis (point at your Redis VM)
    redis_host = os.environ.get('REDIS_HOST', 'localhost')
    redis_port = int(os.environ.get('REDIS_PORT', 6379))
    r = redis.Redis(host=redis_host, port=redis_port)

    print("[RPC Client] Calling remote functions…")
    try:
        sum_result = call(r, 'add', 4, 5)
        print(f"  add(4, 5) = {sum_result}")

        prod_result = call(r, 'multiply', 6, 7)
        print(f"  multiply(6, 7) = {prod_result}")

    except Exception as e:
        print(f"[RPC Client] Error: {e}")

if __name__ == '__main__':
    main()
