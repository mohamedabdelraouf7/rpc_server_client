#!/usr/bin/env python3
import os
import json
import redis

# -- Define the remote‐callable functions here --
def add(a, b):
    return a + b

def multiply(a, b):
    return a * b

FUNCTIONS = {
    'add': add,
    'multiply': multiply,
}

def main():
    # Connect to Redis (set REDIS_HOST/REDIS_PORT as needed)
    redis_host = os.environ.get('REDIS_HOST', 'localhost')
    redis_port = int(os.environ.get('REDIS_PORT', 6379))
    r = redis.Redis(host=redis_host, port=redis_port)

    print(f"[RPC Server] Listening on queue 'rpc_queue' @ {redis_host}:{redis_port}")
    while True:
        # BLPOP blocks until a request arrives
        _, msg = r.blpop('rpc_queue')
        req = json.loads(msg)

        func_name   = req.get('function')
        args        = req.get('args', [])
        kwargs      = req.get('kwargs', {})
        reply_queue = req.get('reply_to')
        corr_id     = req.get('id')

        # build basic response
        resp = {'id': corr_id}

        try:
            result = FUNCTIONS[func_name](*args, **kwargs)
            resp.update(status='ok', result=result)
        except Exception as e:
            resp.update(status='error', error=str(e))

        # send back over the client’s reply queue
        r.rpush(reply_queue, json.dumps(resp))

if __name__ == '__main__':
    main()
