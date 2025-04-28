import redis
import uuid
import json

def main():
    r = redis.Redis(host='192.168.1.9', port=6379, decode_responses=True)

    request_id = str(uuid.uuid4())
    request = {
        'id': request_id,
        'method': 'square',
        'params': 7
    }

    r.lpush('rpc:queue', json.dumps(request))
    print(f"[Client] Sent request with id {request_id}")

    response_queue = f"rpc:response:{request_id}"

    while True:
        response = r.brpop(response_queue, timeout=5)
        if response:
            _, message = response
            result = json.loads(message)
            print(f"[Client] Got response: {result['result']}")
            break
        else:
            print("[Client] Waiting for response...")

if __name__ == "__main__":
    main()
