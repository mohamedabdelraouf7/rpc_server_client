import redis
import json

def main():
    r = redis.Redis(host='192.168.1.9', port=6379, decode_responses=True)

    print("[Server] Waiting for requests...")

    while True:
        request = r.brpop('rpc:queue', timeout=0)
        if request:
            _, message = request
            request_data = json.loads(message)

            request_id = request_data['id']
            method = request_data['method']
            params = request_data['params']

            print(f"[Server] Received request: {method}({params})")

            if method == 'square':
                result = params * params
            else:
                result = None

            response = {
                'result': result
            }
            response_queue = f"rpc:response:{request_id}"
            r.lpush(response_queue, json.dumps(response))

if __name__ == "__main__":
    main()
