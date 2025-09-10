import json
import urllib3

http = urllib3.PoolManager()

def lambda_handler(event, context):
    try:
        for record in event['Records']:
            message = json.loads(record['body'])

            endpoint_url = message.get("endpoint_url")
            http_method = message.get("http_method", "POST").upper()
            headers = message.get("headers", {})
            body = message.get("body", {})

            print(f"Hitting {endpoint_url} with {http_method}")

            encoded_body = json.dumps(body).encode("utf-8") if body else None
            response = http.request(
                http_method,
                endpoint_url,
                body=encoded_body,
                headers=headers
            )

            print(f"Response status: {response.status}")
            print(f"Response data: {response.data.decode('utf-8')}")

        return {"status": "success"}

    except Exception as e:
        print(f"Error: {str(e)}")
        raise
