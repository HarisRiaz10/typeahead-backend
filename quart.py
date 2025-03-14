from quart import Quart, request, jsonify
from flask_cors import CORS
import boto3
from quart_cors import cors
import asyncio
from glide import (
    GlideClusterClient,
    GlideClusterClientConfiguration,
    NodeAddress,
    TimeoutError,
    RequestError,
    ConnectionError,
    ClosingError,
)

app = Quart(__name__)
cors(app)

# Initialize DynamoDB
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("Repository")

# Glide client setup (async)
addresses = [NodeAddress("<Redis_endpoint>", 6379)]
config = GlideClusterClientConfiguration(addresses=addresses, use_tls=True)
client = None

async def connect_to_glide():
    global client
    try:
        print("Connecting to Valkey Glide...")
        client = await GlideClusterClient.create(config)
        print("✅ Connected to Redis.")
    except Exception as e:
        print(f"❌ Redis connection failed: {e}")

@app.before_serving
async def startup():
    await connect_to_glide()

@app.route('/suggest', methods=['GET'])
async def suggest():
    prefix = request.args.get("prefix", "")
    print(f"Received request with prefix: {prefix}")

    if not prefix:
        return jsonify([])

    # Check Redis cache
    try:
        cached_suggestions = await client.lrange(prefix, 0, 9)  # Async call
        if cached_suggestions:
            print("found cached suggestions",cached_suggestions)
            return jsonify([s.decode("utf-8") for s in cached_suggestions])
    except Exception as e:
        print(f"❌ Redis error: {e}")

    # Query DynamoDB if not found in cache
    try:
        response = table.scan(
            FilterExpression="begins_with(#prefix, :prefix)",
            ExpressionAttributeNames={"#prefix": "prefix"},
            ExpressionAttributeValues={":prefix": prefix},
        )
        suggestions = [item["prefix"] for item in response.get("Items", [])]
    except Exception as e:
        print(f"❌ DynamoDB error: {e}")
        return jsonify([])

    # Cache results in Redis
    for suggestion in suggestions:
        await client.lpush(prefix, [suggestion])

    await client.ltrim(prefix, 0, 9)

    return jsonify(suggestions)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=3001)
