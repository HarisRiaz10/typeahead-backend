from quart import Quart, request, jsonify
from flask_cors import CORS
import boto3
from quart_cors import cors
import asyncio
from boto3.dynamodb.conditions import Attr
from glide import (
    GlideClusterClient,
    GlideClusterClientConfiguration,
    NodeAddress
)

app = Quart(__name__)
cors(app)

# Initialize DynamoDB
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("Repository")

# Glide (Valkey) client setup
addresses = [NodeAddress("<Redis_Endpoint>", 6379)]
config = GlideClusterClientConfiguration(addresses=addresses, use_tls=False)
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

@app.route('/')
async def root():
    return jsonify({"message": "API is running"}), 200

@app.route('/suggest', methods=['GET'])
async def suggest():
    prefix = request.args.get("prefix", "").lower()
    print(f"Received request with prefix: {prefix}")

    if not prefix:
        return jsonify([])

    suggestions = []

    # 1. Try Redis cache (get up to 5 suggestions)
    try:
        cached = await client.lrange(prefix, 0, 4)
        cached_suggestions = [s.decode("utf-8") for s in cached]
        print(f"✅ Cached suggestions: {cached_suggestions}")
        suggestions.extend(cached_suggestions)
    except Exception as e:
        print(f"❌ Redis error: {e}")
        cached_suggestions = []

    # 2. Query DynamoDB if fewer than 15 total
    if len(suggestions) < 15:
        try:
            response = table.scan(
                FilterExpression=Attr("lowercase_prefix").begins_with(prefix)
            )
            db_items = [item["prefix"] for item in response.get("Items", [])]

            # Remove suggestions already in cache
            db_suggestions = [
                s for s in db_items if s not in suggestions
            ][:15 - len(suggestions)]

            print(f"📦 DynamoDB suggestions: {db_suggestions}")
            suggestions.extend(db_suggestions)

            # Cache new suggestions
            try:
                for suggestion in db_suggestions:
                    await client.rpush(prefix, suggestion)
                await client.ltrim(prefix, 0, 14)
            except Exception as e:
                print(f"❌ Redis caching error: {e}")

        except Exception as e:
            print(f"❌ DynamoDB error: {e}")

    return jsonify(suggestions)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=3001)
