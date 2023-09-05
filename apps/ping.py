import pymongo

# Replace the following with your MongoDB connection string
mongo_uri = "mongodb://root:example@localhost:27017/iot_simulator"

try:
    # Attempt to connect to the MongoDB database
    client = pymongo.MongoClient(mongo_uri)
    db = client.database_name  # Replace with your database name

    # Check if the connection was successful
    if db.command("ping"):
        print("Connected to MongoDB successfully.")
    else:
        print("Failed to connect to MongoDB.")

    # Close the MongoDB connection
    client.close()

except pymongo.errors.ConnectionFailure as e:
    print(f"Error connecting to MongoDB: {e}")