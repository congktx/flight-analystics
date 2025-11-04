import mongoConnection as mongo

collection = mongo.db["myCollection"]

docs = [{"name": "Alice", "age": 25}]
collection.insert_many(docs)

