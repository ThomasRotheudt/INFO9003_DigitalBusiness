from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")

def verify_connection():
    try:
        client.server_info()
        print("Connected to MongoDB")
        return True
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return False
    
def insertDocument(collection, title):
    post = {"title": title, "content": "Contenu du premier post"}
    collection.insert_one(post)

def printDocument(collection, title):
    # Recherche de documents dans une collection
    result = collection.find_one({"title": title})
    print(result)

if __name__ == "__main__":
    if verify_connection():
        print("Connected to MongoDB")
    else:
        print("Failed to connect to MongoDB")
        exit()

    db = client["test"]
    collection = db["Collection"]

    insertDocument(collection, "chat")

    printDocument(collection, "chat")

    client.close()