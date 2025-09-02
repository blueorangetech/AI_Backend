from database.mongodb import MongoDB
import bcrypt

mongodb = MongoDB.get_instance()
db = mongodb["Customers"]
collection = db["info"]


def hash_password(password):
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password.encode("utf-8"), salt)
    return hashed


def verify_password(hashed_password, password):
    return bcrypt.checkpw(password.encode("utf-8"), hashed_password)


def store_customer(name, password):
    hashed = hash_password(password)
    customer_data = {"name": name, "password": hashed}
    collection.insert_one(customer_data)
    print(f"New Customer{name} Created !")


def authenticate(name, password):
    customer = collection.find_one({"name": name})
    if customer is not None:
        stored_password = customer["password"]

        return verify_password(stored_password, password)

    else:
        print(f"Customer {name} is not existed, Create {name}...")
        store_customer(name, "1111")
        return True
