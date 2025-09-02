from pymongo import MongoClient
import os


class MongoDB:
    _instance = None

    @staticmethod
    def get_instance():
        if MongoDB._instance is None:
            MongoDB._instance = MongoClient(os.environ["mongodb_address"])

        return MongoDB._instance
