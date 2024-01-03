from bson import ObjectId
from pymongo import MongoClient


class MongoDatabaseIdDatabase:
    def __init__(self):
        self.client = MongoClient()
        self.db = self.client["DSL_grade_dbs"]
        self.collection = self.db["student_id_in_db"]

    def add_student_id(self, student_id: str):
        """
        Add a new student ID to the database only if it does not exist.

        Args:
            student_id (str): The student ID to be added.
        """
        if not self.collection.find_one({"student_id": student_id}):
            self.collection.insert_one({"student_id": student_id})

    def remove_student_id(self, student_id: str):
        """
        Removes a student ID from the database.

        Args:
            student_id (str): The student ID to be removed.
        """
        self.collection.delete_one({"student_id": student_id})

    def update_student_id(self, student_id: str, new_student_id: str):
        """
        Updates an existing student ID in the database.

        Args:
            student_id (str): The existing student ID to be updated.
            new_student_id (str): The new student ID to replace the existing one.
        """
        self.collection.update_one({"student_id": student_id},
                                   {"$set": {"student_id": new_student_id}})

    def get_db_id_from(self, student_id: str) -> ObjectId | None:
        """
        Retrieves the ObjectID value for a given student ID.

        Args:
            student_id (str): The student ID for which to retrieve the counter value.

        Returns:
            int: The counter value associated with the specified student ID.
        """
        document = self.collection.find_one({"student_id": student_id})
        return document["_id"] if document is not None else None

    def close(self):
        """Close the database"""
        self.client.close()
