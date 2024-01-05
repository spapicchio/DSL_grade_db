from __future__ import annotations

from bson import ObjectId
from pymongo import MongoClient


class MongoDBStudentId:
    def __init__(self, collection_name="student_id_in_db"):
        self.client = MongoClient()
        self.db = self.client["DSL_grade_dbs"]
        self.collection = self.db[collection_name]

    def add_student_id(self, student_id: str, name: str = "", surname: str = ""):
        """
        Add a new student ID to the database only if it does not exist.

        Args:
            student_id (str): The student ID to be added.
            name (str): The name of the student.
            surname (str): The surname of the student.
        """
        if not self.collection.find_one({"MATRICOLA": student_id}):
            self.collection.insert_one(
                {"MATRICOLA": student_id, "name": name, "surname": surname}
            )

    def remove_student_id(self, student_id: str):
        """
        Removes a student ID from the database.

        Args:
            student_id (str): The student ID to be removed.
        """
        self.collection.delete_one({"MATRICOLA": student_id})

    def update_student_id(self, student_id: str, new_student_id: str):
        """
        Updates an existing student ID in the database.

        Args:
            student_id (str): The existing student ID to be updated.
            new_student_id (str): The new student ID to replace the existing one.
        """
        self.collection.update_one({"MATRICOLA": student_id},
                                   {"$set": {"MATRICOLA": new_student_id}})

    def get_db_id_from(self, student_id: str) -> ObjectId | None:
        """
        Retrieves the ObjectID value for a given student ID.

        Args:
            student_id (str): The student ID for which to retrieve the counter value.

        Returns:
            int: The counter value associated with the specified student ID.
        """
        if not isinstance(student_id, str):
            student_id = str(student_id)
        document = self.collection.find_one({"MATRICOLA": student_id})
        if not document:
            raise KeyError(f"Student ID '{student_id}' not found in the database.")
        return document["_id"]

    def get_student_id_from(self, db_id: ObjectId) -> str | None:
        """
        Retrieves the student ID value for a given ObjectID.

        Args:
            db_id (str): The ObjectID for which to retrieve the student ID value.

        Returns:
            str: The student ID value associated with the specified ObjectID.
        """
        document = self.collection.find_one({"_id": db_id})
        if not document:
            raise KeyError(f"Database ID '{db_id}' not found in the database.")
        return document["MATRICOLA"]

    def set_project_id(self, project_id: str):
        if not isinstance(project_id, str):
            project_id = str(project_id)
        self.collection.update_one({"project_id": "project_id"},
                                   {'$set': {'id': project_id}},
                                   upsert=True)

    def get_project_id(self):
        return self.collection.find_one({"project_id": "project_id"})['id']

    def close(self):
        """Close the database"""
        self.client.close()
