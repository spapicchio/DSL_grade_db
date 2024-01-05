from __future__ import annotations

from datetime import datetime

from bson import ObjectId
from pymongo import MongoClient

from .dsl_student_id_database import MongoDBStudentId


class MongoDBStudentGrade:
    def __init__(self, collection_name="student_grade"):
        """
        Student Document: db_id, name, surname, written_grades, project_grades, has_to_be_correct
        written_grades: array of documents with at least "grade"
        project_grades: array of documents with at least "report_grade",
                        "leaderboard_grade", "final_grade"
        """
        self.client = MongoClient()
        self.db = self.client["DSL_grade_dbs"]
        self.collection = self.db[collection_name]
        self.db_id = MongoDBStudentId()

    def _get_db_id_from(self, student_id: str) -> ObjectId:
        """get the db_id from the student_id.
        Raise an error if the student_id is not in the database"""
        db_id = self.db_id.get_db_id_from(student_id)
        if db_id is None:
            raise ValueError("Student ID not found in the database.")
        return db_id

    def get_student(self, student_id: str) -> dict:
        """Get a student from the database"""
        db_id = self._get_db_id_from(str(student_id))
        student = self.collection.find_one({"db_id": db_id})
        return student

    def get_final_grade_student(self, student_id):
        """Get the final grade of a student"""
        db_id = self._get_db_id_from(student_id)
        student = self.collection.find_one({"db_id": db_id})
        written = self._get_max_written_grade_student(student)
        project = self._get_max_project_grade_student(student)
        return written + project if written and project else None

    def _get_max_written_grade_student(self, student) -> float | None:
        """Get the max written grade of a student"""
        # TODO: this is not the max written grade, but the last one for this version
        written_grades = student["written_grades"]
        # order based on date, ascending
        written_grades.sort(key=lambda x: datetime.strptime(x['date'], '%d/%m/%Y'))
        return written_grades[-1]["grade"] if written_grades else None

    def _get_max_project_grade_student(self, student) -> float | None:
        """Get the max report grade of a student"""
        project_grades = student["project_grades"]
        return max(project['final_grade'] for project in project_grades) \
            if project_grades else None

    def set_report_to_correct(self, threshold: float = 10):
        # get all the documents where written grade is at least threshold
        self.collection.update_many({"written_grades.grade": {"$gte": threshold}},
                                    {"$set": {"has_to_be_correct": True}})

    def get_student_id_to_correct(self):
        """Get the student ID of the students that have to correct their report"""
        students = self.collection.find({
            "$and": [{"has_to_be_correct": True},
                     {"project_grades.report_grade": {"$exists": False}}]
        })
        return [self.db_id.get_student_id_from(student["db_id"]) for student in students]

    def update_student_project_grade(self, student_id: str, project_grades: list):
        """Update the project grade of a student"""
        db_id = self.db_id.get_db_id_from(student_id)
        self.collection.update_one({"db_id": db_id},
                                   {"$set": {"project_grades": project_grades}})

    def update_student_written_grade(self, student_id: str, written_grades: list):
        """Update the written grade of a student"""
        db_id = self.db_id.get_db_id_from(student_id)
        self.collection.update_one({"db_id": db_id},
                                   {"$set": {"written_grades": written_grades}})

    def close(self):
        """Close the database"""
        self.client.close()
