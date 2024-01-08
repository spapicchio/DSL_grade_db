from __future__ import annotations

from datetime import datetime

from bson import ObjectId
from pymongo import MongoClient

from .dsl_student_id_database import MongoDBStudentId


class MongoDBStudentGrade:
    def __init__(self, database_name="DSL_grade_dbs"):
        self.client = MongoClient()
        self.db = self.client[database_name]
        self.collection = self.db["student_grade"]
        self.db_id = MongoDBStudentId(database_name=database_name)

    def _get_db_id_from(self, student_id: str) -> ObjectId:
        """get the db_id from the student_id.
        Raise an error if the student_id is not in the database"""
        db_id = self.db_id.get_db_id_from(student_id)
        if db_id is None:
            raise ValueError("Student ID not found in the database.")
        return db_id

    def insert_student(self, document: dict):
        """ Document comes from mongo_db_enrolled_student.py in data ingestor"""
        # Insert the student only if it does not exist yet
        document = {
            "student_id": str(document['MATRICOLA']),
            "db_id": self._get_db_id_from(str(document['MATRICOLA'])),
            "name": document['NOME'],
            "surname": document['COGNOME - (*) Inserito dal docente'],
            "written_grades": [],
            "project_grades": []
        }
        if not self.collection.find_one({"db_id": document["db_id"]}):
            self.collection.insert_one(document)

    def remove_student(self, student_id: str):
        """Delete a student from the database"""
        db_id = self._get_db_id_from(student_id)
        # remove the student from the student database
        self.collection.delete_one({"db_id": db_id})
        # remove the student_id from the student ID database

        self.db_id.remove_student_id(student_id)

    def update_student_id(self, student_id: str, new_student_id: str):
        """Update the student ID of a student"""
        self.db_id.update_student_id(student_id, new_student_id)
        db_id = self._get_db_id_from(new_student_id)
        self.collection.update_one({"db_id": db_id},
                                   {"$set": {"student_id": new_student_id}})

    def update_students_have_rejected(self, student_ids: list[str]):
        """Update the students that have rejected the project"""
        for student_id in student_ids:
            db_id = self._get_db_id_from(student_id)
            student = self.collection.find_one({"db_id": db_id})
            # get and sort project_grades
            project_grades = student['project_grades']
            project_grades.sort(
                key=lambda x: datetime.strptime(x['project_id'], '%d/%m/%Y'))
            # get and sort written_grades
            written_grades = student['written_grades']
            written_grades.sort(key=lambda x: datetime.strptime(x['date'], '%d/%m/%Y'))
            # update student
            self.update_student_written_grade(student_id, written_grades[:-1])
            self.update_student_project_grade(student_id, project_grades[:-1])

    def get_student(self, student_id: str) -> dict:
        """Get a student from the database"""
        db_id = self._get_db_id_from(student_id)
        student = self.collection.find_one({"db_id": db_id})
        if not student:
            raise ValueError("Student ID not found in the database.")
        return student

    def get_final_grade_given(self, student_id):
        """Get the final grade of a student"""
        db_id = self._get_db_id_from(student_id)
        student = self.collection.find_one({"db_id": db_id})
        written = self._get_max_written_grade(student['written_grades'])
        project = self._get_max_project_grade(student['project_grades'])
        return written + project if written and project else None

    def _get_max_written_grade(self, written_grades) -> float | None:
        """Get the max written grade of a student"""
        # TODO: this is not the max written grade, but the last one for this version
        # order based on date, ascending
        written_grades.sort(key=lambda x: datetime.strptime(x['date'], '%d/%m/%Y'))
        return written_grades[-1]["grade"] if written_grades else None

    def _get_max_project_grade(self, project_grades) -> float | None:
        """Get the max report grade of a student"""
        # select only the projects where there is the report and the leaderboard grade
        # project_grades = [project for project in project_grades
        #                   if 'report_grade' in project and 'leaderboard_grade' in project]
        # # sort the projects based on project_id (date)
        # project_grades.sort(key=lambda x: x['project_id'])
        # return project_grades[-1]["final_grade"] if project_grades else None
        grades = [project['final_grade'] for project in project_grades
                  if 'report_grade' in project and 'leaderboard_grade' in project]
        return max(grades) if grades else None

    def get_student_id_to_correct(self, threshold: float):
        """Get the student ID of the students that have to correct their report"""
        project_id = self.db_id.get_project_id()
        # There is at least one project with the current projectID
        # and the report grade not yet assigned
        # TODO: if No submission to leaderboard, we should not consider it
        # TODO: consume_documents_in_teams() in teams_grade.py
        students = self.collection.find({
            "project_grades": {
                "$elemMatch": {
                    "project_id": project_id,
                    "report_grade": {"$exists": False},
                }}
        })
        # now we have all the students that participate in the projectID
        # select only those with a max_written_grade >= Threshold
        students_ids = [self.db_id.get_student_id_from(student["db_id"])
                        for student in students
                        if self._get_max_written_grade(
                student['written_grades']) >= threshold]
        return students_ids

    def get_students_project_session(self) -> list[dict]:
        """Get the final students"""
        # Return the students that have completed the current session project
        project_id = self.db_id.get_project_id()
        students = self.collection.find({
            "project_grades": {
                "$elemMatch": {
                    "project_id": project_id,
                    "report_grade": {"$exists": True},
                    "leaderboard_grade": {"$exists": True}
                }}
        })
        return list(students)

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
