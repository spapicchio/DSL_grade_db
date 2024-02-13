from __future__ import annotations

import logging

from bson import ObjectId
from pymongo import MongoClient

from .dsl_student_id_database import MongoDBStudentId


class MongoDBStudentGrade:
    def __init__(self, database_name="DSL_grade"):
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

    def get_student(self, student_id: str) -> dict:
        """Get a student from the database"""
        db_id = self._get_db_id_from(student_id)
        student = self.collection.find_one({"db_id": db_id})
        if not student:
            raise ValueError("Student ID not found in the database.")
        return student

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

    def get_final_grade_given(self, student_id):
        """Get the final grade of a student"""
        student = self.get_student(student_id)
        last_written = self._get_last_OK_written_grade_given(student)
        last_project = self._get_last_OK_project_grade_given(student)
        if last_project and last_written:
            return last_written['grade'] + \
                last_project['leaderboard_grade'] + last_project['report_grade'] + last_project['report_extra_grade']

    @staticmethod
    def _get_last_OK_written_grade_given(student: dict) -> list | None:
        """"""
        # if there is at least one written grade
        written_grades = student['written_grades']
        for exam in written_grades[::-1]:
            if exam['flag_written_exam'] == 'OK':
                return exam
        return None

    @staticmethod
    def _get_last_OK_project_grade_given(student: list) -> list | None:
        # select only the projects where there is the report and the leaderboard grade
        project_grades = student['project_grades']
        # for loop from the last element
        for project in project_grades[::-1]:
            if 'report_grade' in project and 'leaderboard_grade' in project \
                    and project['flag_project_exam'] == 'OK':
                return project
        return None

    def _get_written_grade_current_appeal(self, student: dict) -> dict | None:
        """Get the written grade of the current appeal"""

        if not student['written_grades']:
            return None

        if student['written_grades'][-1]['date'] == self.db_id.get_written_id():
            return student['written_grades'][-1]
        return None

    def _get_project_grade_current_appeal(self, student: dict) -> dict | None:
        """Get the project grade of the current appeal"""
        if not student['project_grades']:
            return None
        if student['project_grades'][-1]['project_id'] == self.db_id.get_project_id():
            return student['project_grades'][-1]
        return None

    def get_teams_to_correct_report(self):
        """Get the student ID of the students that have to correct their report from the current exam session"""
        project_id = self.db_id.get_project_id()
        written_id = self.db_id.get_written_id()
        # all the students that have participated in the current exam session
        students = self.collection.find({
            "$or": [
                {  # case both written and project are in the current exam session
                    "written_grades": {"$elemMatch": {"date": written_id, "flag_written_exam": "OK"}},
                    "project_grades": {"$elemMatch": {"project_id": project_id, "flag_project_exam": "NO_REPORT"}}
                },
                {  # case only project is in the current exam session
                    "written_grades": {"$elemMatch": {"date": {'$ne': written_id}, "flag_written_exam": "OK"}},
                    "project_grades": {"$elemMatch": {"project_id": project_id, "flag_project_exam": "NO_REPORT"}}
                },
                {  # case only written is in the current exam session
                    "written_grades": {"$elemMatch": {"date": written_id, "flag_written_exam": "OK"}},
                    "project_grades": {
                        "$elemMatch": {"project_id": {'$ne': project_id}, "flag_project_exam": "NO_REPORT"}}
                }
            ]
        })
        return {(student['project_grades'][-1]['team_info']['Student ID # 1'],
                 student['project_grades'][-1]['team_info']['Student ID # 2'])
                for student in students}

    def get_all_students_current_appeal(self):
        """Get the student ID of the students that have to correct their report from the current exam session"""
        project_id = self.db_id.get_project_id()
        written_id = self.db_id.get_written_id()
        # all the students that have participated in the current exam session
        students = self.collection.find({
            "$or": [
                {  # case both written and project are in the current exam session
                    "written_grades.date": written_id,
                    "project_grades.project_id": project_id
                },
                {  # case only project is in the current exam session
                    "written_grades.date": {'$ne': written_id},
                    "project_grades.project_id": project_id
                },
                {  # case only written is in the current exam session
                    "written_grades.date": written_id,
                    "project_grades.project_id": {'$ne': project_id}
                },
            ]
        })

    def _update_student_has_rejected_written(self, student_id):
        """Update the student who has rejected the written exam"""
        student = self.get_student(student_id)
        written_grades = student['written_grades']
        last_exam = self._get_last_OK_written_grade_given(written_grades)
        if last_exam['date'] != self.db_id.get_written_id():
            # if the last written exam is not the current one
            logging.warning(f'This student id {student_id}, does not have the current written session')
            logging.warning(f'\t{student["written_grades"]}')

        index = last_exam['index']
        written_grades[index]['flag_written_exam'] = 'REJECTED'
        self.update_student_project_grade(student_id, written_grades)

    def _update_student_has_rejected_project(self, student_id: str):
        """Update the students that have rejected the project"""
        student = self.get_student(student_id)
        project_grades = student['project_grades']
        last_project = self._get_last_OK_project_grade_given(project_grades)
        if last_project['project_id'] != self.db_id.get_project_id():
            # if the last project is not the current one
            logging.warning(f'This student id {student_id}, does not have the current project session')
            logging.warning(f'\t{student["project_grades"]}')
        # if the last written exam is the current one
        index = last_project['index']
        project_grades[index]['flag_project_exam'] = 'REJECTED'
        self.update_student_project_grade(student_id, project_grades)

    def update_student_has_rejected_project_and_written(self, students_id: str):
        self._update_student_has_rejected_written(students_id)
        self._update_student_has_rejected_project(students_id)

    def _get_students_to_verbalize(self, student: dict) -> list[dict]:
        current_project_appeal = self._get_project_grade_current_appeal(student)
        current_written_appeal = self._get_written_grade_current_appeal(student)
        if not current_written_appeal:
            # if there is no current written exam, take the last available ok exam only if it is OK
            current_written_appeal = self._get_last_OK_written_grade_given(student)
        if not current_project_appeal:
            # if there is no current project exam, take the last available ok exam only if it is OK
            current_project_appeal = self._get_last_OK_project_grade_given(student)

        if not current_project_appeal and not current_written_appeal:
            # case 7: the student has not taken the written exam or the project
            return 'ABSENT'

        if current_project_appeal and not current_written_appeal:
            # case 6: the student has taken only the project in this appeal
            return 'ABSENT'

        if not current_project_appeal and current_written_appeal:
            # case 5: the student has taken only the written exam in this appeal
            # TODO check what happen in this case
            return ''

        # case where there is at least one written exam and one project exam
        if current_written_appeal['flag_written_exam'] == 'absent':
            # the student was absent at the written exam
            return 'ABSENT'
        if current_written_appeal['flag_written_exam'] == 'failed' or current_written_appeal[
            'flag_written_exam'] == 'retired':
            # the written exam was failed or retired
            return 'RESPINTO'

        # at ths point all the written exams and project should be ok
        final_grade = current_written_appeal['grade'] + \
                      current_project_appeal['leaderboard_grade'] + current_project_appeal['report_grade'] + \
                      current_project_appeal['report_extra_grade']

        return final_grade if final_grade >= 18 else 'RESPINTO'

    def analize_students_current_appeal(self, students_id_appeal: list[str],
                                        students_id_rejected_written: set,
                                        students_id_rejected_written_and_project: set
                                        ) -> dict[str, str | float]:
        """Analize the students of the current appeal"""
        verbalized_students = {}
        for student_id in students_id_appeal:
            # Case 1: the student has rejected the written exam or both the written exam and the project
            if student_id in students_id_rejected_written_and_project or student_id in students_id_rejected_written:
                verbalized_students[student_id] = 'RESPINTO'
            student = self.get_student(student_id)
            verbalized_students[student_id] = self._get_students_to_verbalize(student)
        return verbalized_students

    def _update_student_has_been_verbalized(self, student_id: str):
        db_id = self._get_db_id_from(student_id)
        self.collection.update_one({"db_id": db_id},
                                   {"$set": {"has_been_verbalized": True}})

    def update_student_project_grade(self, student_id: str, project_grades: list):
        """Update the project grade of a student"""
        student = self.get_student(student_id)
        self.collection.update_one({"db_id": student['db_id']},
                                   {"$set": {"project_grades": project_grades}})

    def update_student_written_grade(self, student_id: str, written_grades: list):
        """Update the written grade of a student"""
        student = self.get_student(student_id)
        self.collection.update_one({"db_id": student['db_id']},
                                   {"$set": {"written_grades": written_grades}})

    def close(self):
        """Close the database"""
        self.client.close()
