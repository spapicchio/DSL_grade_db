import pandas as pd
from pymongo import MongoClient

from ..dsl_student_id_database import MongoDBStudentId
from ..mongo_db_student_grade import MongoDBStudentGrade


class MongoDBEnrolledStudent:
    def __init__(self, enrolled_students_csv_file_path, database_name):
        self.client = MongoClient()
        self.db = self.client[database_name]
        self.mongo_db_student_id = MongoDBStudentId(database_name=database_name)
        self.mongo_db_student_grade = MongoDBStudentGrade(database_name=database_name)
        self.file_path = enrolled_students_csv_file_path

    def consume_file(self):
        df = pd.read_csv(self.file_path)
        df['MATRICOLA'] = df['MATRICOLA'].astype(str)
        for _, row in df.iterrows():
            self.mongo_db_student_id.add_student_id(row.to_dict())
            self.mongo_db_student_grade.insert_student(row.to_dict())
