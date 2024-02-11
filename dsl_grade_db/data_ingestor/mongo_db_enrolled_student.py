from pymongo import MongoClient

from .utils import read_file
from ..dsl_student_id_database import MongoDBStudentId
from ..mongo_db_student_grade import MongoDBStudentGrade


class MongoDBEnrolledStudent:
    def __init__(self, database_name):
        self.client = MongoClient()
        self.db = self.client[database_name]
        self.mongo_db_student_id = MongoDBStudentId(database_name=database_name)
        self.mongo_db_student_grade = MongoDBStudentGrade(database_name=database_name)

    def consume_file(self, file_path):
        df = read_file(file_path)
        df['MATRICOLA'] = df['MATRICOLA'].astype(str)
        for _, row in df.iterrows():
            self.mongo_db_student_id.add_student_id(row.to_dict())
            self.mongo_db_student_grade.insert_student(row.to_dict())
