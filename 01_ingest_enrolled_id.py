import pandas as pd

from dsl_grade_db.data_ingestor import MongoDBEnrolledStudent


def main():
    db = MongoDBEnrolledStudent(database_name="DSL_grade")
    db.consume_file(file_path="data/01_students_id/students_01_sessions_2024.csv")


if __name__ == "__main__":
    main()
