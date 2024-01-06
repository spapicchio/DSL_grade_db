from dsl_grade_db.data_ingestor import MongoDBEnrolledStudent


def main():
    db = MongoDBEnrolledStudent(
        database_name="DSL_grade",
        enrolled_students_csv_file_path="data/students_id/students_01_sessions_2024.csv",
    )
    db.consume_file()


if __name__ == "__main__":
    main()
