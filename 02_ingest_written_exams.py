from dsl_grade_db.data_ingestor import MongoDBWrittenGrade


def main():
    db = MongoDBWrittenGrade(
        database_name="DSL_grade",
        written_csv_file_path="data/written_grade/written_grade_01_sessions_2024.csv"
    )

    db.consume_written_grades()


if __name__ == "__main__":
    main()
