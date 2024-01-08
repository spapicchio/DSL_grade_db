from dsl_grade_db.data_ingestor import MongoDBReportGrade

REPORT_FILE = "your file.csv"


def main():
    report_db = MongoDBReportGrade(database_name="DSL_grade_dbs",
                                   report_csv_file_path=REPORT_FILE)

    report_db.consume_reports()


if __name__ == "__main__":
    main()
