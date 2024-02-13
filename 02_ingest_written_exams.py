from dsl_grade_db.data_ingestor import MongoDBWrittenGrade


def main():
    db = MongoDBWrittenGrade(database_name="DSL_grade")
    db.consume_registered_students(
        written_grade_path='data/02_written_grade/primo_appello/01TWZSM_0-Exam_2024-01-29-valutazioni1.xlsx',
        registered_student_path='data/02_written_grade/primo_appello/VISAP_Elenco_Studenti_11022024192517.xls',
        threshold=8.0
    )
    db.close()


if __name__ == "__main__":
    main()
