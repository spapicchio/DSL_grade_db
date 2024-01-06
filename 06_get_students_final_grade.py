import json

from dsl_grade_db import MongoDBStudentGrade

STUDENTS_ID_CORRECT_SAVE_FILE = "students_id_to_correct.json"


def main():
    student_db = MongoDBStudentGrade(database_name="DSL_grade_dbs")
    # all the students tha satisfy the following conditions:
    # 1. have participated in the last project (the project is associated with the last leaderboard ingested)
    # 2. The project contains both leaderboard and report grades
    data = student_db.get_students_project_session()

    with open(STUDENTS_ID_CORRECT_SAVE_FILE, 'w') as json_file:
        json.dump(data, json_file, indent=2)


if __name__ == "__main__":
    main()
