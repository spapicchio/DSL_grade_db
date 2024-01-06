from dsl_grade_db import MongoDBStudentGrade

THRESHOLD = 15
STUDENTS_ID_CORRECT_SAVE_FILE = "students_id_to_correct.txt"


def main():
    student_db = MongoDBStudentGrade(database_name="DSL_grade_dbs")
    # all the students tha satisfy the following conditions:
    # 1. have a max written grade greater than THRESHOLD
    # 2. have participated in the last project (the project is associated with the last leaderboard ingested)
    # 3. The report for this student has not yet been assigned
    students_id_to_correct = student_db.get_student_id_to_correct(threshold=THRESHOLD)
    with open(STUDENTS_ID_CORRECT_SAVE_FILE, "w") as f:
        f.write("\n".join(students_id_to_correct))


if __name__ == "__main__":
    main()
