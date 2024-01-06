import pytest

from dsl_grade_db import MongoDBStudentId, MongoDBStudentGrade


@pytest.fixture
def mongo_db_id():
    db = MongoDBStudentId(database_name="DSL_grade_test")
    db.add_student_id({"MATRICOLA": "123"})
    db.add_student_id({"MATRICOLA": "122"})
    db.add_student_id({"MATRICOLA": "121"})
    db.set_project_id("1/3/2021")
    yield db
    db.collection.drop()


@pytest.fixture
def mongo_student_id(mongo_db_id):
    db = MongoDBStudentGrade(database_name="DSL_grade_test")
    db.db_id = mongo_db_id
    yield db
    db.collection.drop()


# 1st use case: two students in the team but only one pass the exams and it changes team members
def test_1st_case(mongo_student_id):
    # first student with two written grades and two project grades
    mongo_student_id.collection.insert_one({
        "student_id": "123",
        "db_id": mongo_student_id.db_id.get_db_id_from("123"),
        "name": "Simone",
        "surname": "Papicchio",
        "written_grades": [
            {'date': '08/09/2021', 'grade': 5, "written_info": {}},
            {'date': '08/09/2020', 'grade': 5, "written_info": {}}],
        "project_grades": [
            {'project_id': "1/3/2021", 'report_grade': 10,
             'leaderboard_grade': 3, 'final_grade': 13,
             'report_info': {}, 'team_info': {}},
            {'project_id': "1/3/2020", 'report_grade': 7,
             'leaderboard_grade': 3, 'final_grade': 10,
             'report_info': {}, 'team_info': {}},
        ]
    })
    # second students pass the exams
    mongo_student_id.collection.insert_one({
        "student_id": "122",
        "db_id": mongo_student_id.db_id.get_db_id_from("123"),
        "name": "Simone",
        "surname": "Papicchio",
        "written_grades": [{'date': '08/09/2021', 'grade': 18, "written_info": {}}],
        "project_grades": [
            {'project_id': "1/3/2021", 'report_grade': 10,
             'leaderboard_grade': 3, 'final_grade': 13,
             'report_info': {}, 'team_info': {}},
        ]
    })
    # 1. get students of the current session
    students = mongo_student_id.get_students_project_session()
    assert len(students) == 2
    assert students[0]["student_id"] == "123"
    assert students[1]["student_id"] == "122"
    # 2. the second students accept and the first rejected the grade
    mongo_student_id.remove_student("122")  # remove the student from the database
    # update the student that have rejected
    mongo_student_id.update_students_have_rejected(["123"])
    with pytest.raises(KeyError):
        mongo_student_id.get_student("122")
    doc = mongo_student_id.get_student("123")
    assert doc
    assert len(doc["written_grades"]) == 1
    assert doc["written_grades"][0]['date'] == '08/09/2020'
    assert len(doc["project_grades"]) == 1
    assert doc["project_grades"][0]['project_id'] == "1/3/2020"


# 2sd use case: one student change its student ID
def test_2sd_student_change_id(mongo_student_id):
    mongo_student_id.collection.insert_one({
        "student_id": "123",
        "db_id": mongo_student_id.db_id.get_db_id_from("123"),
        "name": "Simone",
        "surname": "Papicchio",
        "written_grades": [{'date': '08/09/2021', 'grade': 5, "written_info": {}}],
        "project_grades": [
            {'project_id': "1/3/2021", 'report_grade': 10,
             'leaderboard_grade': 3, 'final_grade': 13,
             'report_info': {}, 'team_info': {}}]
    })

    # the student_id changes from "123" to "125"
    mongo_student_id.update_student_id("123", "125")
    student = mongo_student_id.get_student("125")
    assert student
    assert student["student_id"] == "125"
    assert student["db_id"] == mongo_student_id.db_id.get_db_id_from("125")
    assert student["name"] == "Simone"
    assert student["surname"] == "Papicchio"
    assert len(student["written_grades"]) == 1
    assert len(student["project_grades"]) == 1
    with pytest.raises(KeyError):
        mongo_student_id.get_student("123")
