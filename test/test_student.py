from unittest.mock import patch

import pytest
from bson import ObjectId

from dsl_grade_db.mongo_db_student_grade import MongoDBStudentGrade

OBJECT_ID = ObjectId("626bccb9697a12204fb22ea3")
DOCUMENT = {"db_id": OBJECT_ID,
            "name": "John Doe",
            "written_grades": [{"grade": 90}],
            "project_grades": [{"project_id": 0,
                                "leaderboard_grade": 5,
                                "report_grade": 90,
                                "final_grade": 95}]}


@pytest.fixture
def mongo_db_student_grade():
    # Create MongoDBStudentGrade with the mocked MongoDBStudentId
    with patch('dsl_grade_db.mongo_db_student_grade.MongoDBStudentId') as mock_student_id:
        db = MongoDBStudentGrade(collection_name="student_grade_test")
        mock_student_id.get_db_id_from.return_value = OBJECT_ID
        db.db_id = mock_student_id
        db.collection.insert_one(DOCUMENT)
        yield db
        db.collection.delete_one({"db_id": OBJECT_ID})
        db.collection.drop()
        db.client.close()


@patch('dsl_grade_db.dsl_student_id_database.MongoDBStudentId')
def test_get_student(mock_student_id, mongo_db_student_grade):
    object_id = ObjectId("626bccb9697a12204fb22222")
    mock_student_id.get_db_id_from.return_value = object_id
    mongo_db_student_grade.db_id = mock_student_id

    student_id = "123"
    document = {"db_id": object_id,
                "name": "John Doe",
                "written_grades": [{"grade": 90}],
                "project_grades": []}

    mongo_db_student_grade.collection.insert_one(document)
    retrieved_student = mongo_db_student_grade.get_student(student_id)

    assert retrieved_student is not None
    assert retrieved_student["name"] == "John Doe"
    assert retrieved_student["written_grades"][0]["grade"] == 90
    mongo_db_student_grade.collection.delete_one({"db_id": object_id})


def test_get_final_grade_student(mongo_db_student_grade):
    student_id = "456"
    final_grade = mongo_db_student_grade.get_final_grade_student(student_id)
    assert final_grade == 185  # 90 (written) + 95 (project)


def test_set_report_to_correct(mongo_db_student_grade):
    threshold = 70
    student_ids = ["789", "101"]
    db_ids = [mongo_db_student_grade.db_id.get_db_id_from(student_id) for student_id in
              student_ids]

    for db_id, student_id in zip(db_ids, student_ids):
        mongo_db_student_grade.collection.insert_one(
            {"student_id": student_id, "db_id": db_id, "written_grades": [{"grade": 75}]})

    mongo_db_student_grade.set_report_to_correct(threshold)

    for student_id in student_ids:
        db_id = mongo_db_student_grade.db_id.get_db_id_from(student_id)
        students = mongo_db_student_grade.collection.find({"db_id": db_id})
        for student in students:
            assert student
            assert student["has_to_be_correct"]
            mongo_db_student_grade.collection.delete_one({"student_id": student_id})


@patch('dsl_grade_db.dsl_student_id_database.MongoDBStudentId')
def test_get_student_id_to_correct(mock_id, mongo_db_student_grade):
    """Test the returned student id are the ones to be corrected"""
    def side_effect_func(value):
        if value == object_ids[0]:
            return student_ids[0]
        elif value == object_ids[1]:
            return student_ids[1]
        elif value == object_ids[2]:
            return student_ids[2]

    student_ids = ["111", "222", "333"]
    object_ids = [ObjectId("626bccb9697a12204fb22221"),
                  ObjectId("626bccb9697a12204fb22222"),
                  ObjectId("626bccb9697a12204fb22223")]

    mock_id.get_student_id_from.side_effect = side_effect_func
    mongo_db_student_grade.db_id = mock_id

    for db_id, student_id in zip(object_ids, student_ids):
        mongo_db_student_grade.collection.insert_one(
            {"student_id": student_id,
             "db_id": db_id,
             "has_to_be_correct": True,
             "project_grades": []
             })

    students_to_correct = mongo_db_student_grade.get_student_id_to_correct()

    assert set(students_to_correct) == set(student_ids)

    mongo_db_student_grade.collection.update_one({"db_id": object_ids[0]},
                                                 {"$unset": {"has_to_be_correct": ""}})

    mongo_db_student_grade.collection.update_one({"db_id": object_ids[1]},
                                                 {"$set": {"project_grades": [
                                                     {"report_grade": 90}]}})
    students_to_correct = mongo_db_student_grade.get_student_id_to_correct()
    assert students_to_correct[0] == student_ids[2]

    for student_id in student_ids:
        mongo_db_student_grade.collection.delete_many({"student_id": student_id})
