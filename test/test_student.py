from unittest.mock import patch

import pytest
from bson import ObjectId

from dsl_grade_db.mongo_db_student_grade import MongoDBStudentGrade

OBJECT_ID = ObjectId("626bccb9697a12204fb22ea3")
DOCUMENT = {"db_id": OBJECT_ID,
            "name": "John Doe",
            "written_grades": [{"date": "08/09/2023", "grade": 90},
                               {"date": "08/09/2022", "grade": 50}],
            "project_grades": [{"project_id": 0,
                                "leaderboard_grade": 5,
                                "final_grade": 5},
                               {"project_id": 1,
                                "leaderboard_grade": 5,
                                "final_grade": 5}
                               ]}


@pytest.fixture
def mongo_db_student_grade():
    # Create MongoDBStudentGrade with the mocked MongoDBStudentId
    with patch('dsl_grade_db.mongo_db_student_grade.MongoDBStudentId') as mock_student_id:
        db = MongoDBStudentGrade(database_name="DSL_grade_test")
        mock_student_id.get_db_id_from.return_value = OBJECT_ID
        mock_student_id.get_project_id.return_value = 1
        db.db_id = mock_student_id
        yield db
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
    mongo_db_student_grade.collection.insert_one(DOCUMENT)
    final_grade = mongo_db_student_grade.get_final_grade_given(student_id)
    # no project completed!
    assert final_grade is None
    mongo_db_student_grade.collection.delete_one({"db_id": DOCUMENT["db_id"]})


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
    mock_id.get_project_id.return_value = 1
    mongo_db_student_grade.db_id = mock_id
    # the first student does not participate in the projectID 1
    # and the written exam is too low
    mongo_db_student_grade.collection.insert_one(
        {"student_id": student_ids[0],
         "db_id": object_ids[0],
         "written_grades": [{"date": "08/09/2023", "grade": 4}],
         "project_grades": [{"project_id": 0,
                             "leaderboard_grade": 5,
                             "final_grade": 5}]
         })
    # the second student has a written exam high,
    # but the report has already been assigned
    mongo_db_student_grade.collection.insert_one(
        {"student_id": student_ids[1],
         "db_id": object_ids[1],
         "written_grades": [{"date": "08/09/2023", "grade": 20}],
         "project_grades": [{"project_id": 1,
                             "leaderboard_grade": 5,
                             "report_grade": 5,
                             "final_grade": 10}]
         })
    # the third student has a written exam high, and it participates in the projectID 1
    mongo_db_student_grade.collection.insert_one(
        {"student_id": student_ids[2],
         "db_id": object_ids[2],
         "written_grades": [{"date": "08/09/2023", "grade": 20}],
         "project_grades": [{"project_id": 1,
                             "leaderboard_grade": 0,
                             "final_grade": 0}]
         })

    students_to_correct = mongo_db_student_grade.get_student_id_to_correct(threshold=10)
    # only the last two students because max written grade >= 10
    assert students_to_correct[0] == student_ids[2]

    students_to_correct = mongo_db_student_grade.get_student_id_to_correct(threshold=21)
    assert not students_to_correct


@patch('dsl_grade_db.dsl_student_id_database.MongoDBStudentId')
def test_get_students(mock_id, mongo_db_student_grade, ):
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
    mock_id.get_project_id.return_value = 1
    mongo_db_student_grade.db_id = mock_id
    # the first student does not participate in the projectID 1
    # and the written exam is too low
    mongo_db_student_grade.collection.insert_one(
        {"student_id": student_ids[0],
         "db_id": object_ids[0],
         "written_grades": [{"date": "08/09/2023", "grade": 4}],
         "project_grades": [{"project_id": 0,
                             "leaderboard_grade": 5,
                             "final_grade": 5}]
         })
    # the second student has a written exam high,
    # but the report has already been assigned
    mongo_db_student_grade.collection.insert_one(
        {"student_id": student_ids[1],
         "db_id": object_ids[1],
         "written_grades": [{"date": "08/09/2023", "grade": 20}],
         "project_grades": [{"project_id": 1,
                             "leaderboard_grade": 5,
                             "report_grade": 5,
                             "final_grade": 10}]
         })
    # the third student has a written exam high, and it participates in the projectID 1
    mongo_db_student_grade.collection.insert_one(
        {"student_id": student_ids[2],
         "db_id": object_ids[2],
         "written_grades": [{"date": "08/09/2023", "grade": 20}],
         "project_grades": [{"project_id": 1,
                             "leaderboard_grade": 0,
                             "final_grade": 0}]
         })

    students = mongo_db_student_grade.get_students_project_session()
    assert len(students) == 1
    # only the second returned because it has both leaderboard and report grade
    # for the current project id = 1
    assert students[0]["student_id"] == student_ids[1]
