from unittest.mock import patch

import pytest
from bson import ObjectId

from dsl_grade_db.mongo_db_student_grade import MongoDBStudentGrade


def side_effect_func(value):
    if value == "123":
        return ObjectId("6595497c6adac1c7b70c33f6")
    elif value == "122":
        return ObjectId("6595497c6adac1c7b70c33f5")
    elif value == "121":
        return ObjectId("6595497c6adac1c7b70c33f4")


@pytest.fixture
def mongo_db_student_grade():
    # Create MongoDBStudentGrade with the mocked MongoDBStudentId
    with patch('dsl_grade_db.mongo_db_student_grade.MongoDBStudentId') as mock_student_id:
        db = MongoDBStudentGrade(database_name="DSL_grade_test")
        mock_student_id.get_db_id_from.side_effect = side_effect_func
        mock_student_id.get_project_id.return_value = "1/3/2022"
        db.db_id = mock_student_id
        yield db
        db.collection.drop()
        db.client.close()


def test_get_student(mongo_db_student_grade):
    student_id = "123"
    document = {"db_id": side_effect_func(student_id),
                "name": "John Doe",
                "written_grades": [{"grade": 90}],
                "project_grades": []}

    mongo_db_student_grade.collection.insert_one(document)
    retrieved_student = mongo_db_student_grade.get_student(student_id)

    assert retrieved_student is not None
    assert retrieved_student["name"] == "John Doe"
    assert retrieved_student["written_grades"][0]["grade"] == 90


def test_insert_student(mongo_db_student_grade):
    student_id = "123"
    document = {"db_id": side_effect_func(student_id),
                "MATRICOLA": student_id,
                "NOME": "John",
                'COGNOME - (*) Inserito dal docente': "Doe",
                }

    mongo_db_student_grade.insert_student(document)
    retrieved_student = mongo_db_student_grade.get_student(student_id)

    assert retrieved_student is not None
    assert retrieved_student["db_id"] == side_effect_func(student_id)
    assert retrieved_student["written_grades"] == []
    assert retrieved_student["project_grades"] == []


def test_insert_student_twice(mongo_db_student_grade):
    """if it is already present, insert only once"""
    student_id = "123"
    document = {"db_id": side_effect_func(student_id),
                "MATRICOLA": student_id,
                "NOME": "John",
                'COGNOME - (*) Inserito dal docente': "Doe",
                }

    mongo_db_student_grade.insert_student(document)  # insert once
    mongo_db_student_grade.insert_student(document)  # insert once
    students = list(mongo_db_student_grade.collection.find({"db_id": side_effect_func(student_id)}))
    assert len(students) == 1


def test_remove_student(mongo_db_student_grade):
    student_id = "123"
    document = {"db_id": side_effect_func(student_id),
                "name": "John Doe",
                "written_grades": [{"grade": 90}],
                "project_grades": []}

    mongo_db_student_grade.collection.insert_one(document)
    retrieved_student = mongo_db_student_grade.get_student(student_id)

    assert retrieved_student is not None
    assert retrieved_student["name"] == "John Doe"
    assert retrieved_student["written_grades"][0]["grade"] == 90


# @pytest.mark.parametrize("written_grades, expected", [()])
# def test_get_last_written_grade_given(written_grades, expected):
#     output = MongoDBStudentGrade._get_last_OK_written_grade_given(written_grades)
#     assert output == expected


@pytest.mark.parametrize("written_grades, project_grades, expected", [
    (
            # simple case, written and project current session OK
            [{'date': '29/01/2024', "grade": 20, "flag_written_exam": "OK"}],
            [{'project_id': 'First', "flag_project_exam": "OK", "leaderboard_grade": 2, "report_grade": 7,
              "report_extra_grade": 1}],
            30
    ),
    (  # simple case, written and project current session OK, but total grade less than 18
            [{'date': '29/01/2024', "grade": 5, "flag_written_exam": "OK"}],
            [{'project_id': 'First', "flag_project_exam": "OK",
              "leaderboard_grade": 2, "report_grade": 7, "report_extra_grade": 1}],
            'RESPINTO'
    ),
    (  # Case only written grade in current session
            [{'date': '29/01/2024', "grade": 5, "flag_written_exam": "OK"}],
            [],
            ''  # TODO: we do not now yet
    ),
    (
            # Case only project grade in current session
            [],
            [{'project_id': 'First', "flag_project_exam": "OK",
              "leaderboard_grade": 2, "report_grade": 7, "report_extra_grade": 1}],
            'ABSENT'
    ),
    (  # failed written exam (less than THRESHOLD)
            [{'date': '29/01/2024', "grade": 5, "flag_written_exam": "failed"}],
            [{'project_id': 'First', "flag_project_exam": "OK",
              "leaderboard_grade": 2, "report_grade": 7, "report_extra_grade": 1}],
            'RESPINTO'
    ),
    (  # Absent written exam
            [{'date': '29/01/2024', "grade": None, "flag_written_exam": "absent"}],
            [{'project_id': 'First', "flag_project_exam": "OK",
              "leaderboard_grade": 2, "report_grade": 7, "report_extra_grade": 1}],
            'ABSENT'
    ),
    (  # retired during the exam
            [{'date': '29/01/2024', "grade": None, "flag_written_exam": "retired"}],
            [{'project_id': 'First', "flag_project_exam": "OK",
              "leaderboard_grade": 2, "report_grade": 7, "report_extra_grade": 1}],
            'RESPINTO'
    ),
    (  # retired during the exam
            [],
            [],
            'ABSENT'
    ),
    (       # the previous project has not been corrected yet
            [{'date': '29/01/2024', "grade": 20, "flag_written_exam": "OK"}],
            [{'project_id': 'different', "flag_project_exam": "NO-REPORT",
              "leaderboard_grade": 2}],
            ''  # TODO: not yet clear
    ),
])
def test_get_students_to_verbalize_at_least_one_current_session(written_grades, project_grades, expected):
    student = {
        "db_id": '111',
        "name": "John Doe",
        "written_grades": written_grades,
        "project_grades": project_grades
    }
    with patch('dsl_grade_db.mongo_db_student_grade.MongoDBStudentId') as mock_student_id:
        mock_student_id.get_project_id.return_value = 'First'
        mock_student_id.get_written_id.return_value = '29/01/2024'
        db = MongoDBStudentGrade(database_name="DSL_grade_test")
        db.db_id = mock_student_id

        assert expected == db._get_students_to_verbalize(student)


@pytest.mark.parametrize("written_grades, project_grades, expected", [
    (
            # written different session but everything is ok
            [{'date': '29/01/2024', "grade": 20, "flag_written_exam": "OK"}],
            [{'project_id': 'Second', "flag_project_exam": "OK", "leaderboard_grade": 2, "report_grade": 7,
              "report_extra_grade": 1}],
            30
    ),
    (
            # written different session but less than 18
            [{'date': '29/01/2024', "grade": 5, "flag_written_exam": "OK"}],
            [{'project_id': 'Second', "flag_project_exam": "OK", "leaderboard_grade": 2, "report_grade": 7,
              "report_extra_grade": 1}],
            'RESPINTO'
    ),
    (
            # written different session but previous session absent
            [{'date': '29/01/2024', "grade": 20, "flag_written_exam": "absent"}],
            [{'project_id': 'Second', "flag_project_exam": "OK", "leaderboard_grade": 2, "report_grade": 7,
              "report_extra_grade": 1}],
            'ABSENT'
    ),
    (
            # project different session
            [{'date': '10/02/2024', "grade": 20, "flag_written_exam": "OK"}],
            [{'project_id': 'First', "flag_project_exam": "OK", "leaderboard_grade": 2, "report_grade": 7,
              "report_extra_grade": 1}],
            30
    ),
    (
            # project different session but previous project rejected
            [{'date': '10/02/2024', "grade": 20, "flag_written_exam": "OK"}],
            [{'project_id': 'First', "flag_project_exam": "rejected", "leaderboard_grade": 2, "report_grade": 7,
              "report_extra_grade": 1}],
            ''  # TODO: not yet clear
    ),
])
def test_get_students_to_verbalize_at_most_one_current_session(written_grades, project_grades, expected):
    student = {
        "db_id": '111',
        "name": "John Doe",
        "written_grades": written_grades,
        "project_grades": project_grades
    }
    with patch('dsl_grade_db.mongo_db_student_grade.MongoDBStudentId') as mock_student_id:
        mock_student_id.get_project_id.return_value = 'Second'
        mock_student_id.get_written_id.return_value = '10/02/2024'
        db = MongoDBStudentGrade(database_name="DSL_grade_test")
        db.db_id = mock_student_id

        assert expected == db._get_students_to_verbalize(student)
