import os
from unittest.mock import patch

import pandas as pd
import pytest
from bson import ObjectId

from dsl_grade_db.data_ingestor.mongo_db_report_grade import MongoDBReportGrade
from dsl_grade_db.mongo_db_student_grade import MongoDBStudentGrade


def side_effect_func(value):
    if isinstance(value, int):
        value = str(value)
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
        db.collection.insert_many([
            {  # this student has no project grades
                "student_id": "123",
                "db_id": ObjectId("6595497c6adac1c7b70c33f6"),
                "name": "John",
                "surname": "Doe",
                "written_grades": [],  # no written grades
                "project_grades": [],  # no project grades
            },
            {  # this student has no project grades
                "student_id": "122",
                "db_id": ObjectId("6595497c6adac1c7b70c33f5"),
                "name": "John",
                "surname": "Doe",
                "written_grades": [],  # no written grades
                "project_grades": [
                    {'project_id': "1/3/2023", 'leaderboard_grade': 3,
                     'final_grade': 3, 'team_info': {}}
                ],
            },
            {  # this student has no project grades
                "student_id": "121",
                "db_id": ObjectId("6595497c6adac1c7b70c33f4"),
                "name": "John",
                "surname": "Doe",
                "written_grades": [],  # no written grades
                "project_grades": [
                    {'project_id': "1/3/2021", 'report_grade': 10,
                     'leaderboard_grade': 3, 'final_grade': 13,
                     'report_info': {}, 'team_info': {}}
                ],
            }
        ])
        mock_student_id.get_db_id_from.side_effect = side_effect_func
        mock_student_id.get_project_id.return_value = "1/3/2023"
        db.db_id = mock_student_id
        yield db
        db.collection.drop()


@pytest.fixture
def report_df(tmp_path):
    # multiple submissions for multiple students
    data_input = [
        {"Matricola": "123",
         "Final score": 8,
         "Note": "MFCC/ZCR/RMS + PCA + SVM/RF/KNN"},
        {"Matricola": "122",
         "Final score": 4,
         "Note": "MFCC/ZCR/RMS + PCA + SVM/RF/KNN"},
        {"Matricola": "121",
         "Final score": 3,
         "Note": "MFCC/ZCR/RMS + PCA + SVM/RF/KNN"},
    ]
    df = pd.DataFrame(data_input)
    df.to_csv(os.path.join(tmp_path, "report.csv"), index=False)


@pytest.fixture
def mongo_db_report(report_df, tmp_path, mongo_db_student_grade):
    db = MongoDBReportGrade(database_name="DSL_grade_test",
                            report_csv_file_path=os.path.join(tmp_path, "report.csv"))
    db.student_coll = mongo_db_student_grade
    return db


def test_student_no_project(mongo_db_report):
    # current student:
    #     {  # this student has no project grades
    #         "student_id": "123",
    #         "db_id": ObjectId("6595497c6adac1c7b70c33f6"),
    #         "name": "John",
    #         "surname": "Doe",
    #         "written_grades": [],  # no written grades
    #         "project_grades": [],  # no project grades
    #     }
    mongo_db_report.consume_documents()
    student = mongo_db_report.student_coll.get_student("123")
    assert len(student['project_grades']) == 1
    assert student['project_grades'][0]['report_grade'] == 8
    assert student['project_grades'][0]['final_grade'] == 8


def test_student_only_lead(mongo_db_report):
    # current student:
    # {  # this student has only lead grade for the project
    #     "student_id": "122",
    #     "db_id": ObjectId("6595497c6adac1c7b70c33f5"),
    #     "name": "John",
    #     "surname": "Doe",
    #     "written_grades": [],  # no written grades
    #     "project_grades": [
    #         {'project_id': "1/3/2023", 'leaderboard_grade': 3,
    #          'final_grade': 3, 'team_info': {}}
    #     ],
    # }
    # not needed for this test
    mongo_db_report.consume_documents()
    student = mongo_db_report.student_coll.get_student("122")
    assert len(student['project_grades']) == 1
    assert student['project_grades'][0]['report_grade'] == 4
    assert student['project_grades'][0]['final_grade'] == 7


def test_student_one_complete_project(mongo_db_report):
    # current student:
    # {  # this student has no project grades
    #     "student_id": "121",
    #     "db_id": ObjectId("6595497c6adac1c7b70c33f4"),
    #     "name": "John",
    #     "surname": "Doe",
    #     "written_grades": [],  # no written grades
    #     "project_grades": [
    #         {'project_id': "1/3/2021", 'report_grade': 10,
    #          'leaderboard_grade': 3, 'final_grade': 13,
    #          'report_info': {}, 'team_info': {}}
    #     ],
    # }
    mongo_db_report.consume_documents()
    student = mongo_db_report.student_coll.get_student("121")
    assert len(student['project_grades']) == 2
    assert student['project_grades'][-1]['report_grade'] == 3
    assert student['project_grades'][-1]['final_grade'] == 3
