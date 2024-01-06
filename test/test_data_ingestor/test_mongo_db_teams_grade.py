import os
from unittest.mock import patch

import pandas as pd
import pytest
from bson import ObjectId

from dsl_grade_db.data_ingestor.mongo_db_teams_grade import MongoDBTeamsGrade
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
        db.collection.insert_many(
            [
                {  # this student has no project grades
                    "student_id": "123",
                    "db_id": ObjectId("6595497c6adac1c7b70c33f6"),
                    "name": "John",
                    "surname": "Doe",
                    "written_grades": [],  # no written grades
                    "project_grades": [],  # no project grades
                },
                {  # this student has one project grade but without leaderboard
                    "student_id": "122",
                    "db_id": ObjectId("6595497c6adac1c7b70c33f5"),
                    "name": "Pippo",
                    "surname": "Pluto",
                    "written_grades": [],
                    "project_grades": [
                        {'project_id': "1/3/2023",
                         'report_grade': 5,
                         'final_grade': 5,
                         'report_info': {}},
                    ]
                },
                {  # this student has one project completed
                    "student_id": "121",
                    "db_id": ObjectId("6595497c6adac1c7b70c33f4"),
                    "name": "Max",
                    "surname": "Power",
                    "written_grades": [],  # no written grades
                    "project_grades": [
                        {'project_id': "1/3/2021", 'report_grade': 10,
                         'leaderboard_grade': 3, 'final_grade': 13,
                         'report_info': {}, 'team_info': {}}],
                }
            ])
        mock_student_id.get_db_id_from.side_effect = side_effect_func
        db.db_id = mock_student_id
        yield db
        db.collection.drop()


@pytest.fixture
def leaderboard_df(tmp_path):
    # multiple submissions for multiple students
    data_input = [
        {"matricola": 123, "score": 0.993, "points": 4, "rounded_points": 4},
        {"matricola": 123, "score": 0.993, "points": 4, "rounded_points": 2},
        {"matricola": 122, "score": 0.993, "points": 4, "rounded_points": 5},
        {"matricola": 121, "score": 0.993, "points": 4, "rounded_points": 1},
        {"matricola": 121, "score": 0.993, "points": 4, "rounded_points": 6},
    ]
    df = pd.DataFrame(data_input)
    df.to_csv(os.path.join(tmp_path, "leaderboard.csv"), index=False)


@pytest.fixture
def teams_df(tmp_path):
    # only one team 123 with 122
    data_input = [
        {"Timestamp": "1/3/2023 22:17:06",
         "Student ID # 1": 123,
         "Student ID # 2": 122}
    ]
    df = pd.DataFrame(data_input)
    df.to_csv(os.path.join(tmp_path, "teams.csv"), index=False)


def test_consume_documents_in_leaderboard(leaderboard_df, teams_df,
                                          tmp_path, mongo_db_student_grade):
    db = MongoDBTeamsGrade(database_name="DSL_grade_test",
                           leaderboard_csv_file_path=os.path.join(tmp_path,
                                                                  "leaderboard.csv"),
                           teams_csv_file_path=os.path.join(tmp_path, "teams.csv"))
    db.student_coll = mongo_db_student_grade

    db.consume_documents_in_leaderboard()
    # create a new team with one student
    # TODO possible error with student id saved as string rather than int
    single_team = db.teams_coll.find_one({"Student ID # 1": 121})
    assert single_team['max_lead_grade'] == 6
    assert single_team['project_id'] == "1/3/2023"
    assert not single_team["Student ID # 2"]
    # check the team with two students has been updated
    double_team = db.teams_coll.find_one({"Student ID # 1": 123})
    assert double_team['max_lead_grade'] == 5
    assert double_team['project_id'] == "1/3/2023"
    assert double_team["Student ID # 2"] == 122
    # drop teams collection
    db.teams_coll.drop()


def test_consume_documents_in_teams(leaderboard_df, teams_df,
                                    tmp_path, mongo_db_student_grade):
    db = MongoDBTeamsGrade(database_name="DSL_grade_test",
                           leaderboard_csv_file_path=os.path.join(tmp_path,
                                                                  "leaderboard.csv"),
                           teams_csv_file_path=os.path.join(tmp_path, "teams.csv"))
    db.student_coll = mongo_db_student_grade
    db.consume_documents_in_teams()
    # the first student has only one project with the leaderboard
    student_1 = mongo_db_student_grade.get_student("123")
    assert student_1["project_grades"][0]["leaderboard_grade"] == 5 \
           and student_1["project_grades"][0]['final_grade'] == 5
    assert student_1["project_grades"][0]['project_id'] == '1/3/2023'
    # the second student has one project with report only
    # assert if it is updated
    student_2 = mongo_db_student_grade.get_student("122")
    assert len(student_2["project_grades"]) == 1
    assert student_2["project_grades"][0]['leaderboard_grade'] == 5 \
           and student_2["project_grades"][0]['final_grade'] == 10
    # the third student has already a complete project, append the new one
    student_3 = mongo_db_student_grade.get_student("121")
    assert len(student_3["project_grades"]) == 2
    assert student_3["project_grades"][-1]['leaderboard_grade'] == 6 \
           and student_3["project_grades"][-1]['final_grade'] == 6
