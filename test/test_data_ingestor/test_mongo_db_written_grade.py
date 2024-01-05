import os
from unittest.mock import patch

import pandas as pd
import pytest
from bson import ObjectId

from dsl_grade_db.data_ingestor.mongo_db_written_grade import MongoDBWrittenGrade
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
        db = MongoDBStudentGrade(collection_name="student_grade_test")
        db.collection.insert_many(
            [
                {  # this student has no written grades
                    "student_id": "123",
                    "db_id": ObjectId("6595497c6adac1c7b70c33f6"),
                    "name": "John",
                    "surname": "Doe",
                    "written_grades": [],  # no written grades
                    "project_grades": [],  # no project grades
                },
                {  # this student has one written grade
                    "student_id": "122",
                    "db_id": ObjectId("6595497c6adac1c7b70c33f5"),
                    "name": "Pippo",
                    "surname": "Pluto",
                    "written_grades": [
                        {'date': '08/09/2023', 'grade': 10, "written_info": {}}
                    ],
                    "project_grades": []
                },
                {  # this student has one project completed
                    "student_id": "121",
                    "db_id": ObjectId("6595497c6adac1c7b70c33f4"),
                    "name": "Max",
                    "surname": "Power",
                    "written_grades": [],  # no written grades
                    "project_grades": [
                        {'project_id': 1, 'report_grade': 10, 'leaderboard_grade': 3,
                         'final_grade': 13, 'report_info': {}, 'team_info': {}}],
                }
            ])
        mock_student_id.get_db_id_from.side_effect = side_effect_func
        db.db_id = mock_student_id
        yield db
        db.collection.drop()


@pytest.fixture
def written_grade_df(tmp_path):
    data_input = [
        {"Corso": "corso00 - 01TWZSM",
         "Username": "01twzsm0_it_23_p1070_s123",
         "Nome": "John",
         "Cognome": "Doe",
         "Stato": "Completato",
         "Iniziato": "8 settembre 2023  09:40",
         "Completato": "8 settembre 2023  09:40",
         "Tempo impiegato": "1 ora 30 min.",
         "Email PDF": "14 settembre 2023  22:55",
         "Valutazione/20,00": "12,88",
         "D. 1 /0,00": "-",
         "D. 2 /1,50": "-0,23",
         "D. 3 /1,50": "-0,23",
         "D. 4 /1,50": "1,50",
         "D. 5 /1,50": "1,50",
         "D. 6 /2,50": "2,30",
         "D. 7 /1,50": "1,50",
         "D. 8 /1,50": "1,50",
         "D. 9 /1,50": "0,53",
         "D. 10 /1,50": "1,50",
         "D. 11 /1,50": "1,50",
         "D. 12 /2,00": "0,00",
         "D. 13 /2,00": "1,50"
         },
        {"Corso": "corso00 - 01TWZSM",
         "Username": "01twzsm0_it_23_p1070_s122",
         "Nome": "Pippo",
         "Cognome": "Pluto",
         "Stato": "Completato",
         "Iniziato": "8 settembre 2024  09:40",
         "Completato": "8 settembre 2024  09:40",
         "Tempo impiegato": "1 ora 30 min.",
         "Email PDF": "14 settembre 2023  22:55",
         "Valutazione/20,00": "12.88",
         "D. 1 /0,00": "-",
         "D. 2 /1,50": "-0,23",
         "D. 3 /1,50": "-0,23",
         "D. 4 /1,50": "1,50",
         "D. 5 /1,50": "1,50",
         "D. 6 /2,50": "2,30",
         "D. 7 /1,50": "1,50",
         "D. 8 /1,50": "1,50",
         "D. 9 /1,50": "0,53",
         "D. 10 /1,50": "1,50",
         "D. 11 /1,50": "1,50",
         "D. 12 /2,00": "0,00",
         "D. 13 /2,00": "1,50"
         },
        {"Corso": "corso00 - 01TWZSM",
         "Username": "01twzsm0_it_23_p1070_s121",
         "Nome": "Max",
         "Cognome": "Power",
         "Stato": "Completato",
         "Iniziato": "8 settembre 2023  09:40",
         "Completato": "8 settembre 2023  09:40",
         "Tempo impiegato": "1 ora 30 min.",
         "Email PDF": "14 settembre 2023  22:55",
         "Valutazione/20,00": "5,6",
         "D. 1 /0,00": "-",
         "D. 2 /1,50": "-0,23",
         "D. 3 /1,50": "-0,23",
         "D. 4 /1,50": "1,50",
         "D. 5 /1,50": "1,50",
         "D. 6 /2,50": "2,30",
         "D. 7 /1,50": "1,50",
         "D. 8 /1,50": "1,50",
         "D. 9 /1,50": "0,53",
         "D. 10 /1,50": "1,50",
         "D. 11 /1,50": "1,50",
         "D. 12 /2,00": "0,00",
         "D. 13 /2,00": "1,50"
         }
    ]
    df = pd.DataFrame(data_input)
    # save on a pytest path
    df.to_csv(os.path.join(tmp_path, 'written_grade.csv'), index=False)


def test_parse_written_csv_file(written_grade_df, tmp_path):
    df = pd.read_csv(os.path.join(tmp_path, 'written_grade.csv'))
    df = MongoDBWrittenGrade._parse_written_csv_file(df)
    assert 'student_id' in df.columns
    assert df['student_id'].tolist() == ['123', '122', '121']
    assert 'date' in df.columns
    assert df['date'].tolist() == ['08/09/2023', '08/09/2024', '08/09/2023']
    columns = ['Valutazione/20,00',
               'D. 1 /0,00', 'D. 2 /1,50', 'D. 3 /1,50', 'D. 4 /1,50', 'D. 5 /1,50',
               'D. 6 /2,50', 'D. 7 /1,50', 'D. 8 /1,50', 'D. 9 /1,50', 'D. 10 /1,50',
               'D. 11 /1,50', 'D. 12 /2,00', 'D. 13 /2,00']


def test_consume_written_grade(mongo_db_student_grade, written_grade_df, tmp_path):
    db = MongoDBWrittenGrade(written_collection_name="written_grade_test",
                             student_collection_name="student_grade_test",
                             written_csv_file_path=os.path.join(tmp_path,
                                                                'written_grade.csv'))
    db.student_coll = mongo_db_student_grade

    db.consume_documents()
    # first student should contain only one exam for the date 08/09/2023
    written_grades_student_1 = mongo_db_student_grade.get_student("123")['written_grades']
    assert len(written_grades_student_1) == 1
    assert written_grades_student_1[0]['date'] == '08/09/2023'
    assert written_grades_student_1[0]['grade'] == 12.88
    # second student should contain two exams: one for 08/09/2024 and one for 08/09/2023
    written_grades_student_2 = mongo_db_student_grade.get_student("122")['written_grades']
    assert len(written_grades_student_2) == 2
    for doc in written_grades_student_2:
        if doc['date'] == '08/09/2024':
            assert doc['grade'] == 12.88
        elif doc['date'] == '08/09/2023':
            continue
        else:
            assert False

    # third student should contain only one exam for the date 08/09/2023
    written_grades_student_3 = mongo_db_student_grade.get_student("121")['written_grades']
    assert len(written_grades_student_3) == 1
    assert written_grades_student_3[0]['date'] == '08/09/2023'
    assert written_grades_student_3[0]['grade'] == 5.6
