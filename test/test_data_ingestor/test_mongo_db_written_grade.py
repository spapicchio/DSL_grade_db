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
        db = MongoDBStudentGrade(database_name="DSL_grade_test")
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
                        {'date': '08/09/2023', 'index': 0, 'grade': 10, "written_info": {}}
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
                        {'project_id': 1, 'index': 0, 'report_grade': 10, 'leaderboard_grade': 3,
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
         "Valutazione/20,00": "12,88",  # Corrected
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
         "Iniziato": "8 settembre 2023  09:40",
         "Completato": "8 settembre 2023  09:40",
         "Tempo impiegato": "1 ora 30 min.",
         "Email PDF": "14 settembre 2023  22:55",
         "Valutazione/20,00": "",  # EMPTY -> retired
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
    ]
    df = pd.DataFrame(data_input)
    # save on a pytest path
    df.to_csv(os.path.join(tmp_path, 'written_grade.csv'), index=False)


@pytest.fixture
def registered_df(tmp_path):
    """The registered students for the appeal"""
    data_input = [
        {
            "MATRICOLA": "123",
            "COGNOME": "Doe",
            "NOME": "John",
        },
        {
            "MATRICOLA": "122",
            "COGNOME": "Pluto",
            "NOME": "Pippo",
        },
        {
            "MATRICOLA": "121",
            "COGNOME": "Power",
            "NOME": "Max",
        },
    ]
    df = pd.DataFrame(data_input)
    df.to_csv(os.path.join(tmp_path, 'registered_student.csv'), index=False)


def test_parse_written_csv_file(written_grade_df, tmp_path):
    df = MongoDBWrittenGrade._read_written_grade(os.path.join(tmp_path, 'written_grade.csv'))
    assert df.index.tolist() == ['123', '122']
    assert 'date' in df.columns
    assert df['date'].unique() == ['08/09/2023']


@pytest.mark.parametrize("written_grades, written_doc_to_add, result", [
    (
            [],  # written_grades
            {'date': '08/09/2023', 'Valutazione/20,00': 10},  # written_doc_to_add
            {'date': '08/09/2023', 'index': 0, 'grade': 10, "flag_written_exam": 'OK',
             "written_info": {'date': '08/09/2023', 'Valutazione/20,00': 10}}  # result
    ),
    (
            [],  # written_grades
            {'date': '08/09/2023', 'Valutazione/20,00': ""},  # written_doc_to_add
            {'date': '08/09/2023', 'index': 0, 'grade': None, "flag_written_exam": 'retired',
             "written_info": {'date': '08/09/2023', 'Valutazione/20,00': ""}}  # result
    ),
    (
            [{'date': '08/09/2022', 'grade': 10, "written_info": {}}],  # written_grades
            {'date': '08/09/2023', 'Valutazione/20,00': 10},  # written_doc_to_add
            {'date': '08/09/2023', 'index': 1, 'grade': 10, "flag_written_exam": 'OK',
             "written_info": {'date': '08/09/2023', 'Valutazione/20,00': 10}}  # result

    ),
])
def test_update_written_grade_case_1(written_grades, written_doc_to_add, result, ):
    # case 1 is when everything is fine, we just need to append the new exam
    db = MongoDBWrittenGrade(database_name="DSL_grade_test")
    db.date = '08/09/2023'
    output = db._update_written_grade(written_doc_to_add, written_grades, 0)
    assert output[-1] == result


@pytest.mark.parametrize("written_grades, written_doc_to_add, result", [
    (
            [],  # written_grades
            {'date': '08/09/2023', 'Valutazione/20,00': ""},  # written_doc_to_add
            {'date': '08/09/2023', 'index': 0, 'grade': None, "flag_written_exam": 'retired',
             "written_info": {'date': '08/09/2023', 'Valutazione/20,00': ""}}  # result
    ),
    (
            [{'date': '08/09/2023', 'index': 0, 'grade': 10, "written_info": {}}],  # written_grades
            {'date': '08/09/2023', 'Valutazione/20,00': ""},  # written_doc_to_add
            {'date': '08/09/2023', 'index': 0, 'grade': None, "flag_written_exam": 'retired',
             "written_info": {'date': '08/09/2023', 'Valutazione/20,00': ""}}  # result
    ),
])
def test_update_written_grade_case_2(written_grades, written_doc_to_add, result):
    # case 1 is when everything is fine, we just need to append the new exam
    db = MongoDBWrittenGrade(database_name="DSL_grade_test")
    db.date = '08/09/2023'
    output = db._update_written_grade(written_doc_to_add, written_grades, 0)
    assert output[-1] == result


@pytest.mark.parametrize("written_grades, written_doc_to_add, result", [
    (
            [],  # written_grades
            None,  # written_doc_to_add
            {'date': '08/09/2023', 'index': 0, 'grade': None, "flag_written_exam": 'absent',
             "written_info": None}  # result
    ),
])
def test_update_written_grade_case_3(written_grades, written_doc_to_add, result):
    # case 1 is when everything is fine, we just need to append the new exam
    db = MongoDBWrittenGrade(database_name="DSL_grade_test")
    db.date = '08/09/2023'
    output = db._update_written_grade(written_doc_to_add, written_grades, 0)
    assert output[-1] == result


@pytest.mark.parametrize("written_grades, written_doc_to_add, result", [
    (
            [{'date': '08/09/2023', 'index': 0, 'grade': 10, "flag_written_exam": 'OK',
              "written_info": {'date': '08/09/2023', 'Valutazione/20,00': 10}}],  # written_grades
            {'date': '08/09/2023', 'Valutazione/20,00': ""},  # written_doc_to_add
            {'date': '08/09/2023', 'index': 0, 'grade': None, "flag_written_exam": 'retired',
             "written_info": {'date': '08/09/2023', 'Valutazione/20,00': ""}}  # result
    ),
])
def test_update_written_grade_case_4(written_grades, written_doc_to_add, result):
    # case 1 is when everything is fine, we just need to append the new exam
    db = MongoDBWrittenGrade(database_name="DSL_grade_test")
    db.date = '08/09/2023'
    output = db._update_written_grade(written_doc_to_add, written_grades, 0)
    assert output[-1] == result


@pytest.mark.parametrize("written_grades, written_doc_to_add, result", [
    (
            [{'date': '08/09/2023', 'index': 0, 'grade': 10, "flag_written_exam": 'OK',
              "written_info": {'date': '08/09/2023', 'Valutazione/20,00': 10}}],  # written_grades
            None,  # written_doc_to_add
            {'date': '08/09/2023', 'index': 0, 'grade': None, "flag_written_exam": 'absent',
             "written_info": None}  # result
    ),
    (
            [{'date': '08/09/2022', 'index': 0, 'grade': 10, "flag_written_exam": 'OK',
              "written_info": {'date': '08/09/2023', 'Valutazione/20,00': 10}},
             {'date': '08/09/2023', 'index': 1, 'grade': 10, "flag_written_exam": 'OK',
              "written_info": {'date': '08/09/2023', 'Valutazione/20,00': 10}}
             ],  # written_grades
            None,  # written_doc_to_add
            {'date': '08/09/2023', 'index': 1, 'grade': None, "flag_written_exam": 'absent',
             "written_info": None}  # result
    ),
])
def test_update_written_grade_case_5(written_grades, written_doc_to_add, result):
    # case 1 is when everything is fine, we just need to append the new exam
    db = MongoDBWrittenGrade(database_name="DSL_grade_test")
    db.date = '08/09/2023'
    output = db._update_written_grade(written_doc_to_add, written_grades, 0)
    assert output[-1] == result


def test_consume_written_grade(mongo_db_student_grade, written_grade_df, registered_df, tmp_path):
    db = MongoDBWrittenGrade(database_name="DSL_grade_test",
                             )
    db.student_coll = mongo_db_student_grade

    db.consume_registered_students(written_grade_path=os.path.join(tmp_path, 'written_grade.csv'),
                                   registered_student_path=os.path.join(tmp_path, 'registered_student.csv'),
                                   threshold=0.0)
    # first student should contain only one exam for the date 08/09/2023
    student = mongo_db_student_grade.get_student("123")
    written_grades_student_1 = student['written_grades']
    assert len(written_grades_student_1) == 1
    assert written_grades_student_1[-1]['index'] == 0
    assert written_grades_student_1[-1]['date'] == '08/09/2023'
    assert written_grades_student_1[-1]['grade'] == 12.88
    assert written_grades_student_1[-1]['flag_written_exam'] == 'OK'
    # second student should contain one update exam
    student = mongo_db_student_grade.get_student("122")
    written_grades_student_2 = student['written_grades']
    assert len(written_grades_student_2) == 1
    assert written_grades_student_1[-1]['index'] == 0
    assert written_grades_student_2[-1]['date'] == '08/09/2023'
    assert written_grades_student_2[-1]['grade'] == None
    assert written_grades_student_2[-1]['flag_written_exam'] == 'retired'

    # third student should contain only one exam where he does not show
    student = mongo_db_student_grade.get_student("121")
    written_grades_student_3 = student['written_grades']
    assert written_grades_student_1[-1]['index'] == 0
    assert len(written_grades_student_3) == 1
    assert written_grades_student_3[-1]['date'] == '08/09/2023'
    assert written_grades_student_3[-1]['grade'] == None
    assert written_grades_student_3[-1]['flag_written_exam'] == 'absent'
