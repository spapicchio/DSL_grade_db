from __future__ import annotations

import locale
import math
from datetime import datetime

import pandas as pd
from pymongo import MongoClient

from .. import MongoDBStudentGrade


class MongoDBWrittenGrade:
    def __init__(self, database_name="DSL_grade_dbs",
                 written_grade_csv_file_path="written_grade.csv",
                 registered_student_csv_file_path="registered_student.csv"):

        self.client = MongoClient()
        self.db = self.client[database_name]
        self.student_coll = MongoDBStudentGrade(database_name=database_name)

        self.written_grade_csv_file_path = written_grade_csv_file_path
        self.registered_student_csv_file_path = registered_student_csv_file_path
        self.date = None

    def set_project_date(self, written_df):
        date = written_df['date'][0]
        # the date now is present in the database id and can be used by the report and the leaderboard
        self.student_coll.db_id.set_project_id(date)
        return date

    @staticmethod
    def _read_written_grade(written_grade_csv_file_path):
        df = pd.read_csv(written_grade_csv_file_path)
        # 1 create data column
        # transform from "8 settembre 2023  08:25" to "08/09/2023"
        locale.setlocale(locale.LC_TIME, 'it_IT')
        df['date'] = df.Iniziato.map(
            lambda x: datetime.strptime(x, '%d %B %Y %H:%M').strftime('%d/%m/%Y')
        )
        # 2 remove the comma in the float values
        for col in df.columns:
            df[col] = df[col].str.replace(",", ".")
        # 3 create a new column for student ID
        # from "01twzsm0_it_23_p1070_s313385" to "313385"
        df['student_id'] = df['Username'].map(lambda x: x.split('_')[-1][1:])
        # Set None values to NONE
        df = df.where(df.notnull(), None)
        # set index
        df.set_index('student_id', inplace=True)
        return df

    @staticmethod
    def _read_registered_student(registered_student_csv_file_path):
        df = pd.read_csv(registered_student_csv_file_path)
        df['student_id'] = df['MATRICOLA'].astype(str)
        return df

    def consume_registered_students(self):
        def _update_students(student_id_):
            # get the student
            student_ = self.student_coll.get_student(student_id_)
            # if the student was absent you get None, otherwise you get the written grade to add
            student_exam_ = written_df.loc[student_id_].to_dict() if student_id_ in present_student_in_class else None
            # update the written grades of the student
            written_grades = self._update_written_grade(written_doc_to_add=student_exam_,
                                                        written_grades=student_['written_grades'])
            # update the database only if there is an update
            if written_grades:
                self.student_coll.update_student_written_grade(student_id_, written_grades)

        written_df = self._read_written_grade(self.written_grade_csv_file_path)
        self.date = self.set_project_date(written_df)
        registered_df = self._read_registered_student(self.registered_student_csv_file_path)

        present_student_in_class = written_df.index
        registered_df.apply(lambda row: _update_students(row['student_id']), axis=1)

    def _update_written_grade(self, written_doc_to_add: dict | None, written_grades: list[dict]) -> list[dict] | None:

        set_flag = lambda x: 'OK' if x['Valutazione/20,00'] else 'retired'

        def create_grade_dict():
            return {
                'date': self.date,
                'grade': float(grade) if grade else None,
                "flag_written_exam": set_flag(written_doc_to_add) if written_doc_to_add else 'absent',
                'written_info': written_doc_to_add
            }

        grade = written_doc_to_add['Valutazione/20,00'] if written_doc_to_add else None
        # if the student has not yet taken the written exam
        if written_grades and written_grades[-1]['date'] == self.date:
            if written_grades[-1]['grade'] != grade:
                # if there is at least one grade and this grade is the current exam
                # but the grade has changed then you pop
                written_grades.pop()
            else:
                return None

        # if the student has taken the written exam
        written_grades.append(create_grade_dict())
        return written_grades
