import locale
from datetime import datetime

import pandas as pd
from pymongo import MongoClient

from dsl_grade_db.mongo_db_student_grade import MongoDBStudentGrade


class MongoDBWrittenGrade:
    def __init__(self, written_collection_name="written_grade",
                 student_collection_name="student_grade",
                 written_csv_file_path="written_grade.csv"):
        self.client = MongoClient()
        self.db = self.client["DSL_grade_dbs"]
        self.written_coll = self.db[written_collection_name]
        self.student_coll: MongoDBStudentGrade = self.db[student_collection_name]
        self.written_df = self._parse_written_csv_file(pd.read_csv(written_csv_file_path))
        self._insert_in_collections(self.written_coll, self.written_df)

    @staticmethod
    def _parse_written_csv_file(df):
        # 1 create data column
        # transform from "8 settembre 2023  08:25" to "08/09/2023"
        locale.setlocale(locale.LC_TIME, 'it_IT')
        df['date'] = df.Iniziato.map(
            lambda x: datetime.strptime(x, '%d %B %Y %H:%M').strftime('%d/%m/%Y')
        )
        # 2 remove the comma in the float values
        columns = ['Valutazione/20,00',
                   'D. 1 /0,00', 'D. 2 /1,50', 'D. 3 /1,50', 'D. 4 /1,50', 'D. 5 /1,50',
                   'D. 6 /2,50', 'D. 7 /1,50', 'D. 8 /1,50', 'D. 9 /1,50', 'D. 10 /1,50',
                   'D. 11 /1,50', 'D. 12 /2,00', 'D. 13 /2,00']
        for col in columns:
            df[col] = df[col].str.replace(",", ".")
        # 3 create a new column for student ID
        # from "01twzsm0_it_23_p1070_s313385" to "313385"
        df['student_id'] = df['Username'].map(lambda x: x.split('_')[-1][1:])
        return df

    @staticmethod
    def _insert_in_collections(collection, df):
        collection.insert_many(df.to_dict('records'))

    def consume_documents(self):
        for written_doc in self.written_coll.find():
            student_id = written_doc['student_id']
            student = self.student_coll.get_student(student_id)
            written_grades = self._update_written_grade(written_doc=written_doc,
                                                        written_grades=student[
                                                            'written_grades'])
            self.student_coll.update_student_written_grade(student_id, written_grades)
        # drop the consumed collection
        self.written_coll.drop()

    def _update_written_grade(self, written_doc, written_grades: list[dict]):
        # the written grades are recognized by the date (only one written grade per date)
        written_dates = [written['date'] for written in written_grades]
        if written_doc['date'] not in written_dates:
            # initialize the written grade document
            written_grades.append({
                'date': written_doc['date'],
                'grade': float(written_doc['Valutazione/20,00']),
                'written_info': written_doc
            })
        return written_grades
