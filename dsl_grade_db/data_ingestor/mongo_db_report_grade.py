import pandas as pd
from pymongo import MongoClient

from dsl_grade_db.mongo_db_student_grade import MongoDBStudentGrade


class MongoDBReportGrade:
    def __init__(self, database_name, report_csv_file_path):
        self.client = MongoClient()
        self.db = self.client[database_name]
        self.student_coll = MongoDBStudentGrade(database_name=database_name)
        self.report_csv_file_path = report_csv_file_path

    @property
    def date(self):
        return self.student_coll.db_id.get_project_id()

    def _read_report_df(self, df):
        """the project ID is added automatically"""
        project_id = self.student_coll.db_id.get_project_id()
        df['project_id'] = project_id
        return df

    def consume_reports(self):
        df = self._read_report_df(pd.read_csv(self.report_csv_file_path))
        df.apply(self._update_project_grade, axis=1)

    def _update_project_grade(self, student_report):
        def create_project_dict():
            return {
                'project_id': self.date,
                'index': len(project_grades),
                'flag_project_exam': 'NO_LEADERBOARD',
                'report_grade': float(student_report['Final score']),
                'report_extra_grade': float(student_report['report_extra_grade']),
                'report_info': student_report.to_dict(),
            }

        report_grade = float(student_report['Final score'])
        report_extra_grade = float(student_report['report_extra_grade'])

        student_id = student_report['Matricola']

        student = self.student_coll.get_student(student_id)
        project_grades = student['project_grades']

        if project_grades and self.date == project_grades[-1]['project_id']:
            # this means this project already exists.
            project = project_grades.pop()
            project['report_grade'] = report_grade
            project['report_extra_grade'] = report_extra_grade
            # you need to change only if there is the leaderboard
            if 'team_info' in project:
                project['flag_project_exam'] = 'OK'
            project['report_info'] = student_report.to_dict()
        else:
            project = create_project_dict()

        project_grades.append(project)
        self.student_coll.update_student_project_grade(student_id, project_grades)
