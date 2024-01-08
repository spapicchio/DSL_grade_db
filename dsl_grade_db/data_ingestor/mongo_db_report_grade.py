import pandas as pd
from pymongo import MongoClient

from dsl_grade_db.mongo_db_student_grade import MongoDBStudentGrade


class MongoDBReportGrade:
    def __init__(self, database_name, report_csv_file_path):
        self.client = MongoClient()
        self.db = self.client[database_name]
        self.student_coll = MongoDBStudentGrade(database_name=database_name)
        self.report_csv_file_path = report_csv_file_path

    def _read_report_df(self, df):
        """the project ID is added automatically"""
        project_id = self.student_coll.db_id.get_project_id()
        df['project_id'] = project_id
        return df

    def consume_reports(self):
        df = self._read_report_df(pd.read_csv(self.report_csv_file_path))
        df.apply(self._update_project_grade, axis=1)

    def _get_student_project_grade(self, student_id):
        student = self.student_coll.get_student(student_id)
        return student['project_grades']

    def _update_project_grade(self, student_report):
        student_id = student_report['Matricola']
        student_project_grades = self._get_student_project_grade(student_id)
        project_ids = [project['project_id'] for project in student_project_grades]
        if student_report['project_id'] in project_ids:
            # update the project grade
            for project in student_project_grades:
                if project['project_id'] == student_report['project_id']:
                    if 'report_grade' not in project:
                        project['report_grade'] = float(student_report['Final score'])
                        project['final_grade'] += float(student_report['Final score'])
                        project['report_info'] = student_report.to_dict('records')
                        break
        else:
            # initialize the project document
            student_project_grades.append({
                'project_id': str(student_report['project_id']),
                'report_grade': float(student_report['Final score']),
                'final_grade': float(student_report['Final score']),
                'report_info': student_report.to_dict('records'),
            })
        self.student_coll.update_student_project_grade(student_id, student_project_grades)
