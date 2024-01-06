import pandas as pd
from pymongo import MongoClient

from dsl_grade_db.mongo_db_student_grade import MongoDBStudentGrade


class MongoDBReportGrade:
    def __init__(self, database_name, report_csv_file_path):
        self.client = MongoClient()
        self.db = self.client[database_name]
        self.report_coll = self.db["report_grade"]
        self.student_coll = MongoDBStudentGrade(database_name=database_name)
        self.report_csv_file_path = report_csv_file_path

    def _read_and_insert(self, df, collection):
        """the project ID is added automatically"""
        project_id = self.student_coll.db_id.get_project_id()
        df['project_id'] = project_id
        collection.insert_many(df.to_dict('records'))

    def consume_documents(self):
        self._read_and_insert(pd.read_csv(self.report_csv_file_path), self.report_coll)
        for report in self.report_coll.find():
            student_id = report['Matricola']
            student = self.student_coll.get_student(student_id)
            project_grades = self._update_project_grade(report=report,
                                                        project_grades=student[
                                                            'project_grades'])
            self.student_coll.update_student_project_grade(student_id, project_grades)
        self.report_coll.drop()

    def _update_project_grade(self, report, project_grades: list[dict]):
        project_ids = [project['project_id'] for project in project_grades]
        if report['project_id'] in project_ids:
            # update the project grade
            for project in project_grades:
                if project['project_id'] == report['project_id']:
                    if 'report_grade' not in project:
                        project['report_grade'] = float(report['Final score'])
                        project['final_grade'] += float(report['Final score'])
                        project['report_info'] = report
                        break
        else:
            # initialize the project document
            project_grades.append({
                'project_id': str(report['project_id']),
                'report_grade': float(report['Final score']),
                'final_grade': float(report['Final score']),
                'report_info': report
            })
        return project_grades
