from __future__ import annotations

import pandas as pd
from pymongo import MongoClient

from dsl_grade_db.mongo_db_student_grade import MongoDBStudentGrade


class MongoDBTeamsGrade:
    """
    The core idea is that for each team we have maximum Grade.
    If the student is not assigned to any team we create one team with the maximum grade.
    We need both files because we want to use the idea of the "teams" only if there is
    a submission to the leaderboard in order to update the project grades.
    """

    def __init__(self, database_name, leaderboard_csv_file_path, teams_csv_file_path):

        self.client = MongoClient()
        self.db = self.client[database_name]
        self.student_coll = MongoDBStudentGrade(database_name=database_name)
        # read leaderboard and add to collection
        self.leaderboard_csv_file_path = leaderboard_csv_file_path
        self.teams_csv_file_path = teams_csv_file_path

    def set_project_id(self, project_id: str):
        self.student_coll.db_id.set_project_id(project_id)

    @property
    def date(self):
        return self.student_coll.db_id.get_project_id()

    def _parse_df(self, df) -> pd.DataFrame:
        if 'Timestamp' in df:  # true only for teams.csv
            # this is the date of the CURRENT exam session
            date = df['project_id']
            self.set_project_id(date)
            df['max_lead_grade'] = -1  # set it to -1 in case of no update
        return df

    def _create_student_id2_team_index(self, df_teams):
        student_id2_team_index = dict()
        for index, row in df_teams.iterrows():
            student_id2_team_index[row['Student ID # 1']] = index
            student_id2_team_index[row['Student ID # 2']] = index
        return student_id2_team_index

    def consume_documents_in_leaderboard(self):
        df_leaderboard = self._parse_df(pd.read_csv(self.leaderboard_csv_file_path))
        df_teams = self._parse_df(pd.read_csv(self.teams_csv_file_path))
        # create a hashmap where the key is the student_id and the value is the index of the team
        student_id2_team_index = self._create_student_id2_team_index(df_teams)

        # iterate over the leaderboard
        for _, lead_doc in df_leaderboard.iterrows():
            # get the team index
            student_id = lead_doc["matricola"]
            if student_id in student_id2_team_index:
                # there exists a team
                self.update_value_team_given_index(df_teams, student_id2_team_index[student_id], lead_doc)
            else:
                # there is no team so we insert a new one
                df_teams = pd.concat([df_teams, self.create_new_team(lead_doc)], axis=0).reset_index(drop=True)
                student_id2_team_index[student_id] = len(df_teams) - 1
        return df_teams

    def update_value_team_given_index(self, df_teams, index, lead_doc):
        # update the max_lead_grade
        if df_teams.loc[index, 'max_lead_grade'] < lead_doc['rounded_points']:
            df_teams.loc[index, 'max_lead_grade'] = lead_doc['rounded_points']

    def create_new_team(self, lead_doc):
        # create a new team
        return pd.DataFrame([{
            'Student ID # 1': lead_doc['matricola'],
            'Student ID # 2': None,
            'Timestamp': self.date,
            'project_id': self.date,
            'max_lead_grade': lead_doc['rounded_points']
        }])

    def consume_documents_in_teams(self):
        """update the students from teams collection"""

        def update_team(team_):
            student_id_1 = team_['Student ID # 1']
            student_id_2 = team_['Student ID # 2']
            update_student(student_id_1, team_)
            update_student(student_id_2, team_)

        def update_student(student_id_, team_):
            if student_id_:  # update only if it exists
                student_ = self.student_coll.get_student(student_id_)
                project_grades_ = self._update_project_grade(team=team_, project_grades=student_['project_grades'])
                self.student_coll.update_student_project_grade(student_id_, project_grades_)

        df_teams = self.consume_documents_in_leaderboard()
        df_teams.apply(lambda team: update_team(team.to_dict()), axis=1)

    @staticmethod
    def _update_project_grade(team, project_grades: list) -> list[dict]:
        def create_project_dict():
            return {
                'project_id': team['project_id'],
                'index': len(project_grades),
                'flag_project_exam': 'NO_REPORT',
                'leaderboard_grade': float(team['max_lead_grade']),
                'team_info': team
            }

        lead_grad = float(team['max_lead_grade'])

        if project_grades and team['project_id'] == project_grades[-1]['project_id']:
            project = project_grades.pop()
            project['leaderboard_grade'] = lead_grad
            if 'report_info' in project:
                project['flag_project_exam'] = 'OK'
            project['team_info'] = team
        else:
            project = create_project_dict()

        # if the student has taken the written exam
        project_grades.append(project)
        return project_grades
