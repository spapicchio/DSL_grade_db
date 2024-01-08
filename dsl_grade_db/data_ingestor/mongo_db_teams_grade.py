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
        self.date = None

    def set_project_date(self, date):
        self.date = date
        # the date now is present in the database id and can be used by the report
        self.student_coll.db_id.set_project_id(date)

    def _parse_df(self, df) -> pd.DataFrame:
        if 'Timestamp' in df:  # true only for teams.csv
            # from "1/3/2023 22:17:06" to "1/3/2023"
            df['project_id'] = df['Timestamp'].apply(lambda x: x.split(' ')[0])
            self.set_project_date(df['project_id'][0])
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
                project_grades_ = self._update_project_grade(team=team_,
                                                             project_grades=student_['project_grades'])
                self.student_coll.update_student_project_grade(student_id_, project_grades_)

        df_teams = self.consume_documents_in_leaderboard()
        df_teams.apply(lambda team: update_team(team.to_dict()), axis=1)

    def _update_project_grade(self, team, project_grades: list):
        project_date = [project['project_id'] for project in project_grades]
        if team['project_id'] in project_date:
            # update the project grade
            for project in project_grades:
                if project['project_id'] == team['project_id']:
                    # update only if there is at least one submission to leaderboard
                    # the initial value of max_lead_grade is -1
                    # in this way the student that does not submit to leaderboard
                    # will not have this project in the database
                    if 'leaderboard_grade' not in project and team['max_lead_grade'] >= 0:
                        project['leaderboard_grade'] = float(team['max_lead_grade'])
                        project['final_grade'] += float(team['max_lead_grade'])
                        project['team_info'] = team
                        break
        else:
            # initialize the project document
            project_grades.append({
                'project_id': team['project_id'],
                'leaderboard_grade': float(team['max_lead_grade']),
                'final_grade': float(team['max_lead_grade']),
                'team_info': team
            })
        return project_grades
