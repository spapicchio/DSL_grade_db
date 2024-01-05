import pandas as pd
from pymongo import MongoClient

from dsl_grade_db.mongo_db_student_grade import MongoDBStudentGrade


class MongoDBTeamsGrade:
    """
    The core idea is that for each team we have maximum Grade.
    If the student is not assigned to any team we create one team with the maximum grade.
    """

    def __init__(self, leaderboard_collection_name="leaderboard_grade",
                 student_collection_name="student_grade",
                 teams_collections_grade="teams_grade",
                 leaderboard_csv_file_path="leaderboard_csv",
                 teams_csv_file_path="teams_csv"):

        self.client = MongoClient()
        self.db = self.client["DSL_grade_dbs"]
        self.leaderboard_coll = self.db[leaderboard_collection_name]
        self.teams_coll = self.db[teams_collections_grade]
        self.student_coll: MongoDBStudentGrade = self.db[student_collection_name]
        # read leaderboard and add to collection
        self.leaderboard_csv_file_path = leaderboard_csv_file_path
        self.teams_csv_file_path = teams_csv_file_path
        self.date = None

    def set_project_date(self, date):
        self.date = date
        # the date now is present in the database id and can be used by the report
        self.student_coll.db_id.set_project_id(date)

    def _parse_df_and_insert(self, df, collection):
        if 'Timestamp' in df:  # true only for teams.csv
            # from "1/3/2023 22:17:06" to "1/3/2023"
            df['project_id'] = df['Timestamp'].apply(lambda x: x.split(' ')[0])
            self.set_project_date(df['project_id'][0])
            df['max_lead_grade'] = 0 # set it to zero in case of no update
        collection.insert_many(df.to_dict('records'))

    def consume_documents_in_leaderboard(self):
        self._parse_df_and_insert(pd.read_csv(self.leaderboard_csv_file_path),
                                  self.leaderboard_coll)
        # read teams and add to collection
        self._parse_df_and_insert(pd.read_csv(self.teams_csv_file_path), self.teams_coll)
        # iterate over the leaderboard
        for lead_doc in self.leaderboard_coll.find():
            team = self.teams_coll.find_one({
                "$or": [
                    {"Student ID # 1": lead_doc["matricola"]},
                    {"Student ID # 2": lead_doc["matricola"]}
                ]
            })

            if not team:
                # there is no team with this student
                self.insert_in_teams(lead_doc)
            else:
                # there is a team with this student
                self.update_team_max_lead_grade(team["_id"], lead_doc)
        self.leaderboard_coll.drop()

    def update_team_max_lead_grade(self, team_id, lead_doc):
        """Update the max_lead_grade only if:
        - max_lead_grade does not exist yet
        - the new one is greater or equal to the one present
        """
        self.teams_coll.update_one(
            {"$and": [{"_id": team_id},
                      {"$or": [  # max lead does not exist or it has a lower value
                          {"max_lead_grade": {"$exists": False}},
                          {"max_lead_grade": {"$lt": lead_doc["rounded_points"]}}]}
                      ]
             },
            {"$set": {
                "max_lead_grade": float(lead_doc["rounded_points"]),
                "leaderboard_info": lead_doc
            }}
        )

    def insert_in_teams(self, lead_doc):
        """Create new team with only one student"""
        document = {
            "Timestamp": "",
            "project_id": self.date,
            "Student ID # 1": lead_doc["matricola"],
            "Student ID # 2": "",
            "max_lead_grade": float(lead_doc["rounded_points"]),
            "leaderboard_info": lead_doc
        }
        self.teams_coll.insert_one(document)

    def consume_documents_in_teams(self):
        """update the students from teams collection"""
        # TODO What happen if one team does not have any leaderboard submission?
        # TODO Now we set everything to 0 so in case it is saved as zero
        def update_student(student_id_, team_):
            if student_id_:  # update only if it exists
                student_ = self.student_coll.get_student(student_id_)
                project_grades_ = self._update_project_grade(team=team_,
                                                             project_grades=student_[
                                                                 'project_grades'])
                self.student_coll.update_student_project_grade(student_id_,
                                                               project_grades_)

        self.consume_documents_in_leaderboard()
        for team in self.teams_coll.find():
            student_id_1 = team['Student ID # 1']
            student_id_2 = team['Student ID # 2']
            update_student(student_id_1, team)
            update_student(student_id_2, team)
        self.teams_coll.drop()

    def _update_project_grade(self, team, project_grades: list):
        project_date = [project['project_id'] for project in project_grades]
        if team['project_id'] in project_date:
            # update the project grade
            for project in project_grades:
                if project['project_id'] == team['project_id']:
                    if 'leaderboard_grade' not in project:
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
