from dsl_grade_db.data_ingestor import MongoDBTeamsGrade


def main():
    db = MongoDBTeamsGrade(database_name="DSL_grade", project_id='project_winter_2024')
    db.consume_documents_in_teams(
        leaderboard_path='data/03_project/project_01/dsl_project_winter_leaderboard_2024.xlsx',
        teams_path='data/03_project/project_01/dsl_project_winter_teams_2024.xlsx'
    )


if __name__ == "__main__":
    main()
