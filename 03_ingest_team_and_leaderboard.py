from dsl_grade_db.data_ingestor import MongoDBTeamsGrade


def main():
    db = MongoDBTeamsGrade(
        database_name="DSL_grade",
        teams_csv_file_path="data/project/project_01/teams_grade_01_session_2024.csv",
        leaderboard_csv_file_path="data/project/project_01/leaderboard_01_session_2024.csv"
    )

    db.consume_documents_in_teams()


if __name__ == "__main__":
    main()
