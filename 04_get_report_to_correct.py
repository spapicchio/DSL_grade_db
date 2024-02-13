import pandas as pd

from dsl_grade_db import MongoDBStudentGrade

THRESHOLD = 15

STUDENTS_ID_CORRECT_SAVE_FILE = "students_id_to_correct.txt"


def main(save_path):
    student_db = MongoDBStudentGrade(database_name="DSL_grade")
    # return
    # all the students that satisfy the following conditions:
    # 1. have a max written grade greater than THRESHOLD
    # 2. have participated in the last project (the project is associated with the last leaderboard ingested)
    # 3. The report for this student has not yet been assigned
    teams = student_db.get_teams_to_correct_report()
    df = pd.DataFrame({
        'Student ID': list(teams),
        'Student ID for report submission': None,
        'Reviewer': None,
        "Relevance of contents": None,
        "Clarity of explanations": None,
        "Presentation": None,
        "Quality and originality of contributions": None,
        "Raw Total( / 20)": None,
        "Score( / 8)": None,
        "Final score": None,
        "Report written w/ LLMs (1 -not at all- to 3 -entirely-)": None,
        "Notes": None
    })
    df = df.explode('Student ID')
    create_excell(df, save_path)


def create_excell(df, save_path: str):
    # Create a Pandas Excel writer using XlsxWriter as the engine
    writer = pd.ExcelWriter(save_path, engine='xlsxwriter')

    # Convert the dataframe to an XlsxWriter Excel object
    df.to_excel(writer, sheet_name='Report', index=False)

    # Get the xlsxwriter workbook and worksheet objects from the dataframe
    workbook = writer.book
    worksheet = writer.sheets['Report']

    # Create a format for the even rows
    team_1_stud_1 = workbook.add_format({'bg_color': '#DDEBF7'})
    team_1_stud_2 = workbook.add_format({'bg_color': '#DDEBF7', 'bottom': 1})

    # Create a format for the odd rows
    team_2_stud_2 = workbook.add_format({'bottom': 1})

    # Add conditional format to alternate rows
    for row in range(0, len(df) + 1, 4):
        worksheet.set_row(row + 1, 15, team_1_stud_1)
        worksheet.set_row(row + 2, 15, team_1_stud_2)
        worksheet.set_row(row + 4, 15, team_2_stud_2)

    for row in range(2, len(df) + 1):
        worksheet.write_formula(f'H{row}', f'=SUM(D{row},E{row},F{row},G{row})')
        worksheet.write_formula(f'I{row}', f'=H{row} / 20 * 8')

    # Close the Pandas Excel writer and output the Excel file
    writer._save()


if __name__ == "__main__":
    main(save_path='report_to_correct.xlsx')
