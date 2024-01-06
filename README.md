# Data Science Lab (DSL) Exam Management System
This project serves as the core database for managing student grades for the course Data Science Lab (DSL) at "Politecnico di Torino".
providing a robust and efficient solution for data storage and retrieval.
Developed to support real-world scenarios, this backend is designed to handle the complexities of student data management seamlessly.
The backend is developed using MongoDB.

# Core Ideas
The database is composed of two different collections:

- ***"student_grade"*** collection: stores the grades of each student for each exam
- ***"enrolled_students"*** collection: stores the list of students enrolled to the course 

Document in *studen_grade* collection:
```python
student_grade_document = {
    "_id": Objectid(...),
    "student_id": "281604"
    "db_id": Objectid(X)  # Same object id
    "name": "Simone",
    "surname": "Papicchio",
    "written_grades":[],
    "project_grades":[]
}
```

Document in *enrolled_students* collection:
```python
enrolled_student_document = {
    "_id": Objectid(X),  # Same object id
    "student_id": "281604"
    "name": "Simone",
    "surname": "Papicchio",
}
```

The *student_grade_document* is accessed only through the ***db_id*** extracted from *enrolled_student_document*. In this way if a student changes its student_id, it is updated only *enrolled_students*.

- ***written_grades*** is a list of written exams sorted by date
- ***project_grades*** is a list of projects sorted by the date. The date is also used as key to combine grades from leaderboard, teams and reports.
Document in *written_grades* array:
```python
written_grade = {
    'date': '08/09/2021',
    'grade': 5,
    "written_info": {}
}
```
Document in *project_grades* array:
```python
project_grade = {
    'project_id': "1/3/2021",
    'report_grade': 10,
    'leaderboard_grade': 3,
    'final_grade': 13,
    'report_info': {},
    'team_info': {}
}
```
# How to ingest data?
The only collections we keep records are *students_grade* and *enrolled_students*. All the other files are immediately consumed to populate the stored collections.
## 1. mongo_db_enrolled_student.py
From the enrolled_students.csv files create the new Students (only if they do not already exist) in *students_grade* and *enrolled_students*.

**N.B. this is the only place where *enrolled_students* is populate**

## 2. mongo_db_written_grade.py
Given the written_grade.csv file, append the written exam for each students.

## 3. mongo_db_teams_grade.py
The core idea is that for each team we have maximum Grade.
If the student is not assigned to any team we create one team with the maximum grade.
We need both files because we want to use the idea of the "teams" only if there is 
a submission to the leaderboard in order to update the project grades.


Example:
```python
teams = [
  {'date': '1/3/2021', 'student_id_1': '123', 'student_id_1': '122', 'max_leaderboard_grade': -1},
  {'date': '1/3/2021', 'student_id_1': '124', 'student_id_1': '125', 'max_leaderboard_grade': -1},

]

leaderboard = [
  {'student': '123', 'lead_grade': 2}, 
  {'student': '123', 'lead_grade': 3}, 
  {'student': '122', 'lead_grade': 4}, 
  {'student': '122', 'lead_grade': 10},
  {'student': '121', 'lead_grade': 7},
]
```

The leaderboard file is used to populate the teams file following these rules:
- only the max grade is saved for each team
- if no team is present, one is created
Teams after this step:
```python
teams = [
  {'date': '1/3/2021', 'student_id_1': '123', 'student_id_1': '122', 'max_leaderboard_grade': 10},
  {'date': '1/3/2021', 'student_id_1': '124', 'student_id_1': '125', 'max_leaderboard_grade': -1},
  {'date': '1/3/2021', 'student_id_1': '121', 'student_id_1': '', 'max_leaderboard_grade': 7},
]
```
No the Teams is consumed to populate each student ONLY if max_leaderboard_grade is different from -1:
```python
teams = [] # consumed
students = [
{"student_id": "123"
 "db_id": Objectid(...)  
 "written_grades":[],
 "project_grades":[{'project_id': "1/3/2021", 'leaderboard_grade': 10, 'final_grade': 10, 'team_info': {}}]
},
{"student_id": "122"
 "db_id": Objectid(...) 
 "written_grades":[],
 "project_grades":[{'project_id': "1/3/2021", 'leaderboard_grade': 10, 'final_grade': 10, 'team_info': {}}]
},
{"student_id": "121"
 "db_id": Objectid(...)  
 "written_grades":[],
 "project_grades":[{'project_id': "1/3/2021", 'leaderboard_grade': 7, 'final_grade': 7, 'team_info': {}}]
},
```

## 4. mongo_db_report_grade.py
The report grades are assigned to the same project_id ONLY IF the "max written exams" is higher than threshold

# How to access students?
- MongoDBStudentGrade().get_students_project_session() returns all the students with the last project completed
- MongoDBStudentGrade().update_students_have_rejected(students: list) remove the last written/projects because rejection
- MongoDBStudentGrade().remove_student(student: dict) remove the student when they accept the grade


