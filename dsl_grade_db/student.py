from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import List


@dataclass
class WrittenGrade:
    date: datetime
    grade: int


@dataclass
class ProjectGrade:
    date: datetime
    leaderboard_grade: float
    report_grade: float

    @property
    def final_grade(self):
        return self.leaderboard_grade + self.report_grade


@dataclass
class Student:
    student_id: str
    db_id: int
    name: str
    surname: str
    written_grades: List[WrittenGrade] = None
    project_grades: List[ProjectGrade] = None

    @property
    def final_grade(self):
        return self.get_max_written_grade().grade + self.get_max_report_grade().final_grade

    def is_report_to_correct(self, threshold: float) -> bool:
        """Correct the report ONLY if the student has a written grade > THRESHOLD"""
        max_written_grade = self.get_max_written_grade()
        return max_written_grade.grade >= threshold

    def get_max_written_grade(self) -> WrittenGrade:
        """Return only the last written grade. Possible to change to the max grade"""
        if self.written_grades is None:
            return WrittenGrade(date=None, grade=0)
        return self.written_grades[-1]

    def get_max_report_grade(self) -> ProjectGrade:
        if self.project_grades is None:
            return ProjectGrade(date=None, leaderboard_grade=0, report_grade=0)
        return max(self.project_grades, key=lambda x: x.final_grade)
