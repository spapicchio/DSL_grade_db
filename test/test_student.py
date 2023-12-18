import datetime
import pytest

from dsl_grade_db.student import Student, WrittenGrade, ProjectGrade


# Replace "your_module" with the actual module name where your classes are defined

@pytest.fixture
def sample_student():
    return Student(
        student_id="123",
        db_id=1,
        name="John",
        surname="Doe",
        written_grades=[
            WrittenGrade(date=datetime.datetime(2022, 1, 1), grade=95),
            WrittenGrade(date=datetime.datetime(2022, 2, 1), grade=89),
        ],
        project_grades=[
            ProjectGrade(date=datetime.datetime(2022, 1, 1), leaderboard_grade=92, report_grade=90),
            ProjectGrade(date=datetime.datetime(2022, 2, 1), leaderboard_grade=88, report_grade=85),
        ],
    )


def test_final_grade(sample_student):
    assert sample_student.final_grade == 89 + 90 + 92


def test_is_report_to_correct_true(sample_student):
    assert sample_student.is_report_to_correct(89.0) is True


def test_is_report_to_correct_false(sample_student):
    assert sample_student.is_report_to_correct(96.0) is False


def test_get_max_written_grade(sample_student):
    max_written_grade = sample_student.get_max_written_grade()
    assert max_written_grade.grade == 89


def test_get_max_report_grade(sample_student):
    max_report_grade = sample_student.get_max_report_grade()
    assert max_report_grade.final_grade == 90 + 92
