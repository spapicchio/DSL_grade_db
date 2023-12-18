import shutil

import pytest

from dsl_grade_db.dsl_student_id_database import DSLDatabaseIdDatabase


# Fixture to create an instance of DSLDatabaseIdDatabase for testing
@pytest.fixture
def test_db_instance(tmpdir):
    # Use a temporary directory for testing
    test_db_path = tmpdir.mkdir("test_database")
    test_db_instance = DSLDatabaseIdDatabase(str(test_db_path))
    return test_db_instance


# Test for adding a student ID
def test_add_student_id(test_db_instance):
    test_db_instance.add_student_id("john_doe")
    assert test_db_instance.db_json['counter'] == 1
    assert test_db_instance.db_json['john_doe'] == 1


def test_add_student_id_twice(test_db_instance):
    """the counter must not increase"""
    test_db_instance.add_student_id("john_doe")
    test_db_instance.add_student_id("john_doe")
    assert test_db_instance.db_json['counter'] == 1
    assert test_db_instance.db_json['john_doe'] == 1


def test_add_multiple_student_id(test_db_instance):
    """the counter must not increase"""
    test_db_instance.add_student_id("john_doe")
    test_db_instance.add_student_id("simone_doe")
    assert test_db_instance.db_json['counter'] == 2
    assert test_db_instance.db_json['john_doe'] == 1
    assert test_db_instance.db_json['simone_doe'] == 2


def test_add_multiple_student_id_and_remove(test_db_instance):
    """the counter must not increase"""
    test_db_instance.add_student_id("john_doe")
    test_db_instance.add_student_id("simone_doe")
    test_db_instance.remove_student_id("john_doe")
    assert test_db_instance.db_json['counter'] == 1
    assert test_db_instance.db_json['simone_doe'] == 2


# Test for updating a student ID
def test_update_student_id(test_db_instance):
    test_db_instance.add_student_id("john_doe")
    test_db_instance.update_student_id("john_doe", "jane_doe")
    assert 'john_doe' not in test_db_instance.db_json
    assert test_db_instance.db_json['jane_doe'] == 1
    assert test_db_instance.db_json['counter'] == 1


# Test for removing a student ID
def test_remove_student_id(test_db_instance):
    test_db_instance.add_student_id("john_doe")
    test_db_instance.remove_student_id("john_doe")
    assert 'john_doe' not in test_db_instance.db_json
    assert test_db_instance.db_json['counter'] == 0


# Test for getting a student ID
def test_get_student_id(test_db_instance):
    test_db_instance.add_student_id("john_doe")
    counter_value = test_db_instance.get_student_id("john_doe")
    assert counter_value == 1


# Test for initializing the database with existing data
def test_init_db_with_data(tmpdir):
    test_db_path = tmpdir.mkdir("test_database")
    db_instance = DSLDatabaseIdDatabase(test_db_path)

    # Add some initial data
    db_instance.add_student_id("john_doe")
    db_instance.add_student_id("jane_doe")
    db_instance.close()

    # Create a new instance to test initialization
    new_instance = DSLDatabaseIdDatabase(test_db_path)

    # Check if the data is loaded correctly
    assert new_instance.db_json['counter'] == 2
    assert new_instance.db_json['john_doe'] == 1
    assert new_instance.db_json['jane_doe'] == 2
    new_instance.close()


