import pytest
from bson import ObjectId

from dsl_grade_db.dsl_student_id_database import MongoDBStudentId


# Fixture to create an instance of DSLDatabaseIdDatabase for testing
@pytest.fixture
def mongo_database():
    db = MongoDBStudentId(database_name="DSL_grade_test")
    yield db
    db.collection.drop()
    db.close()


# Test for adding a student ID
def test_add_student_id(mongo_database):
    document = {'MATRICOLA': "123"}
    mongo_database.add_student_id(document)
    assert mongo_database.collection.find_one(
        {"MATRICOLA": document['MATRICOLA']}) is not None
    # remove the student ID otherwise it will be present in the database!
    mongo_database.remove_student_id(document['MATRICOLA'])


def test_add_student_id_twice(mongo_database):
    """the counter must not increase"""
    document = {'MATRICOLA': "123"}
    mongo_database.add_student_id(document)
    mongo_database.add_student_id(document)
    assert mongo_database.collection.find_one(
        {"MATRICOLA": document['MATRICOLA']}) is not None
    assert mongo_database.collection.count_documents(
        {"MATRICOLA": document['MATRICOLA']}) == 1
    # remove the student ID otherwise it will be present in the database!
    mongo_database.remove_student_id(document['MATRICOLA'])


def test_remove_student_id(mongo_database):
    student_id = "456"
    document = {'MATRICOLA': student_id}
    mongo_database.add_student_id(document)
    mongo_database.remove_student_id(student_id)
    assert mongo_database.collection.find_one({"MATRICOLA": student_id}) is None


def test_update_student_id(mongo_database):
    student_id = "789"
    new_student_id = "999"
    mongo_database.add_student_id({'MATRICOLA': student_id})
    key_student_id = mongo_database.get_db_id_from(student_id)
    mongo_database.update_student_id(student_id, new_student_id)
    key_new_student_id = mongo_database.get_db_id_from(new_student_id)
    assert mongo_database.collection.find_one({"MATRICOLA": student_id}) is None
    assert mongo_database.collection.find_one({"MATRICOLA": new_student_id}) is not None
    assert key_student_id == key_new_student_id  # same key in the database
    # remove the student ID otherwise it will be present in the database!
    mongo_database.remove_student_id(new_student_id)


def test_get_db_id_from(mongo_database):
    student_id = "111"
    mongo_database.add_student_id({'MATRICOLA': student_id})
    db_id = mongo_database.get_db_id_from(student_id)
    assert db_id is not None
    assert isinstance(db_id, ObjectId)
    # remove the student ID otherwise it will be present in the database!
    mongo_database.remove_student_id(student_id)


def test_get_db_id_from_nonexistent_student_id(mongo_database):
    student_id = "nonexistent"
    with pytest.raises(KeyError):
        db_id = mongo_database.get_db_id_from(student_id)


def test_get_student_id_from(mongo_database):
    student_id = "123"
    document = mongo_database.collection.insert_one({"MATRICOLA": student_id})
    retrieved_student_id = mongo_database.get_student_id_from(document.inserted_id)
    assert retrieved_student_id == student_id
    mongo_database.remove_student_id(student_id)


def test_get_student_id_from_nonexistent_id(mongo_database):
    nonexistent_db_id = ObjectId()
    with pytest.raises(KeyError):
        mongo_database.get_student_id_from(nonexistent_db_id)
