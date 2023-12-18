import json
import os


class DSLDatabaseIdDatabase:
    """
       DSLDatabaseIdDatabase - A class for managing a simple student ID database with a counter.

       Attributes:
           db_path (str): The path to the directory where the database file is stored.
           db_file_path (str): The full path to the JSON database file.
           db_json (dict): The dictionary representation of the database. The structure is as follows:
               {
                   "counter": 0,
                   "student_id_1": 1,
                   "student_id_2": 2,
                   ...
               }

       Methods:
           __init__(db_id_path): Initializes the DSLDatabaseIdDatabase object.
           add_student_id(student_id: str): Adds a new student ID to the database.
           update_student_id(student_id: str, new_student_id: str): Updates an existing student ID in the database.
           remove_student_id(student_id: str): Removes a student ID from the database.
           get_student_id(student_id: str) -> int: Retrieves the counter value for a given student ID.
           _init_db() -> dict: Initializes the database by loading existing data or creating a new one.
           _save_db(): Saves the current state of the database to the JSON file.

       Example Usage:
           # Create a new instance of DSLDatabaseIdDatabase
           db = DSLDatabaseIdDatabase("/path/to/database")

           # Add a new student ID
           db.add_student_id("john_doe")

           # Update a student ID
           db.update_student_id("john_doe", "jane_doe")

           # Remove a student ID
           db.remove_student_id("jane_doe")

           # Get the counter value for a student ID
           counter_value = db.get_student_id("john_doe")

       Note:
           - The database file is a JSON file named 'student_id_db.json' located in the specified directory.
           - The database structure includes a counter for tracking the number of student IDs.
           - The database is automatically initialized when an instance of DSLDatabaseIdDatabase is created.
           - Changes to the database are persisted to the file using the _save_db method.
       """

    def __init__(self, db_id_path):
        self.db_path = db_id_path
        self.db_file_path = os.path.join(self.db_path, 'student_id_db.json')
        self.db_json = self._init_db()

    def add_student_id(self, student_id: str):
        """
        Add a new student ID to the database.

        Args:
            student_id (str): The student ID to be added.
        """
        if student_id not in self.db_json:
            self.db_json['counter'] += 1
            # create only if it does not exist yet
            self.db_json[student_id] = self.db_json['counter']

    def update_student_id(self, student_id: str, new_student_id: str):
        """
        Updates an existing student ID in the database.

        Args:
            student_id (str): The existing student ID to be updated.
            new_student_id (str): The new student ID to replace the existing one.
        """
        self.db_json[new_student_id] = self.db_json[student_id]
        del self.db_json[student_id]

    def remove_student_id(self, student_id: str):
        """
        Removes a student ID from the database.

        Args:
            student_id (str): The student ID to be removed.
        """
        del self.db_json[student_id]
        self.db_json['counter'] -= 1

    def get_student_id(self, student_id: str):
        """
        Retrieves the counter value for a given student ID.

        Args:
            student_id (str): The student ID for which to retrieve the counter value.

        Returns:
            int: The counter value associated with the specified student ID.
        """
        return self.db_json[student_id]

    def _init_db(self) -> dict:
        """
        Initializes the database by loading existing data or creating a new one.

        Returns:
            dict: The initialized database represented as a dictionary.
        """
        if os.path.exists(self.db_file_path):
            with open(self.db_file_path, 'r') as f:
                db_dict = json.load(f)

        else:
            if not os.path.exists(self.db_path):
                os.makedirs(self.db_path)
            db_dict = dict()
            db_dict['counter'] = 0
        return db_dict

    def _save_db(self):
        """
        Append the current state of the database to the JSON file.
        """
        with open(self.db_file_path, "a") as fp:
            json.dump(self.db_json, fp)

    def close(self):
        """Close the database"""
        self._save_db()
