
# -------------------------------------------------------------------------
# AUTHOR: your name
# FILENAME: title of the source file
# SPECIFICATION: description of the program
# FOR: CS 4250- Assignment #1
# TIME SPENT: how long it took you to complete the assignment
# -----------------------------------------------------------*/

# IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

# importing some Python libraries
# --> add your Python code here
import string
import psycopg2
from psycopg2.extras import RealDictCursor
import psycopg2

def connectDataBase():

    # Create a database connection object using psycopg2
    DB_NAME = "CS4250-Assignment-2"
    DB_USER = "postgres"
    DB_PASS = "123"
    DB_HOST = "localhost"
    DB_PORT = "5432"
    try:
        conn = psycopg2.connect(database=DB_NAME,
                                user=DB_USER,
                                password=DB_PASS,
                                host=DB_HOST,
                                port=DB_PORT,
                                cursor_factory=RealDictCursor)
        return conn

    except:
        print("Database not connected successfully")

def createCategory(cur, id, name):
    # try:
    # Insert a category into the database
    # cur.execute("INSERT INTO Categories (id, name) VALUES (%s, %s);", (id, name))

    # Commit the transaction to apply the changes
    # cur.connection.commit()
    sql = "Insert into Categories (id, name) Values (%s, %s)"
    recset = [id, name]
    cur.execute(sql, recset)
    print("Category inserted successfully.")
    """except psycopg2.Error as e:
        # Rollback the transaction in case of an error
        cur.connection.rollback()
        print("Error: Unable to insert category.")
        print(e)
"""

def createDocument(cur, doc_number, text, title, date, num_chars, category_name):

    # 1 Get the category id based on the informed category name
    cur.execute("SELECT id FROM categories WHERE name = %s;", (category_name,))
    category_id = cur.fetchone()["id"]
    print("|", category_id, "|")
    # 2 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
    cur.execute("INSERT INTO documents VALUES (%s, %s, %s, %s, %s, %s);",
                (doc_number, title, date, text, num_chars, category_id))
    # category_id = cur.fetchone()[0]

    # 3 Update the potential new terms.
    # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and Remember to lowercase terms and remove punctuation marks.
    # 3.2 For each term identified, check if the term already exists in the database
    # 3.3 In case the term does not exist, insert it into the database
    terms = [term.lower().strip(string.punctuation) for term in text.split()]
    unique_terms = set(terms)

    for term in unique_terms:
        # 3.2 Check if the term already exists in the database
        cur.execute("SELECT COUNT(*) FROM Terms WHERE term = %s;", (term,))
        # print("|", cur.fetchone(), "|")
        term_count = cur.fetchone()["count"]

        if term_count == 0:
            # 3.3 In case the term does not exist, insert it into the database
            cur.execute("INSERT INTO Terms (term, num_chars) VALUES (%s, %s);", (term, len(term)))

        # 4 Update the index
        # 4.1 Find all terms that belong to the document
        # 4.2 Create a data structure the stores how many times (count) each term appears in the document
        # 4.3 Insert the term and its corresponding count into the database
    term_counts = {term: terms.count(term) for term in unique_terms}

    for term, count in term_counts.items():
        # 4.3 Insert the term and its corresponding count into the database
        cur.execute("""
            INSERT INTO Index (count, doc_number, text)
            VALUES (%s, %s, %s);
        """, (count, doc_number, term))

    # Commit the transaction to apply the changes
    cur.connection.commit()

    print("Document inserted successfully.")
"""except psycopg2.Error as e:
    # Rollback the transaction in case of an error
    cur.connection.rollback()
    print("Error: Unable to insert document.")
    print(e)"""

def deleteDocument(cur, doc_number):

    # 1 Query the index based on the document to identify terms
    # 1.1 For each term identified, delete its occurrences in the index for that document
    # 1.2 Check if there are no more occurrences of the term in another document. If this happens, delete the term from the database.
    # --> add your Python code here

    # 2 Delete the document from the database
    # --> add your Python code here
    try:
        # 1. Query the index based on the document to identify terms
        cur.execute("SELECT text, count FROM index WHERE doc_number = %s;", (doc_number,))
        term_counts = cur.fetchall()

        for term, count in term_counts:
            # 1.1 For each term identified, delete its occurrences in the index for that document
            cur.execute("DELETE FROM index WHERE doc_number = %s AND text = %s;", (doc_number, text))

            # 1.2 Check if there are no more occurrences of the term in another document
            cur.execute("SELECT COUNT(*) FROM index WHERE text = %s;", (text,))
            term_count = cur.fetchone()[0]

            if term_count == 0:
                # If no more occurrences of the term, delete the term from the database
                cur.execute("DELETE FROM terms WHERE term = %s;", (text,))

        # 2. Delete the document from the database
        cur.execute("DELETE FROM documents WHERE doc_number = %s;", (doc_number,))
        cur.execute("DELETE FROM index WHERE doc_number = %s;", (doc_number,))

        # Commit the transaction to apply the changes
        cur.connection.commit()

        print("Document deleted successfully.")
    except psycopg2.Error as e:
        # Rollback the transaction in case of an error
        cur.connection.rollback()
        print("Error: Unable to delete document.")
        print(e)

def updateDocument(cur, doc_number, text, title, date, category_id):

    # 1 Delete the document
    # --> add your Python code here

    # 2 Create the document with the same id
    # --> add your Python code here
    try:
        # 1. Delete the document
        cur.execute("DELETE FROM documents WHERE doc_number = %s;", (doc_number,))

        # 2. Create the document with the same id
        # Discard spaces and punctuation marks to calculate num_chars
        num_chars = len(''.join([char for char in text if char not in string.whitespace + string.punctuation]))
        cur.execute("""
                INSERT INTO Documents (doc_number, title, date, text, num_chars, category_id)
                VALUES (%s, %s, %s, %s, %s, (SELECT category_id FROM Categories WHERE id = %s));
            """, (doc_number, title, date, text, num_chars, category_id))
        # Commit the transaction to apply the changes
        cur.connection.commit()

        print("Document updated successfully.")
    except psycopg2.Error as e:
        # Rollback the transaction in case of an error
        cur.connection.rollback()
        print("Error: Unable to update document.")
        print(e)


def getIndex(cur):
    try:
        # Query the database to return the documents where each term occurs with their corresponding count
        cur.execute("""
                SELECT documents.title, index.text, index.count
                FROM Index JOIN Documents ON Index.doc_number = Documents.doc_number;
            """)
        rows = cur.fetchall()

        # Create a dictionary to store the terms and their corresponding documents and counts
        index_dict = {}

        for row in rows:
            print("|" + str(row) + "|")
            #title, text, count = row
            title = row['title']
            text = row['text']
            count = row['count']
            print("|" + title + "|" + text + "|" + str(count) + "|")
            # If term is not already in the index_dict, add it
            if text not in index_dict:
                index_dict[row['text']] = f"{row['title']}:{row['count']}"
                print("not in: " + str(index_dict))
            else:
                # If term is already in the index_dict, append the new document and count
                index_dict[row["text"]] += f",{row['title']}:{row['count']}"
                print("in: " + str(index_dict))

        return index_dict
    except psycopg2.Error as e:
        print("Error: Unable to get index.")
        print(e)
        return None

