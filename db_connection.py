#-------------------------------------------------------------------------
# AUTHOR: your name
# FILENAME: title of the source file
# SPECIFICATION: description of the program
# FOR: CS 4250- Assignment #1
# TIME SPENT: how long it took you to complete the assignment
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
import string
import psycopg2
from psycopg2.extras import RealDictCursor

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
    #try:
        # Insert a category into the database
    cur.execute("INSERT INTO Categories (id, name) VALUES (%s, %s);", (id, name))

        # Commit the transaction to apply the changes
    cur.connection.commit()

    print("Category inserted successfully.")
    """except psycopg2.Error as e:
        # Rollback the transaction in case of an error
        cur.connection.rollback()
        print("Error: Unable to insert category.")
        print(e)
"""
def createDocument(cur, doc_number, text, title, date, category_ID):

    # 1 Get the category id based on the informed category name
    cur.execute("SELECT category_id FROM Categories WHERE id = %s;", (category_ID,))
    category_id = cur.fetchone()[0]

    # 2 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
    cur.execute("SELECT category_id FROM Categories WHERE id = %s;", (category_ID,))
    category_id = cur.fetchone()[0]

    # 3 Update the potential new terms.
    # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and Remember to lowercase terms and remove punctuation marks.
    # 3.2 For each term identified, check if the term already exists in the database
    # 3.3 In case the term does not exist, insert it into the database
    terms = [term.lower().strip(string.punctuation) for term in text.split()]
    unique_terms = set(terms)

    for term in unique_terms:
        # 3.2 Check if the term already exists in the database
        cur.execute("SELECT COUNT(*) FROM Terms WHERE term = %s;", (term,))
        term_count = cur.fetchone()[0]

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

def deleteDocument(cur, docId):

    # 1 Query the index based on the document to identify terms
    # 1.1 For each term identified, delete its occurrences in the index for that document
    # 1.2 Check if there are no more occurrences of the term in another document. If this happens, delete the term from the database.
    # --> add your Python code here

    # 2 Delete the document from the database
    # --> add your Python code here
    try:
        # 1. Query the index based on the document to identify terms
        cur.execute("SELECT text, count FROM Index WHERE doc_number = %s;", (docId,))
        term_counts = cur.fetchall()

        for term, count in term_counts:
            # 1.1 For each term identified, delete its occurrences in the index for that document
            cur.execute("DELETE FROM Index WHERE doc_number = %s AND text = %s;", (docId, term))

            # 1.2 Check if there are no more occurrences of the term in another document
            cur.execute("SELECT COUNT(*) FROM Index WHERE text = %s;", (term,))
            term_count = cur.fetchone()[0]

            if term_count == 0:
                # If no more occurrences of the term, delete the term from the database
                cur.execute("DELETE FROM Terms WHERE term = %s;", (term,))

        # 2. Delete the document from the database
        cur.execute("DELETE FROM Documents WHERE doc_number = %s;", (docId,))

        # Commit the transaction to apply the changes
        cur.connection.commit()

        print("Document deleted successfully.")
    except psycopg2.Error as e:
        # Rollback the transaction in case of an error
        cur.connection.rollback()
        print("Error: Unable to delete document.")
        print(e)

def updateDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Delete the document
    # --> add your Python code here

    # 2 Create the document with the same id
    # --> add your Python code here
    try:
        # 1. Delete the document
        cur.execute("DELETE FROM Documents WHERE doc_number = %s;", (docId,))

        # 2. Create the document with the same id
        # Discard spaces and punctuation marks to calculate num_chars
        num_chars = len(''.join([char for char in docText if char not in string.whitespace + string.punctuation]))
        cur.execute("""
                INSERT INTO Documents (doc_number, title, date, text, num_chars, category_ID)
                VALUES (%s, %s, %s, %s, %s, (SELECT category_ID FROM Categories WHERE id = %s));
            """, (docId, docTitle, docDate, docText, num_chars, docCat))

        # Commit the transaction to apply the changes
        cur.connection.commit()

        print("Document updated successfully.")
    except psycopg2.Error as e:
        # Rollback the transaction in case of an error
        cur.connection.rollback()
        print("Error: Unable to update document.")
        print(e)

def getIndex(cur):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # --> add your Python code here
    import psycopg2

    def getIndex(cur):
        try:
            # Query the database to return the documents where each term occurs with their corresponding count
            cur.execute("""
                SELECT text, term, count
                FROM Index JOIN Documents ON Index.doc_number = Documents.doc_number;
            """)
            rows = cur.fetchall()

            # Create a dictionary to store the terms and their corresponding documents and counts
            index_dict = {}

            for row in rows:
                text, term, count = row
                # If term is not already in the index_dict, add it
                if term not in index_dict:
                    index_dict[term] = f'{text}:{count}'
                else:
                    # If term is already in the index_dict, append the new document and count
                    index_dict[term] += f',{text}:{count}'

            return index_dict
        except psycopg2.Error as e:
            print("Error: Unable to get index.")
            print(e)
            return None
