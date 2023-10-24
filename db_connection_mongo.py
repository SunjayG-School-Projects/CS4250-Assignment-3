#-------------------------------------------------------------------------
# AUTHOR: Sunjay Guttikonda
# FILENAME: db_connection_mongo.py
# SPECIFICATION: Contains the functions for all the features used to create and edit databases in MongoDB (Compass)
# FOR: CS 4250- Assignment #2
# TIME SPENT: 45 minutes
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# Import the PyMongo library
import pymongo


# Create a database connection object using pymongo
def connectDataBase():
    try:
        # Replace 'your_connection_string' with your actual MongoDB connection string
        client = pymongo.MongoClient('mongodb://localhost:27017')
        database = client['corpus']
        return database
    except pymongo.errors.ConnectionFailure as e:
        print("Connection to MongoDB failed.")
        return None


# Create a document in the collection
def createDocument(col, docId, docText, docTitle, docDate, docCat):
    # Create a dictionary to count how many times each term appears in the document
    terms = {}
    for term in docText.split():
        term = term.lower()
        term = term.strip('.,!?()[]{}"\'')
        if term in terms:
            terms[term] += 1
        else:
            terms[term] = 1

    # Create a list of dictionaries to include term objects
    term_list = [{'term': term, 'count': count} for term, count in terms.items()]

    # Producing a final document as a dictionary including all the required document fields
    document = {
        'docId': docId,
        'docText': docText,
        'docTitle': docTitle,
        'docDate': docDate,
        'docCat': docCat,
        'terms': term_list
    }

    # Insert the document
    col.insert_one(document)


# Delete a document from the collection
def deleteDocument(col, docId):
    col.delete_one({'docId': docId})


# Update a document in the collection
def updateDocument(col, docId, docText, docTitle, docDate, docCat):
    # Delete the document
    col.delete_one({'docId': docId})

    # Create the document with the same id
    createDocument(col, docId, docText, docTitle, docDate, docCat)


# Get the index of terms and their counts
def getIndex(col):
    index = {}

    for doc in col.find():
        docText = doc['docText']
        docId = doc['docId']
        docTitle = doc['docTitle']

        for term_obj in doc['terms']:
            term = term_obj['term']
            count = term_obj['count']

            if term in index:
                index[term] += f',{docTitle}:{count}'
            else:
                index[term] = f'{docTitle}:{count}'

    return index


# Example usage:
if __name__ == "__main__":
    db = connectDataBase()
    if db is not None:
        col = db['your_collection_name']

        createDocument(col, 1, "Document text goes here.", "Document Title", "2023-10-01", "Category")
        updateDocument(col, 1, "Updated text.", "Updated Title", "2023-10-02", "Updated Category")
        deleteDocument(col, 1)

        index = getIndex(col)
        print(index)