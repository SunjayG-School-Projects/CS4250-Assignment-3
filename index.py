#-------------------------------------------------------------------------
# AUTHOR: your name
# FILENAME: title of the source file
# SPECIFICATION: description of the program
# FOR: CS 4250- Assignment #2
# TIME SPENT: how long it took you to complete the assignment
#-----------------------------------------------------------*/
import string

#importing some Python libraries
from db_connection import *

if __name__ == '__main__':

    # Connecting to the database
    conn = connectDataBase()

    # Getting a cursor
    cur = conn.cursor()

    #print a menu
    print("")
    print("######### Menu ##############")
    print("#a - Create a category.")
    print("#b - Create a document")
    print("#c - Update a document")
    print("#d - Delete a document.")
    print("#e - Output the inverted index.")
    print("#q - Quit")

    option = ""
    while option != "q":

          print("")
          option = input("Enter a menu choice: ")

          if (option == "a"):

              id = input("Enter the ID of the category: ")
              name = input("Enter the name of the category: ")

              createCategory(cur, id, name)
              conn.commit()

          elif (option == "b"):

              doc_number = input("Enter the ID of the document: ")
              text = input("Enter the text of the document: ")
              title = input("Enter the title of the document: ")
              date = input("Enter the date of the document: ")
              category_name = input("Enter the category of the document: ")

              num_chars = len(text)

              createDocument(cur, doc_number, text, title, date, num_chars, category_name)
              conn.commit()

          elif (option == "c"):

              doc_number = input("Enter the ID of the document: ")
              text = input("Enter the text of the document: ")
              title = input("Enter the title of the document: ")
              date = input("Enter the date of the document: ")
              category_id = input("Enter the category of the document: ")

              updateDocument(cur, doc_number, text, title, date, category_id)

              conn.commit()

          elif (option == "d"):

              doc_number = input("Enter the document id to be deleted: ")
              deleteDocument(cur, doc_number)
              conn.commit()

          elif (option == "e"):
              index = getIndex(cur)
              print(index)

          elif (option == "q"):

               print("Leaving the application ... ")

          else:

               print("Invalid Choice.")