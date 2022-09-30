# formula_one

Objectives:
- Demonstrate how to create actionable data from raw sources 
- Ensuring data is clean, accurate, formatted and ready to be used for analysis
- Demonstrate how to handle missing or incorrect data, and reformat it based on the requirements from the downstream analytics tool

- Demonstrate knowledge of building databases / DATAFRANES/SCHEMAS?
- Demonstrate knowledge of basic database building operations (using PK/FK, database constraints,  
- Implement STAR schema CHECK
- Implement database denormalisation, 121 relationships CHECK first and second normal forms IN WHAT CONTEXT
- Database transactions? coumn vs row stores

Python - Transforming data 
- Implement namespace LEGB rule (Local, Enclosing, Global, Built-in hierarchy used by interpeter to determine values of variables in python) 
- OOP ? create seperate functions with ACID properties?? CHECK

SQL - Analytics 
- Demonstrate working knowledge of SQL, using Data Manipulation Language (DML) and Data Definition Language (DDL) commands
- Perform left joins ETC?

Project aim :
- Separate original dataset into smaller datasets suitable for analysis

Project steps: 
- Downloaded formula 1 dataset from Kaggle website as csv file
- Read data from csv file data and converted data into list of dictionaries SO THAT IT IS IN A FORMAT THAT PSYCHOPG2 UNDERSTANDS AND CAN TRANSFER INTO THE POSTGRES TABLE? 
- Broke down data into smaller datasets
- Used psycopg2 to establish connection with postgres database ??? ('db_connection' file)
- THEN ?? through connection created tables and wrote into tables smaller recordsets USING SQL INSERT STATEMENTS?
DO I MENTION THAT I DO WEIRD FOREIGN KEY REFERNCING THING AND WHAT IS IT CALLED
SAME THING FOR COMPOUND PRIMARY KEY??
- DID SOME WEIRD SHIZ IN ANALYTICS?
- Carry out analytics on table data ('analytics' file), using TECHNIQUES? SUCH AS JOINS?
- WHAT IS DB INITIALISATION FILE FOR??

Data engineering best practice ????
- Implement controller file i.e. Python script that controls the operations of other Python scripts
- Including docstrings IS THIS STH TO EVEN MENTION
- Transforming data into different data structures

Improvements: 
- 

Tools used:
- Kaggle website dataset: Kaggle is an online community platform for data scientists with readily available datasets.
- Postgres: 
- Psychopg2: PYTHON??? package used to interact with Postgres from Python
- csv:
- uuid:
- pprint WHY???
