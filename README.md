# formula_one

Objectives:
- Demonstrate how to create actionable data from raw sources. 
- Ensuring data is clean, accurate, formatted and ready to be used for analysis.
- Demonstrate how to handle missing or incorrect data, and reformat it based on the requirements from the downstream analytics tool.
- Demonstrate working Python knowledge (LEGB namespace rule, transforming data into different data structures).
- Demonstrate working knowledge of SQL, conducting queries using Data Manipulation Language (DML), Data Definition Language (DDL) commands.

Project outline: 
- Downloaded formula 1 dataset from Kaggle website as csv file.
- Read data in Python, wrangled the data. 
- Loaded into a Postgres database.
- Carried out analytics on table data ('analytics' file).

Tools used:
- Kaggle website dataset: Kaggle is an online community platform for data scientists with readily available datasets.
- PostgreSQL: PostgreSQL is an RDBMS (Relational Database Management System). It is one of the most popular RDMS on the market. Open source (free), it is backed by more than 30 years of community development which has contributed to its high levels of resilience, integrity and correctness. It supports both SQL (relational) and JSON (non-relational) querying. PostgreSQL is used as the primary data store or data warehouse for many web, mobile, geospatial, and analytics applications.  
- Psycopg2: a PostgreSQL database driver, it is used to interact with Postgres from Python.
