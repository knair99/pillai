import psycopg2

# Connect to CockroachDB
connection = psycopg2.connect(
    host="localhost",
    port="26257",
    user="root",
    password="",
    database="defaultdb"
)

cursor = connection.cursor()

# Example query to create a table (adjust as needed)
cursor.execute("CREATE TABLE IF NOT EXISTS malware_detection (id UUID PRIMARY KEY, script TEXT, result TEXT);")

connection.commit()
cursor.close()
connection.close()
