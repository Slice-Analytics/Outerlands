import snowflake.connector

#create connection
conn = snowflake.connector.connect(
    user="suser",
    password="pass@123",
    account="abc123.us-east-2",
    warehouse="demo_wh",
    database="demo",
    schema="public"
)
#create cursor
curs = conn.cursor()

#execute SQL statement
curs.execute("select current date")

#fetch result
print(curs.fetchone()[0])
