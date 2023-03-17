import psycopg2

#connect to potsgresql
conn = psycopg2.connect(
    host="localhost",
    database="final_project",
    user="postgres",
    password="12345678"
)

# open SQL file 
with open('serving_layer/create-serving-schema.sql', 'r') as f:
    sql_serving = f.read()

# run sql
cur  = conn.cursor()
cur.execute(sql_serving)
    
conn.commit()

# Menutup koneksi
cur.close()
conn.close()
# print("Create Table Success")

