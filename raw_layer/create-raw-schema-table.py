import psycopg2

#connect to potsgresql
conn = psycopg2.connect(
    host="localhost",
    database="final_project",
    user="postgres",
    password="12345678"
)

# open SQL file 
with open('raw_layer/create-raw-schema.sql', 'r') as f:
    sql_raw = f.read()

# run sql
cur  = conn.cursor()
cur.execute(sql_raw)
    
conn.commit()

# Menutup koneksi
cur.close()
conn.close()
# print("Create Table Success")

