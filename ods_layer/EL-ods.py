import psycopg2

#connect to potsgresql
conn = psycopg2.connect(
    host="localhost",
    database="final_project",
    user="postgres",
    password="12345678"
)

# open SQL file 
with open('ods_layer/EL-ods.sql', 'r') as f:
    sql_ods = f.read()

# run sql
cur  = conn.cursor()
cur.execute(sql_ods)
    
conn.commit()

# Menutup koneksi
cur.close()
conn.close()
# print("Create Table Success")

