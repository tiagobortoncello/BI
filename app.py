import sqlite3

conn = sqlite3.connect('almg_local.db')
cursor = conn.cursor()
cursor.execute("PRAGMA table_info(fat_autoria_proposicao);")
colunas = cursor.fetchall()
print("Colunas reais de fat_autoria_proposicao:")
for col in colunas:
    print(f"- {col[1]}")
conn.close()
