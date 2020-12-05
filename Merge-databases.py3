import sqlite3
import os
import shutil

##Merge all the databases in a directory. 
## Databases must have the same schema and tables.
## Repeated data is ignored


## Unión de todas las bases de datos de un directorio. 
## Las bases de datos han de tener el mismo esquema y tablas.
## Se ignoran los datos repetidos

# lista de archivos sqliet(.db)
archivos = [ archivo for archivo in os.listdir() if archivo.endswith('.db')]
# nombre de la base de datos unida
base_datos='merge-databases.db'

# Si existe la base de datos inicial, se borra para empezar de nuevo
if base_datos in archivos:
    archivos.remove(base_datos)
    os.remove(base_datos)
nun_bases=len(archivos)    
shutil.copy2(archivos[0], base_datos)
archivos.pop(0)

mypath = os.getcwd()
conflictos=0

for archivo in archivos:
    conexion = sqlite3.connect(base_datos)
    cursorObject= conexion.cursor()
    cursorObject.execute("SELECT name FROM sqlite_master WHERE type='table';")
    all_tbls = cursorObject.fetchall()
    print(archivo)
    db=archivo
    cursorObject.execute("ATTACH ? AS curr_db;", (os.path.join(mypath, db),))

    for tbl in all_tbls:
        cursorObject.execute(f'select * from {tbl[0]}')
        datos_iniciales=len(cursorObject.fetchall())
        sql="select * from curr_db.[{}] EXCEPT select * from {}".format(tbl[0], tbl[0])
        cursorObject.execute(sql)
        datos_anadir = len(cursorObject.fetchall())
        cursorObject.execute(f'INSERT OR IGNORE INTO {tbl[0]} select * from curr_db.[{tbl[0]}] EXCEPT select * from {tbl[0]} ')
        cursorObject.execute(f'select * from {tbl[0]}')
        datos_totales=len(cursorObject.fetchall())
        datos_pendientes=datos_iniciales+datos_anadir-datos_totales
        conexion.commit()
        print(f'{tbl[0]}---Initial data={datos_iniciales} Data to be added {datos_anadir} Total data={datos_totales}  Pending data={datos_pendientes}')
        if datos_pendientes>0:conflictos=conflictos+datos_pendientes
    conexion.commit()
    conexion.close()

if conflictos>0:print(f'There have been {conflictos} conflicts with data')
print(f'The merging of {nun_bases} databases has been completed')
    
