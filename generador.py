import os
import pandas as pd
from tabulate import tabulate
import sqlalchemy
from config import CON_URL

desktop = os.path.expanduser("~/Desktop")


def escribir_archivo(nom_archivo, seccion, *args):
    if seccion == 'encabezado':
        with open(nom_archivo, 'w+') as encabezado:
            encabezado.write(f"""
SET SERVEROUTPUT ON
SET DEFINE OFF

/*
MODIFICACION DE LA DELEGACION CIRCUNSCRIPCION
TABLA RCE_GED.SYS_CIRCUNSCRIPCION
*/

DECLARE
    v_registros_modificados NUMBER (30);
    v_numero_esperado       NUMBER (30);
    REG_ACT_EXCEPTION       EXCEPTION;

BEGIN
--!!!ESTE VALOR SE DEBE ACTUALIZAR ACORDE A LA CANTIDAD MAXIMA DE REGISTROS QUE SE ESPERAN INSERTAR!!!
v_numero_esperado       := {args[0]}; --Cantidad de Inserts en la base de datos
v_registros_modificados := 0;
DBMS_OUTPUT.put_line ('***COMIENZA SCRIPT***');
DBMS_OUTPUT.put_line ('Se esperan insertar '||v_numero_esperado ||' registros');

--BEGIN INSERT

""")


    if seccion == 'final':
        with open(nom_archivo, 'a') as final:
            final.write("""

--FIN INSERT

IF (v_registros_modificados != v_numero_esperado) THEN
    RAISE REG_ACT_EXCEPTION;
END IF;

ROLLBACK; --!!!PARA EJECUTAR PASAR A COMMIT!!!
DBMS_OUTPUT.put_line ('Se insertaron '||v_registros_modificados||' registros');
DBMS_OUTPUT.put_line ('***COMMIT REALIZADO***');

EXCEPTION
WHEN REG_ACT_EXCEPTION THEN
    BEGIN
    ROLLBACK;
    DBMS_OUTPUT.put_line ('SE REALIZA ROLLBACK DE TRANSACCION: ');
    DBMS_OUTPUT.put_line ('LA CANTIDAD DE REGISTROS INSERTADOS NO COINCIDE CON LA ESPERADA ' || v_registros_modificados);
END;

WHEN OTHERS THEN
    BEGIN
    ROLLBACK;
    DBMS_OUTPUT.put_line ('SE REALIZA ROLLBACK DE TRANSACCION: ');
    DBMS_OUTPUT.put_line ('    ' || SUBSTR (SQLERRM,1, 200));
END;

END;
""")


    if seccion == 'insert':
        with open(nom_archivo, 'a') as insert:
            insert.write(f"""
INSERT
INTO RCE_GED.SYS_CIRCUNSCRIPCION
(
    ID_CIRCUNSCRIPCION,
    NUMERO,
    NOMBRE,
    FK_TIPO_REGISTRO,
    CODIGO_DELEGACION_RENAPER,
    SECCION_KODAK,
    FK_LOCALIDAD,
    FK_REPARTICION,
    FK_LOCALIDAD_CONSULADOS
)
VALUES
(
    {args[0]}, --Id de tabla Circunscripcion
    '{args[0]}', --mismo Id de la tabla literal
    '{args[1]}', ----Nombre de la Delegación
    {args[2]}, -- Tipo de Registro (2 Defunciones, 8 Nacimientos, 4 Matrimonios, 1 Certificaciones, 3 Digitalizacion de Nacimientos, 6 Digitalizacion de Defunciones, 5 Digitalizacion de Matrimonios)
    '{args[3]}', -- Código de Delegación Renaper debe venir indicado en el GLPI
    NULL, --Null en todos los casos
    {args[4]}, --Id de localidad
    NULL,
    NULL
);

DBMS_OUTPUT.put_line ('Registros insertados: ' || SQL%%ROWCOUNT);
v_registros_modificados := v_registros_modificados + SQL%%ROWCOUNT;

""")

    if seccion == 'encabezado_kodak':
        with open(nom_archivo, 'w+') as encabezado:
            encabezado.write(f"""
SET SERVEROUTPUT ON
SET DEFINE OFF

/*
SE AGREGAN DELEGACIONES A KODAK CIRCUNSCRIPCION
TABLA RCE_GED.SYS_KODAK_CIRCUNSCRIPCION
*/

DECLARE
    v_registros_insertados  NUMBER (30);
    v_numero_esperado       NUMBER (30);
    v_id_kodak              NUMBER (30);
    REG_ACT_EXCEPTION       EXCEPTION;

BEGIN
    --!!!ESTE VALOR SE DEBE ACTUALIZAR ACORDE A LA CANTIDAD MAXIMA DE REGISTROS QUE SE ESPERAN INSERTAR!!!
    v_numero_esperado       := {args[0]}; --Cantidad de Inserts en la base de datos
    v_registros_insertados  := 0;
    v_id_kodak              := {args[1]};
    DBMS_OUTPUT.put_line ('***COMIENZA SCRIPT***');
    DBMS_OUTPUT.put_line ('Se esperan insertar '||v_numero_esperado ||' registros');

    --BEGIN INSERT

""")


    if seccion == 'final_kodak':
        with open(nom_archivo, 'a') as final:
            final.write("""
--FIN INSERT

IF (v_registros_insertados != v_numero_esperado) THEN
    RAISE REG_ACT_EXCEPTION;
END IF;

ROLLBACK; --!!!PARA EJECUTAR PASAR A COMMIT!!!
DBMS_OUTPUT.put_line ('Se insertaron '||v_registros_insertados||' registros');
DBMS_OUTPUT.put_line ('***COMMIT REALIZADO***');

EXCEPTION
WHEN REG_ACT_EXCEPTION THEN
    BEGIN
    ROLLBACK;
    DBMS_OUTPUT.put_line ('SE REALIZA ROLLBACK DE TRANSACCION: ');
    DBMS_OUTPUT.put_line ('LA CANTIDAD DE REGISTROS INSERTADOS NO COINCIDE CON LA ESPERADA ' || v_registros_insertados);
END;

WHEN OTHERS THEN
    BEGIN
    ROLLBACK;
    DBMS_OUTPUT.put_line ('SE REALIZA ROLLBACK DE TRANSACCION: ');
    DBMS_OUTPUT.put_line ('    ' || SUBSTR (SQLERRM,1, 200));
END;

END;
""")


    if seccion == 'insert_kodak':
        with open(nom_archivo, 'a') as insert:
            insert.write(f"""
    INSERT INTO RCE_GED.SYS_KODAK_CIRCUNSCRIPCION (ID_KODAK_CIRCUNSCRIPCION,FK_TIPO_REGISTRO,NOMBRE,FK_CIRCUNSCRIPCION) VALUES (v_id_kodak,{args[0]},'{args[1]}',{args[2]});
    v_id_kodak := v_id_kodak + 1;
    DBMS_OUTPUT.put_line ('Registros insertados: ' || SQL%%ROWCOUNT);
    v_registros_insertados := v_registros_insertados + SQL%%ROWCOUNT;

""")


def crear_scripts_sql(df):
    flag_encabezado_kodak = 1
    cant1 = 0
    cant2 = 0
    nombre_archivo_d = desktop+'/PROD - 1 - INSERT DELEGACIONES.sql'

    engine = sqlalchemy.create_engine(CON_URL)
    with engine.connect() as conn:
        print("Obteniendo último ID de delegación...")
        
        query = """SELECT ID_CIRCUNSCRIPCION FROM 
            RCE_GED.SYS_CIRCUNSCRIPCION ORDER BY ID_CIRCUNSCRIPCION DESC
            fetch first 1 rows only
            """        
        data = pd.read_sql(query, conn)
        id_cir = data.iloc[0][0]

        print("Obteniendo último ID KODAK...")
        query = """
            SELECT ID_KODAK_CIRCUNSCRIPCION FROM RCE_GED.SYS_KODAK_CIRCUNSCRIPCION ORDER BY ID_KODAK_CIRCUNSCRIPCION DESC
            fetch first 1 rows only
            """

        data = pd.read_sql(query, conn)
        k_ini = data.iloc[0][0]
        k_ini += 1
        k_ini = str(k_ini)
        print("OK")

        for _, row in df.iterrows():
            if row['TIPO REGISTRO'] in (3, 5, 6):
                cant2 += 1
            cant1 += 1

        escribir_archivo(nombre_archivo_d, 'encabezado', cant1)

        for _, row in df.iterrows():
            id_cir += 1
            id_cir_str = str(id_cir)

            nom = row['NOMBRE DELEGACION']
            nom = str(nom).strip()

            reg = row['TIPO REGISTRO']
            reg = str(reg).strip()

            renaper = row['COD RENAPER']
            renaper = str(renaper).strip()

            nom_loc = row['LOCALIDAD']
            nom_loc = str(nom_loc).strip()
            nom_loc = nom_loc.upper()

            nom_dep = row['PARTIDO']
            nom_dep = str(nom_dep).strip()
            nom_dep = nom_dep.upper()

            try:
                print(f"Obteniendo ID de localidad {nom_loc}...")
                query = f"""SELECT LOC.ID
                    FROM EU_GED.EU_LOCALIDAD  LOC
                    INNER JOIN EU_GED.EU_DEPARTAMENTO DEP ON LOC.ID_DEPARTAMENTO = DEP.ID
                    WHERE LOC.NOMBRE = '{nom_loc}' AND DEP.NOMBRE = '{nom_dep}' AND LOC.ID_PROVINCIA = 97
                    """
                data = pd.read_sql(query, conn)
                res = data.iloc[0][0]
                loc = str(res)

            except:
                print("Error al obtener ID")
                loc = 'xxxxx'

            escribir_archivo(nombre_archivo_d, 'insert', id_cir_str, nom, reg, renaper, loc)

            if int(reg) in (3, 5, 6):
                if flag_encabezado_kodak == 1:
                    flag_encabezado_kodak = 0
                    nombre_archivo_k = desktop+'/PROD - 2 - INSERT DELEGACIONES KODAK.sql'
                    escribir_archivo(nombre_archivo_k, 'encabezado_kodak', cant2, k_ini)

                escribir_archivo(nombre_archivo_k, 'insert_kodak', reg, renaper, id_cir_str)

        escribir_archivo(nombre_archivo_d, 'final')
        print(f"{nombre_archivo_d} generado correctamente")

        if flag_encabezado_kodak == 0:
            escribir_archivo(nombre_archivo_k, 'final_kodak')
            print(f"{nombre_archivo_k} generado correctamente")



if __name__ == "__main__":
    
    df = pd.read_excel('delegaciones.xlsx')
    pdtabulate=lambda df:tabulate(df,headers='keys',tablefmt='psql')
    print(pdtabulate(df))
    crear_scripts_sql(df)

