from string import Template

encabezado = Template("""
SET SERVEROUTPUT ON
SET DEFINE OFF

DECLARE
    v_registros_insertados  NUMBER (30);
    v_numero_esperado       NUMBER (30);
    REG_ACT_EXCEPTION       EXCEPTION;

BEGIN
--!!!ESTE VALOR SE DEBE ACTUALIZAR ACORDE A LA CANTIDAD MAXIMA DE REGISTROS QUE SE ESPERAN INSERTAR!!!
v_numero_esperado       := $numero_esperado; --Cantidad de Inserts en la base de datos
v_registros_insertados := 0;
DBMS_OUTPUT.put_line ('***COMIENZA SCRIPT***');
DBMS_OUTPUT.put_line ('Se esperan insertar '||v_numero_esperado ||' registros');

--BEGIN INSERT
""")

insert = Template("""
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
    $id_circunscripcion, --Id de tabla Circunscripcion
    $numero, --mismo Id de la tabla literal
    '$nombre', ----Nombre de la Delegación
    $fk_tipo_registro, -- Tipo de Registro (2 Defunciones, 8 Nacimientos, 4 Matrimonios, 1 Certificaciones, 3 Digitalizacion de Nacimientos, 6 Digitalizacion de Defunciones, 5 Digitalizacion de Matrimonios)
    '$codigo_delegacion_renaper', -- Código de Delegación Renaper debe venir indicado en el GLPI
    NULL, --Null en todos los casos
    $fk_localidad, --Id de localidad
    NULL,
    NULL
);

v_registros_insertados := v_registros_insertados + SQL%ROWCOUNT;
""")

insert_kodak = Template("""
    INSERT INTO RCE_GED.SYS_KODAK_CIRCUNSCRIPCION (ID_KODAK_CIRCUNSCRIPCION,FK_TIPO_REGISTRO,NOMBRE,FK_CIRCUNSCRIPCION) VALUES ($id_kodak_circunscripcion,$fk_tipo_registro,'$nombre',$fk_circunscripcion);
    v_registros_insertados := v_registros_insertados + SQL%ROWCOUNT;
""")

final = Template("""
--FIN INSERT

IF (v_registros_insertados != v_numero_esperado) THEN
    RAISE REG_ACT_EXCEPTION;
END IF;

COMMIT; --!!!PARA EJECUTAR PASAR A COMMIT!!!
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
END;""")