--CREACION DE DATABASE LINKS
    --En el servidor A:
CREATE DATABASE LINK db_link_b
CONNECT TO system IDENTIFIED BY oracle
USING '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=172.21.0.3)(PORT=1521))(CONNECT_DATA=(SID=XE)))';

    --En el servidor B:
CREATE DATABASE LINK db_link_b
CONNECT TO system IDENTIFIED BY oracle
USING '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=172.21.0.2)(PORT=1521))(CONNECT_DATA=(SID=XE)))';


--FRAGMENTAR
    --En el servidor A:
CREATE TABLE sucursal_A AS 
SELECT * FROM sucursal WHERE region = 'A';

CREATE TABLE prestamo_A AS 
SELECT * FROM prestamo WHERE idsucursal IN (SELECT idsucursal FROM sucursal_A);

    --En el servidor B:
CREATE TABLE sucursal_B AS 
SELECT * FROM sucursal WHERE region = 'B';

CREATE TABLE prestamo_B AS 
SELECT * FROM prestamo WHERE idsucursal IN (SELECT idsucursal FROM sucursal_B);


--VISTA GLOBAL DE LA TABLA SUCURSALES:
CREATE VIEW sucursal_global AS 
SELECT * FROM sucursal_A 
UNION 
SELECT * FROM sucursal_B@db_link;

--VISTA GLOBAL DE LA TABLA PRESTAMOS:
CREATE VIEW prestamo_global AS 
SELECT * FROM prestamo_A 
UNION 
SELECT * FROM prestamo_B@db_link;


--SINONIMOS
CREATE SYNONYM sucursal2 FOR sucursal_B@db_link;

CREATE SYNONYM sucursal2 FOR sucursal_A@db_link;

CREATE SYNONYM prestamo2 FOR prestamo_B@db_link;

CREATE SYNONYM prestamo2 FOR prestamo_A@db_link;

--PROCEDIMIENTOS ALMACENADOS PARA DAR DE ALTA SUCURSALES 
CREATE OR REPLACE PROCEDURE alta_sucursal (
    p_idsucursal IN VARCHAR2,
    p_nombresucursal IN VARCHAR2,
    p_ciudadsucursal IN VARCHAR2,
    p_activos IN NUMBER,
    p_region IN VARCHAR2
) AS
BEGIN
    IF p_region = 'A' THEN
        INSERT INTO sucursal_A (idsucursal, nombresucursal, ciudadsucursal, activos, region)
        VALUES (p_idsucursal, p_nombresucursal, p_ciudadsucursal, p_activos, p_region);
    ELSE
        INSERT INTO sucursal2 (idsucursal, nombresucursal, ciudadsucursal, activos, region)
        VALUES (p_idsucursal, p_nombresucursal, p_ciudadsucursal, p_activos, p_region);
    END IF;
END;
/

    --Pruebas
BEGIN
    alta_sucursal('S0010', 'NewTown', 'Brooklyn', 500000, 'A');
END;
/

BEGIN
    alta_sucursal('S0011', 'OldTown', 'Bennington', 300000, 'B');
END;
/

--PROCEDIMINETOS ALMACENADOS PARA DAR DE ALTA PRESTAMOS
CREATE OR REPLACE PROCEDURE alta_prestamo (
    p_noprestamo IN VARCHAR2,
    p_idsucursal IN VARCHAR2,
    p_cantidad IN NUMBER
) AS
    v_region VARCHAR2(2);
BEGIN
    SELECT region INTO v_region
    FROM sucursal
    WHERE idsucursal = p_idsucursal;

    IF v_region = 'A' THEN
        INSERT INTO prestamo_A (noprestamo, idsucursal, cantidad)
        VALUES (p_noprestamo, p_idsucursal, p_cantidad);
    ELSE
        INSERT INTO prestamo2 (noprestamo, idsucursal, cantidad)
        VALUES (p_noprestamo, p_idsucursal, p_cantidad);
    END IF;
END;
/

    --Pruebas
BEGIN
    alta_prestamo('L-25', 'S0002', 5000);
END;
/

BEGIN
    alta_prestamo('L-26', 'S0008', 7500);
END;
/

--TRIGGER
    --En el servidor A:
CREATE OR REPLACE TRIGGER replicacion_sucursal_A
AFTER INSERT OR UPDATE OR DELETE ON sucursal_A
FOR EACH ROW
BEGIN
  IF INSERTING THEN
    INSERT INTO sucursal@db_link (idsucursal, nombresucursal, ciudadsucursal, activos, region)
    VALUES (:NEW.idsucursal, :NEW.nombresucursal, :NEW.ciudadsucursal, :NEW.activos, :NEW.region);
  
  ELSIF UPDATING THEN
    UPDATE sucursal@db_link
    SET nombresucursal = :NEW.nombresucursal,
        ciudadsucursal = :NEW.ciudadsucursal,
        activos = :NEW.activos,
        region = :NEW.region
    WHERE idsucursal = :OLD.idsucursal;

  ELSIF DELETING THEN
    DELETE FROM sucursal@db_link
    WHERE idsucursal = :OLD.idsucursal;
  END IF;
END;

    --Prueba
INSERT INTO sucursal_A (idsucursal, nombresucursal, ciudadsucursal, activos, region)
VALUES ('S0015', 'Central', 'Horseneck', 100000, 'A');

    --En el servidor B:
CREATE OR REPLACE TRIGGER replicacion_sucursal_B
AFTER INSERT OR UPDATE OR DELETE ON sucursal_B
FOR EACH ROW
BEGIN
  IF INSERTING THEN
    INSERT INTO sucursal@db_link (idsucursal, nombresucursal, ciudadsucursal, activos, region)
    VALUES (:NEW.idsucursal, :NEW.nombresucursal, :NEW.ciudadsucursal, :NEW.activos, :NEW.region);
  
  ELSIF UPDATING THEN
    UPDATE sucursal@db_link
    SET nombresucursal = :NEW.nombresucursal,
        ciudadsucursal = :NEW.ciudadsucursal,
        activos = :NEW.activos,
        region = :NEW.region
    WHERE idsucursal = :OLD.idsucursal;

  ELSIF DELETING THEN
    DELETE FROM sucursal@db_link
    WHERE idsucursal = :OLD.idsucursal;
  END IF;
END;


--VISTA MATERIALIZADA DE LA TABLA GLOBAL DE SUCURSALES (SERVIDOR A)
CREATE MATERIALIZED VIEW sucursal_global_mat
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
AS
SELECT idsucursal, nombresucursal, ciudadsucursal, activos, region
FROM sucursal_A
UNION
SELECT idsucursal, nombresucursal, ciudadsucursal, activos, region
FROM sucursal2;

SELECT * FROM sucursal_global_mat;

--VISTA MATERIALIZADA DE LA TABLA GLOBAL DE PRESTAMOS (SERVIDOR B)
CREATE MATERIALIZED VIEW prestamo_global_mat
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
AS
SELECT noprestamo, idsucursal, cantidad
FROM prestamo_B
UNION
SELECT noprestamo, idsucursal, cantidad
FROM prestamo2;

--VISTA PARA OBTENER CANTIDAD TOTAL DE PRESTAMOS POR SUCURSAL (SERVIDOR A)
CREATE VIEW prestamos_sucursal AS
SELECT idsucursal, SUM(cantidad) AS total_prestamos
FROM prestamo_A
GROUP BY idsucursal;

