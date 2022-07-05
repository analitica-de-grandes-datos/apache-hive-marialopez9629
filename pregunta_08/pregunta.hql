/*

Pregunta
===========================================================================

Escriba una consulta que para cada valor único de la columna `t0.c2,` 
calcule la suma de todos los valores asociados a las claves en la columna 
`t0.c6`.

Apache Hive se ejecutará en modo local (sin HDFS).

Escriba el resultado a la carpeta `output` de directorio de trabajo.

*/

DROP TABLE IF EXISTS tbl0;
CREATE TABLE tbl0 (
    c1 INT,
    c2 STRING,
    c3 INT,
    c4 DATE,
    c5 ARRAY<CHAR(1)>, 
    c6 MAP<STRING, INT>
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY ':'
MAP KEYS TERMINATED BY '#'
LINES TERMINATED BY '\n';
LOAD DATA LOCAL INPATH 'data0.csv' INTO TABLE tbl0;

DROP TABLE IF EXISTS tbl1;
CREATE TABLE tbl1 (
    c1 INT,
    c2 INT,
    c3 STRING,
    c4 MAP<STRING, INT>
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY ':'
MAP KEYS TERMINATED BY '#'
LINES TERMINATED BY '\n';
LOAD DATA LOCAL INPATH 'data1.csv' INTO TABLE tbl1;

/*
    >>> Escriba su respuesta a partir de este punto <<<
*/

DROP TABLE IF EXISTS tbl0;
CREATE TABLE tbl0 (
    c1 INT,
    c2 STRING,
    c3 INT,
    c4 DATE,
    c5 ARRAY<CHAR(1)>,
    c6 MAP<STRING, INT>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY ':'
MAP KEYS TERMINATED BY '#'
LINES TERMINATED BY '\n';
LOAD DATA LOCAL INPATH 'data0.csv' INTO TABLE tbl0;


--INSERT OVERWRITE LOCAL DIRECTORY 'output'
--ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
--COLLECTION ITEMS TERMINATED BY ':'
SELECT c2,collect_set(c6)
FROM tbl0
GROUP BY c2;

DROP TABLE IF EXISTS tablita;
CREATE TABLE tablita
AS
   SELECT c1,nuevo1,nuevo2 FROM tbl0
LATERAL VIEW explode(c6) tbl0 AS nuevo1,nuevo2;

DROP TABLE IF EXISTS tablita2;
CREATE TABLE tablita2
AS
   SELECT tbl0.c1,tbl0.c2,tablita.nuevo2 FROM tbl0
JOIN tablita ON tbl0.c1=tablita.c1;

INSERT OVERWRITE LOCAL DIRECTORY 'output'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT c2,sum(nuevo2)
FROM tablita2
GROUP BY c2;

