create function c2r as 'org.apache.flink.sql.examples.join.ColumnToRowFunction' LANGUAGE JAVA;

create table source
(
    id     int,
    params Array<String>
) with (
      'connector' = 'datagen',
      'rows-per-second' = '1',
      'fields.id.min' = '1',
      'fields.id.max' = '100',
      'fields.params.element.length' = '5'
      );

create table sink
(
    id   int,
    para String
) with (
      'connector' = 'print'
      );

insert into sink
select
    source.id,
    t.para
from
source,
lateral TABLE(c2r(params)) as t(para);