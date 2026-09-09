create table source
(
    id     int,
    params Array<String>
) with (
      'connector' = 'datagen',
      'rows-per-second' = '1',
      'fields.id.min' = '1',
      'fields.id.max' = '100',
      'fields.params.element.length' = '5',
      'fields.params.length' = '5'
    );

create table sink
(
    id   int,
    para Array<String>
) with (
      'connector' = 'print'
      );

insert into sink
select id, params
    from source;

-- insert into sink
-- select source.id, t.para
-- from source
-- CROSS JOIN UNNEST(params) AS t (para);