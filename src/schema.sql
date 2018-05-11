begin;

create table datoms (
  e            integer not null,
  a            integer not null,
  v            blob    not null,
  t            integer not null,
  retracted_tx integer default null
);

create index e on datoms(e);

-- create index ea_unique on datoms(e, a);
-- create index ae_unique on datoms(a, e);
-- create index a_idx on datoms(a);
create unique index eav_retracted_unique on datoms(e, a, v, retracted_tx);

commit;
