begin;

create table datoms (
  e            integer not null,
  a            integer not null,
  v            blob    not null,
  t            integer not null,
  retracted_tx integer default null
);

create unique index eav_retracted_unique on datoms(e, a, v, retracted_tx);

commit;
