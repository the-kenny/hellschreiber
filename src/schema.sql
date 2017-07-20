begin;

create table datoms (
  e         integer not null,
  a         integer not null,
  v         blob    not null,
  t         integer not null,
  retracted integer not null
);

commit;
