begin;

create table datoms (
  e            integer not null check(e >= 0),
  a            integer not null check(a >= 0),
  v            blob    not null,
  t            integer not null check(t >= 0),
  retracted_tx integer default null
);

create index eavt on datoms(e, a, v, t);
create index aevt on datoms(a, e, v, t);
create index avet on datoms(a, v, e, t);
create index vaet on datoms(v, a, e, t);

-- `e` should reference datoms.e but can't as datoms.e isn't the
-- primary key of the table
create table unique_attributes (
  e integer not null unique
);

commit;
