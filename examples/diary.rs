extern crate ratomic;
extern crate chrono;

use ratomic::*;

use std::io::{BufRead, BufReader};

fn main() {
  let mut db = ratomic::SqliteDb::open("diary.sqlite").unwrap();

  if !db.has_attribute("diary.entry/text") {
    store_schema(&mut db);
  }

  for line in BufReader::new(std::io::stdin()).lines() {
    let line = line.unwrap();

    let entry = db.tempid();
    db.transact(&[(Assert, entry, "diary.entry/date", Value::DateTime(chrono::Utc::now())),
                  (Assert, entry, "diary.entry/text", line.into())]);
  }
}

fn store_schema(db: &mut ratomic::SqliteDb) {
  let schema_tx = &[(Assert, db.tempid(), "db/ident", "diary.entry/text"),
                    (Assert, db.tempid(), "db/ident", "diary.entry/date")];
  db.transact(schema_tx);
}
