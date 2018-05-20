# Hellschreiber

[![Build Status](https://travis-ci.org/the-kenny/hellschreiber.svg?branch=master)](https://travis-ci.org/the-kenny/hellschreiber)

Experiments in writing a SQLite backed database similar to Datomic.

## Examples

```rust
extern crate hellschreiber;
extern crate chrono;

use hellschreiber::{Db, SqliteDb, Assert, Value};

fn main() {
    // Open an in-memory DB
    let mut db = SqliteDb::new().expect("Failed to open DB");

    // Transact Schema
    let schema = &[(Assert, db.tempid(), "db/ident", "diary.entry/text"),
                   (Assert, db.tempid(), "db/ident", "diary.entry/date")];
    db.transact(schema).expect("Failed to transact schema");

    // Get temporary ID for our diary entry
    let entry_tempid = db.tempid();
    // A list of facts to be asserted.
    let tx = &[(Assert, entry_tempid, "diary.entry/date", Value::DateTime(chrono::Utc::now())),
               (Assert, entry_tempid, "diary.entry/text", "Hello World!".into())];
    let tx_data = db.transact(tx).expect("Failed to transact diary entry");

    // `tx_data` maps from our tempid `entry` to the real EntityId:
    let entity_id = tx_data.tempid_mappings[&entry_tempid];
    println!("Diary entry entity: {:?}", db.entity(entity_id).unwrap());
}
```
