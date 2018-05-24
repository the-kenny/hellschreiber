extern crate hellschreiber;
extern crate chrono;

use hellschreiber::*;

use std::io::{BufRead, BufReader};

#[allow(unused)]
struct DiaryEntry {
    eid: EntityId,
    date: chrono::DateTime<chrono::Utc>,
    text: String
}

impl<'a, D: Db> From<Entity<'a, D>> for DiaryEntry {
    fn from(o: Entity<'a, D>) -> DiaryEntry {
        DiaryEntry {
            eid: o.eid,
            date: o.get("diary.entry/date").unwrap().as_datetime().unwrap(),
            text: o.get("diary.entry/text").unwrap().as_string().unwrap(),
        }
    }
}

fn main() {
    let mut db = hellschreiber::SqliteDb::open("diary.sqlite").unwrap();

    if !db.has_attribute("diary.entry/text") {
        store_schema(&mut db);
    }

    let text_attribute = db.attribute("diary.entry/text").unwrap();

    for datom in db.datoms(Index::Aevt.a(text_attribute)).unwrap().iter() {
        let entry: DiaryEntry = db.entity(datom.entity).unwrap().into();
        println!("{:?}: {}", entry.date, entry.text);
    }

    for line in BufReader::new(std::io::stdin()).lines() {
        let line = line.unwrap();

        let entry = db.tempid();
        db.transact(&[(Assert, entry, "diary.entry/date", Value::DateTime(chrono::Utc::now())),
                      (Assert, entry, "diary.entry/text", line.into())])
            .unwrap();
    }
}

fn store_schema(db: &mut hellschreiber::SqliteDb) {
    let schema_tx = &[(Assert, db.tempid(), "db/ident", "diary.entry/text"),
                      (Assert, db.tempid(), "db/ident", "diary.entry/date")];
    db.transact(schema_tx)
        .unwrap();
}
