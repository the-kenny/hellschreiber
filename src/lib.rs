#[cfg(test)]
extern crate rand;
extern crate rusqlite;

#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
pub struct EntityId(i64);

#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
pub struct Attribute(EntityId);

#[derive(Debug, PartialEq, Eq, Clone, PartialOrd, Ord)]
pub enum Value {
  Str(String),
  Int(i64),
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
pub struct TxId(i64);

#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
pub enum Status {
  Added,
  Retracted
}

#[derive(Debug, PartialEq, Eq, Clone, PartialOrd, Ord)]
pub struct Datom {
  pub entity:    EntityId,
  pub attribute: Attribute,
  pub value:     Value,
  pub tx:        TxId,
  pub status:    Status,
}

type TempId = EntityId;
type Fact   = (TempId, Attribute, Value, Status);

use std::collections::BTreeMap;

#[allow(dead_code)]
pub struct Entity<'a, D: Db + 'a> {
  db: &'a D,
  values: BTreeMap<Attribute, Value>,
}

use std::borrow::Cow;
pub type Datoms<'a> = Cow<'a, [Datom]>;

pub trait Db: Sized {
  fn transact<T: Into<Fact>>(&mut self, tx: &[T]) -> TxId;
  fn all_datoms<'a>(&'a self) -> Datoms; // TODO: impl Iter?
    
  fn entity<'a>(&'a self, entity: EntityId) -> Entity<'a, Self> {
    let datoms = self.all_datoms();
    let mut attrs: BTreeMap<Attribute, (TxId, &Datom)> = Default::default();
    
    for d in datoms.into_iter().filter(|d| d.entity == entity) {
      use std::collections::btree_map::Entry;
      
      let entry = (d.tx, d);
      match attrs.entry(d.attribute) {
        Entry::Vacant(e) => { e.insert(entry); }
        Entry::Occupied(mut e) => {
          let &(db_tx, db_d) = e.get();
          if d.tx > db_tx {
            match d.status {
              // Newer datom was added, replace it
              Status::Added => {
                e.insert(entry);
              },
              // If retracted and the retracted value matches the
              // datom's value, apply retraction
              Status::Retracted if d.value == db_d.value => {
                e.remove();
              },
              // Retraction with wrong value. Ignore. TODO: Log warning
              Status::Retracted => (),
            }
          }
        }
      }
    }

    let values = attrs.values()
      .map(|&(_,d)| (d.attribute, d.value.clone()))
      .collect();

    Entity {
      db: self,
      values: values,
    }
  }

  fn store_datoms(&mut self, _datoms: &[Datom]) {
    unimplemented!()
  }
}


pub struct SqliteDb {
  conn: rusqlite::Connection,
}

impl SqliteDb {
  pub fn new() -> Self {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.execute_batch(include_str!("schema.sql"))
      .unwrap();
    
    SqliteDb { conn: conn }
  }
}

impl Db for SqliteDb {
  fn transact<T: Into<Fact>>(&mut self, _tx: &[T]) -> TxId {
    unimplemented!()
  }

  fn all_datoms<'a>(&'a self) -> Datoms {
    let mut stmt = self.conn.prepare("select e,a,v,t,retracted from datoms;").unwrap();
    let iter = stmt.query_map(&[], |row| {
      // let value = unimplemented!();
      Datom {
        entity: EntityId(row.get(0)),
        attribute: Attribute(EntityId(row.get(1))),
        value: Value::Str("".into()),
        tx: TxId(row.get(3)),
        status: match row.get(4) {
          0 => Status::Added,
          1 => Status::Retracted,
          unknown => panic!("Invalid status: {:?}", unknown)
        }
      }
    }).unwrap();
      
    let mut datoms = Vec::new();
    for d in iter {
      datoms.push(d.unwrap());
    }
    datoms.into()
  }

  fn store_datoms(&mut self, datoms: &[Datom]) {
    let mut stmt = self.conn.prepare("insert into datoms (e,a,v,t,retracted) values (?1, ?2, ?3, ?4, ?5)")
      .unwrap();
    for d in datoms {
      let retracted = match d.status {
        Status::Added => 1,
        Status::Retracted => 0,
      };
      stmt.execute(&[&(d.entity.0),
                     &(d.attribute.0).0,
                     &"",
                     &d.tx.0,
                     &retracted])
        .unwrap();
    }
  }
}


#[cfg(test)]
mod test_data;

#[cfg(test)]
mod test {
  use super::*;
  
  #[derive(Debug)]
  struct TestDb(Vec<Datom>);

  impl Db for TestDb {
    fn transact<T: Into<Fact>>(&mut self, _tx: &[T]) -> TxId {
      unimplemented!()
    }

    fn store_datoms(&mut self, datoms: &[Datom]) {
      self.0.clear();
      self.0.extend_from_slice(datoms);
    }
    
    fn all_datoms<'a>(&'a self) -> Datoms {
      Cow::Borrowed(&self.0)
    }
  }

  #[test]
  fn test_default_impl() {
    test_entity(TestDb(vec![]));
  }

    #[test]
  fn test_sqlite_impl() {
    test_entity(SqliteDb::new());
  }


  fn test_entity<D: Db>(mut db: D) {
    db.store_datoms(&test_data::make_test_data());

    assert_eq!(db.entity(EntityId(99999)).values.len(), 0);
    
    let heinz = db.entity(EntityId(1)).values;
    assert_eq!(heinz.len(), 2);
    assert_eq!(heinz.get(&test_data::person_name), Some(&Value::Str("Heinz".into())));
    assert_eq!(heinz.get(&test_data::person_age), Some(&Value::Int(42)));
    assert_eq!(heinz.get(&test_data::album_name), None);

    let karl  = db.entity(EntityId(2)).values;
    assert_eq!(karl.len(), 1);

    let nevermind = db.entity(EntityId(3)).values;
    assert_eq!(nevermind.len(), 1);
    assert_eq!(nevermind.get(&test_data::album_name), Some(&Value::Str("Nevermind".into())));
  }
}
