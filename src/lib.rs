#[cfg(test)]
extern crate rand;
extern crate rusqlite;

use std::borrow::Borrow;

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

#[derive(Debug, Clone, Copy)]
pub enum Index {
  Eavt,
  // Avet,
  // Aevt,
  // Vaet,
}

#[derive(Debug)]
pub struct Components(Option<EntityId>,
                      Option<Attribute>,
                      Option<Value>,
                      Option<TxId>);

impl Components {
  pub fn empty() -> Self {
    Components(None, None, None, None)
  }

  pub fn matches(&self, datom: &Datom) -> bool {
    let &Components(e, a, ref v, t) = self;

    let e = e.is_none() || e.unwrap() == datom.entity;
    let a = a.is_none() || a.unwrap() == datom.attribute;
    let v = v.is_none() || v.as_ref().unwrap() == &datom.value;
    let t = t.is_none() || t.unwrap() == datom.tx;

    return e && a && v && t;;
  }
}

pub trait Db: Sized {
  fn transact<T: Into<Fact>>(&mut self, tx: &[T]) -> TxId;
  fn datoms<'a, C: Borrow<Components>>(&'a self, index: Index, components: C) -> Datoms;

  fn entity<'a>(&'a self, entity: EntityId) -> Entity<'a, Self> {
    let datoms = self.datoms(Index::Eavt, Components::empty());
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

  fn datoms<'a, C: Borrow<Components>>(&'a self, _index: Index, _components: C) -> Datoms {
    unimplemented!()
  }

  // fn all_datoms<'a>(&'a self) -> Datoms {
  //   let mut stmt = self.conn.prepare("select e,a,v,t,retracted from datoms order by t asc").unwrap();
  //   let iter = stmt.query_map(&[], |row| {
  //     Datom {
  //       entity: EntityId(row.get(0)),
  //       attribute: Attribute(EntityId(row.get(1))),
  //       value: row.get(2),
  //       tx: TxId(row.get(3)),
  //       status: match row.get(4) {
  //         0 => Status::Added,
  //         1 => Status::Retracted,
  //         unknown => panic!("Invalid status: {:?}", unknown)
  //       }
  //     }
  //   }).unwrap();

  //   let mut datoms = Vec::new();
  //   for d in iter {
  //     datoms.push(d.unwrap());
  //   }
  //   datoms.into()
  // }

  fn store_datoms(&mut self, datoms: &[Datom]) {
    let mut stmt = self.conn.prepare("insert into datoms (e,a,v,t,retracted) values (?1, ?2, ?3, ?4, ?5)")
      .unwrap();
    for d in datoms {
      let retracted = match d.status {
        Status::Added => 0,
        Status::Retracted => 1,
      };
      stmt.execute(&[&(d.entity.0),
                     &(d.attribute.0).0,
                     &d.value,
                     &d.tx.0,
                     &retracted])
        .unwrap();
    }
  }
}

impl rusqlite::types::FromSql for Value {
  fn column_result(value: rusqlite::types::ValueRef) -> rusqlite::types::FromSqlResult<Self> {
    use rusqlite::types::ValueRef;

    match value {
      ValueRef::Text(t)    => Ok(Value::Str(t.into())),
      ValueRef::Integer(i) => Ok(Value::Int(i)),
      _                    => unreachable!() // TODO
    }
  }
}

impl rusqlite::types::ToSql for Value {
  fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput> {
    use rusqlite::types::{ValueRef, ToSqlOutput};
    match self {
      &Value::Str(ref s) => Ok(ToSqlOutput::Borrowed(ValueRef::Text(s))),
      &Value::Int(i) => Ok(ToSqlOutput::Owned(rusqlite::types::Value::Integer(i)))
    }
  }
}

#[cfg(test)]
mod test_data;

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_components() {
    let d = Datom {
      entity:    EntityId(42),
      attribute: Attribute(EntityId(1)),
      value:     Value::Int(23),
      tx:        TxId(10),
      status:    Status::Added,
    };

    assert_eq!(true, Components(None, None, None, None).matches(&d));

    assert_eq!(true,  Components(Some(d.entity), None,              None,                  None).matches(&d));
    assert_eq!(true,  Components(None,           Some(d.attribute), None,                  None).matches(&d));
    assert_eq!(true,  Components(None,           None,              Some(d.value.clone()), None).matches(&d));
    assert_eq!(true,  Components(None,           None,              None,                  Some(d.tx)).matches(&d));
    assert_eq!(true,  Components(Some(d.entity), Some(d.attribute), None,                  None).matches(&d));
    assert_eq!(true,  Components(Some(d.entity), Some(d.attribute), Some(d.value.clone()), None).matches(&d));
    assert_eq!(true,  Components(Some(d.entity), Some(d.attribute), Some(d.value.clone()), Some(d.tx)).matches(&d));

    assert_eq!(false, Components(Some(EntityId(999)), None, None, None).matches(&d));
    assert_eq!(false, Components(None, Some(Attribute(EntityId(999))), None, None).matches(&d));
    assert_eq!(false, Components(None, None, Some(Value::Int(1000)),None).matches(&d));
    assert_eq!(false, Components(None, None, None, Some(TxId(999))).matches(&d));
  }

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

    fn datoms<'a, C: Borrow<Components>>(&'a self, index: Index, components: C) -> Datoms {
      let mut datoms = self.0.clone();
      datoms.retain(|d| components.borrow().matches(&d));

      datoms.sort_by(|l,r| {
        use std::cmp::Ordering;
        macro_rules! cmp {
          ($i:ident) => (l.$i.cmp(&r.$i));
          ($($i:ident),*) => {
            [$(cmp!($i)),*].into_iter().fold(Ordering::Equal, |o, x| o.then(*x))

          };
        }

        match index {
          Index::Eavt => cmp!(tx, entity, attribute, value),
        }
      });

      struct EavEquality(Datom);

      impl PartialEq for EavEquality {
        fn eq(&self, rhs: &EavEquality) -> bool {
          let EavEquality(ref lhs) = *self;
          let EavEquality(ref rhs) = *rhs;
          lhs.entity == rhs.entity
            && lhs.attribute == rhs.attribute
            && lhs.value == rhs.value
        }
      }

      use std::collections::BTreeMap;
      use std::collections::btree_map::Entry;

      let mut ds: BTreeMap<(EntityId,Attribute), (&Value, TxId, Status)> = BTreeMap::new();

      for d in datoms.iter() {
        let item = (d.entity, d.attribute);

        match ds.entry(item) {
          Entry::Vacant(_) if d.status == Status::Retracted => { }
          Entry::Vacant(e)       => { e.insert((&d.value, d.tx, d.status)); }
          Entry::Occupied(mut e) => {
            let (value, tx, _) = *e.get();
            if d.tx > tx {
              match d.status {
                // Newer datom was added, replace it
                Status::Added => {
                  e.insert((&d.value, d.tx, d.status));
                },
                // If retracted and the retracted value matches the
                // datom's value, apply retraction
                Status::Retracted if *value == d.value => {
                  e.remove();
                },
                Status::Retracted => {}
              }
            }
          }
        }
      }

      let datoms = ds.into_iter()
        .map(|((e,a),(v, tx, s))| {
          Datom {
            entity: e,
            attribute: a,
            value: v.clone(),
            tx: tx,
            status: s,
          }
        })
        .collect();

      Cow::Owned(datoms)
    }
  }

  macro_rules! test_db_impl {
    ($name:ident, $t:expr) => {
      mod $name {
        use super::*;
        #[test]
        fn test_entity() {
          super::test_entity(($t));
        }
        #[test]
        fn test_datoms() {
          super::test_datoms(($t));
        }
      }
    }
  }

  test_db_impl!(in_memory, TestDb(vec![]));
  test_db_impl!(sqlite,    SqliteDb::new());

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

  fn test_datoms<D: Db>(mut db: D) {
    db.store_datoms(&test_data::make_test_data());

    let pn = test_data::person_name;
    let pa = test_data::person_age;
    let an = test_data::album_name;

    let heinz     = EntityId(1);
    let karl      = EntityId(2);
    let nevermind = EntityId(3);

    let eavt = db.datoms(Index::Eavt, Components::empty());
    assert_eq!(eavt.iter().map(|d| d.entity).collect::<Vec<_>>(),
               vec![heinz,heinz,karl,nevermind]);
    assert_eq!(eavt.iter().map(|d| d.attribute).collect::<Vec<_>>(),
               vec![pn, pa, pn, an]);

    // None
    let eavt = db.datoms(Index::Eavt, Components(Some(EntityId(99999)), None, None, None));
    assert!(eavt.iter().map(|d| d.entity).collect::<Vec<_>>().is_empty());

    // Heinz
    let eavt = db.datoms(Index::Eavt, Components(Some(heinz), None, None, None));
    assert_eq!(eavt.iter().map(|d| d.entity).collect::<Vec<_>>(),
               vec![heinz,heinz]);
    assert_eq!(eavt.iter().map(|d| d.attribute).collect::<Vec<_>>(),
               vec![pn, pa]);

    // Nevermind
    let eavt = db.datoms(Index::Eavt, Components(Some(nevermind), None, None, None));
    assert_eq!(eavt.iter().map(|d| d.entity).collect::<Vec<_>>(),
               vec![nevermind]);
    assert_eq!(eavt.iter().map(|d| d.attribute).collect::<Vec<_>>(),
               vec![an]);
  }
}
