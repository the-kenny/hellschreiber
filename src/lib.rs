#[cfg(test)] extern crate rand;
extern crate rusqlite;

pub mod sqlite;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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


#[cfg(test)]
mod tests {
  mod db;
  mod data;

  #[macro_export]
  macro_rules! test_db_impl {
    ($name:ident, $t:expr) => {
      mod $name {
        #[test]
        fn test_entity() {
          super::db::test_entity(($t));
        }
        #[test]
        fn test_datoms() {
          super::db::test_datoms(($t));
        }
      }
    }
  }

  test_db_impl!(sqlite,    ::sqlite::SqliteDb::new());
  test_db_impl!(in_memory, ::tests::db::TestDb::new());
}
