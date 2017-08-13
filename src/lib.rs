#[cfg(test)] extern crate rand;
extern crate rusqlite;
// #[macro_use] extern crate log;

pub mod sqlite;

use std::collections::{BTreeMap, BTreeSet};
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
  Retracted(TxId)
}

impl Status {
  fn is_retraction(&self) -> bool {
    self.retraction_tx().is_some()
  }

  fn is_assertion(&self) -> bool { *self == Status::Added }

  fn retraction_tx(&self) -> Option<TxId> {
    match *self {
      Status::Retracted(tx) => Some(tx),
      _                     => None
    }
  }
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

#[allow(dead_code)]
pub struct Entity<'a, D: Db + 'a> {
  db: &'a D,
  values: BTreeMap<Attribute, Vec<Value>>,
}

impl<'a, D: Db> Entity<'a, D> {
  // TODO
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

// TODO: AVET, AEVT, VAET

// VAET is used for navigating relations backwards and stores all
// datoms with *reference* attributes. Given VAET, you can not only find
// out whom John follows (“John” :follows ?x), but also efficiently
// lookup who follows John (?x :follows “John”).

// AEVT allows efficient access to all entities with a given attribute5

// AVET provides efficient lookup by value and stores datoms with
// attributes marked as unique or index in schema. Attributes of this
// kind are good for external ids. AVET is the most problematic index
// in practice, and it’s better if you can manage to put monotonic
// values in it, or use it sparingly.


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
  #[cfg(test)]
  fn all_datoms<'a>(&'a self) -> Datoms<'a>;

  fn transact<T: Into<Fact>>(&mut self, tx: &[T]) -> TxId;
  fn datoms<'a, C: Borrow<Components>>(&'a self, index: Index, components: C) -> Datoms<'a>;

  fn entity<'a>(&'a self, entity: EntityId) -> Entity<'a, Self> {
    let datoms = self.datoms(Index::Eavt, Components::empty());
    let mut attrs: BTreeMap<Attribute, BTreeSet<&Datom>> = Default::default();

    for d in datoms.into_iter().filter(|d| d.entity == entity) {
      let entry = attrs.entry(d.attribute)
        .or_insert_with(|| BTreeSet::new());
      
      match d.status {
        Status::Added => {
          entry.insert(&d);
        },
        Status::Retracted(_) if entry.contains(&d) => {
          entry.remove(&d);
        },
        Status::Retracted(_) => {
          unreachable!()
        }
      }
    }

    // Assert all datoms are of the same entity
    assert!(attrs.values().flat_map(|x| x).all(|d| d.entity == entity));
    
    let values = attrs.into_iter()
      .map(|(a, ds)| {
        let mut d: Vec<_> = ds.into_iter().collect();
        d.sort_by_key(|d| d.tx);
        (a, d.into_iter().map(|d| d.value.clone()).collect())
      }).collect();

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

        #[test]
        #[allow(unused_parens)]
        fn test_db_other_equality() {
          let db1 = ::tests::db::TestDb::new();
          let db2 = ($t);
          super::db::test_db_equality(db1, db2);
        }

        #[test]
        fn test_db_self_equality() {
          super::db::test_db_equality(($t), ($t));
        }
      }
    }
  }

  test_db_impl!(sqlite,    ::sqlite::SqliteDb::new());
  test_db_impl!(in_memory, ::tests::db::TestDb::new());
}
