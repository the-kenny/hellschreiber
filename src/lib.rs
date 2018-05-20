#[cfg(test)] extern crate rand;
#[macro_use] extern crate derive_more;
#[macro_use] extern crate lazy_static;
#[macro_use] extern crate serde_derive;
extern crate chrono;
extern crate edn;
extern crate rusqlite;
extern crate serde;
extern crate serde_json;
#[macro_use] extern crate failure;

mod index;
pub use index::*;

mod transaction;
pub use transaction::*;

mod entity;
pub use entity::Entity;

mod value;
pub use value::Value;

mod sqlite;
pub use sqlite::SqliteDb;

use failure::Error;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::atomic;
use std::iter::FromIterator;

#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EntityId(i64);

pub type TxId = EntityId;

#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord, Hash)]
pub struct Attribute(EntityId);

impl Attribute {
  fn new(id: EntityId) -> Self {
    Attribute(id)
  }
}

pub trait ToAttribute {
  fn to_attribute<D: Db>(&self, db: &D) -> Option<Attribute>;
}

impl ToAttribute for Attribute {
  fn to_attribute<D: Db>(&self, _db: &D) -> Option<Attribute> {
    Some(*self)
  }
}

impl<'a> ToAttribute for &'a str {
  fn to_attribute<D: Db>(&self, db: &D) -> Option<Attribute> {
    db.attribute(self)
  }
}


#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
pub enum Status {
  Asserted,
  Retracted(EntityId)
}

impl Status {
  fn is_assertion(&self) -> bool { *self == Status::Asserted }

  fn is_retraction(&self) -> bool {
    match self {
      Status::Retracted(_) => true,
      _ => false
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

#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
pub struct TempId(pub i64);

pub type Datoms<'a> = Vec<Datom>;

pub(crate) mod attr {
  #![allow(non_upper_case_globals)]
  use super::{Attribute, EntityId};
  pub const id:         Attribute = Attribute(EntityId(10));
  pub const ident:      Attribute = Attribute(EntityId(11));
  pub const doc:        Attribute = Attribute(EntityId(12));
  pub const tx_instant: Attribute = Attribute(EntityId(13));
}

fn seed_datoms() -> Datoms<'static> {
  let datoms = [(attr::id,         "db/id"),
                (attr::ident,      "db/ident"),
                (attr::doc,        "db/doc"),
                (attr::tx_instant, "db/tx_instant"),
  ].iter().
    map(|(attr, ident)| {
      Datom {
        entity: attr.0,
        attribute: attr::ident,
        value: Value::Str(ident.to_string()),
        tx: EntityId(0),
        status: Status::Asserted,
      }
    }).collect::<Vec<Datom>>();

  datoms
}

#[derive(Debug, Clone)]
pub struct TransactionData {
  pub tx_id: TxId,
  pub tempid_mappings: BTreeMap<TempId, EntityId>
}

#[derive(Debug, Fail, PartialEq, Eq)]
pub enum TransactionError {
  #[fail(display = "Tried to transact fact for attribute {:?} without db/ident", _0)]
  NonIdentAttributeTransacted(Attribute),
}

lazy_static! {
  static ref LATEST_TEMPID: atomic::AtomicIsize  = 100.into();
}

// TODO: Add `is_initialized?` and `initialize`
pub trait Db: Sized {
  #[cfg(test)]
  fn all_datoms<'a>(&'a self) -> Datoms<'a>;

  fn highest_eid(&self) -> EntityId {
    // TODO: Use FilteredIndex's impl
    let n = self.datoms(Index::Eavt).unwrap() // TODO
      .into_iter()
      .last()
      .map(|datom| datom.entity.0)
      .unwrap_or(0);

    EntityId(std::cmp::max(n, 1000))
  }

  fn tempid(&mut self) -> TempId {
    let i = LATEST_TEMPID.fetch_add(1, atomic::Ordering::SeqCst);
    TempId(i as i64)
  }


  fn transact<O: ToOperation, I: IntoIterator<Item=O>>(&mut self, tx: I) -> Result<TransactionData, Error> {
    let tx_eid = self.highest_eid();

    let now = chrono::Utc::now();

    let mut datoms = vec![Datom {
      entity: tx_eid,
      attribute: attr::tx_instant,
      value: Value::DateTime(now),
      tx: tx_eid,
      status: Status::Asserted
    }];

    let tx: Vec<Operation> = tx.into_iter().map(|op| op.to_operation(self)).collect();

    datoms.reserve(tx.len());

    let eids = {
      let mut eids = BTreeMap::new();
      let mut highest_eid = tx_eid.0;
      for operation in tx.iter() {
        if let &Operation::TempidAssertion(e, _, _) = operation {
          eids.entry(e)
            .or_insert_with(|| {
              highest_eid += 1;
              EntityId(highest_eid)
            });
        }
      }
      eids
    };

    for operation in tx {
      let (e, a, v, status) = match operation.into() {
        Operation::Assertion(eid, a, v)       => (eid,        a, v, Status::Asserted),
        Operation::Retraction(eid, a, v)      => (eid,        a, v, Status::Retracted(tx_eid)),
        Operation::TempidAssertion(tid, a, v) => (eids[&tid], a, v, Status::Asserted)
      };

      if !self.attribute_name(a).is_some() {
        return Err(TransactionError::NonIdentAttributeTransacted(a).into())
      }

      let datom = Datom {
        entity: e,
        attribute: a,
        value: v.clone(),

        tx: tx_eid,
        status: status
      };

      datoms.push(datom);
    }

    self.store_datoms(&datoms)?;

    Ok(TransactionData {
      tx_id: tx_eid,
      tempid_mappings: eids,
    })
  }

  fn datoms<'a, I: Into<FilteredIndex>>(&'a self, index: I) -> Result<Datoms<'a>, Error>;

  fn entity<'a>(&'a self, entity: EntityId) -> Result<Entity<'a, Self>, Error> {
    let datoms = self.datoms(Index::Eavt.e(entity))?;
    let mut attrs: BTreeMap<Attribute, BTreeSet<&Datom>> = BTreeMap::new();

    for d in datoms.iter() {
      let entry = attrs.entry(d.attribute)
        .or_insert_with(|| BTreeSet::new());

      match d.status {
        Status::Asserted => {
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

    let mut values = attrs.into_iter()
      .map(|(a, ds)| {
        let mut d: Vec<_> = ds.into_iter().collect();
        d.sort_by_key(|d| d.tx);
        (a, d.into_iter().map(|d| d.value.clone()).collect())
      }).collect::<BTreeMap<Attribute, Vec<Value>>>();

    values.insert(attr::id, vec![Value::Int(entity.0)]);

    let entity = Entity {
      db: self,
      eid: entity,
      values: values,
    };

    Ok(entity)
  }

  fn store_datoms(&mut self, _datoms: &[Datom]) -> Result<(), Error>;

  fn has_attribute(&self, attribute_name: &str) -> bool {
    self.attribute(attribute_name).is_some()
  }

  fn indexed_attributes(&self) -> HashSet<Attribute> {
    HashSet::from_iter(vec![attr::ident])
  }

  fn attribute(&self, attribute_name: &str) -> Option<Attribute> {
    self.datoms(Index::Avet.a(attr::ident).v(Value::Str(attribute_name.into())))
      .unwrap()
      .iter().next()
      .map(|d| Attribute::new(d.entity))
  }

  fn attribute_name<'a>(&'a self, attribute: Attribute) -> Option<String> {
    self.datoms(Index::Avet.e(attribute.0).a(attr::ident)).unwrap()
      .into_iter()
      .next()
      .and_then(|d| match d.value {
        Value::Str(ref s) => Some(s.clone()),
        _ => None
      })
  }
}

#[cfg(test)]
mod tests {
  mod db;
  mod data;
  mod in_memory;
  mod usage;

  macro_rules! test_db_impl {
    ($name:ident, $t:expr) => {
      mod $name {
        #[test]
        #[allow(unused_parens)]
        fn test_db_other_equality() {
          let db1 = ::tests::in_memory::TestDb::new();
          let db2 = ($t);
          super::db::test_db_equality(db1, db2);
        }

        #[test] fn test_entity() {super::db::test_entity($t);}
        #[test] fn test_seed_datoms() {super::db::test_seed_datoms($t);}
        #[test] fn test_eavt_datoms() {super::db::test_eavt_datoms($t);}
        #[test] fn test_aevt_datoms() {super::db::test_aevt_datoms($t);}
        #[test] fn test_self_equality() {super::db::test_db_equality($t, $t);}
        #[test] fn test_fn_attribute() {super::db::test_fn_attribute($t)}
        #[test] fn test_metadata() { super::db::test_db_metadata($t) }
        #[test] fn test_string_attributes() { super::db::test_string_attributes($t) }
        #[test] fn test_highest_eid() { super::db::test_highest_eid($t) }
        #[test] fn test_transact_unknown_attribute_error() { super::db::test_transact_unknown_attribute_error($t) }
        #[test] fn test_avet_index() { super::db::test_avet_index($t); }

        #[test] fn test_entity_index_trait() { super::db::test_entity_index_trait($t) }

        #[test] fn test_usage_001() { super::usage::test_usage_001($t) }
      }
    }
  }

  test_db_impl!(sqlite_db,    ::sqlite::SqliteDb::new().unwrap());
  test_db_impl!(in_memory_db, ::tests::in_memory::TestDb::new());
}
