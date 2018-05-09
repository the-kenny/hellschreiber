#[cfg(test)] extern crate rand;
extern crate rusqlite;
extern crate edn;
// #[macro_use] extern crate log;

pub mod sqlite;

pub use sqlite::SqliteDb;

use std::collections::{BTreeMap, BTreeSet};
use std::borrow::{Borrow, Cow};
use std::fmt;

#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
pub struct EntityId(i64);

pub type TxId = EntityId;

#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
pub struct Ref(EntityId);

#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
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

#[derive(Debug, PartialEq, Eq, Clone, PartialOrd, Ord)]
pub enum Value {
  Str(String),
  Int(i64),
  // TODO: Ref
}

impl<'a> From<&'a str> for Value {
  fn from(s: &'a str) -> Value { Value::Str(s.into()) }
}

impl From<String> for Value {
  fn from(s: String) -> Value { Value::Str(s) }
}

impl From<i64> for Value {
  fn from(i: i64) -> Value { Value::Int(i) }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
pub enum Status {
  Added,
  Retracted(EntityId)
}

impl Status {
  fn is_retraction(&self) -> bool {
    self.retraction_tx().is_some()
  }

  fn is_assertion(&self) -> bool { *self == Status::Added }

  fn retraction_tx(&self) -> Option<EntityId> {
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

impl Datom {
  pub fn from_edn<D: Db>(db: &D, edn: edn::Value) -> Result<Self, ()> {
    use edn::Value::*;

    if let Vector(x) = edn {
      if x.len() != 4 { return Err(()) }

      let mut x = x.into_iter();
      let e = x.next().unwrap();
      let a = x.next().unwrap();
      let v = x.next().unwrap();
      let t = x.next().unwrap();

      match (e,a,v,t) {
        (Integer(e), Keyword(a), v, Integer(t)) => {
          let v = match v {
            String(s)  => Value::Str(s),
            Integer(i) => Value::Int(i),
            x          => unimplemented!("Conversion from EDN value {:?} isn't implemented", x),
          };

          let a = db.attribute(&a).unwrap();

          let d = Datom {
            entity: EntityId(e),
            attribute: a, // TODO
            value: v,
            tx: EntityId(t),
            status: Status::Added,
          };

          println!("{:?}", d);
        },
        _ => unreachable!()
      }
    }

    unimplemented!("Implementation of from_edn")
  }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
pub struct TempId(pub i64);

#[derive(Debug)]
pub enum Operation {
  Assertion(EntityId, Attribute, Value),
  Retraction(EntityId, Attribute, Value),
  TempidAssertion(TempId, Attribute, Value)
}

pub struct Assert;
pub struct Retract;

impl<'a, V: Into<Value> + Clone> From<&'a (Assert, TempId, Attribute, V)> for Operation {
  fn from(o: &(Assert, TempId, Attribute, V)) -> Self {
    Operation::TempidAssertion(o.1, o.2, o.3.clone().into())
  }
}

impl<'a, V> From<&'a (Assert, EntityId, Attribute, V)> for Operation
  where V: Into<Value> + Clone {
  fn from(o: &'a (Assert, EntityId, Attribute, V)) -> Operation {
    Operation::Assertion(o.1, o.2, o.3.clone().into())
  }
}

impl<'a, V> From<&'a (Retract, EntityId, Attribute, V)> for Operation
  where V: Into<Value> + Clone {
  fn from(o: &'a (Retract, EntityId, Attribute, V)) -> Operation {
    Operation::Retraction(o.1, o.2, o.3.clone().into())
  }
}


#[allow(dead_code)]
pub struct Entity<'a, D: Db + 'a> {
  pub db: &'a D,
  pub values: BTreeMap<Attribute, Vec<Value>>,
}

impl<'a, D: Db> fmt::Debug for Entity<'a, D> {
  fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
    let pretty_values: BTreeMap<_, &Vec<Value>> = self.values.iter()
      .map(|(attr, value)| (self.db.attribute_name(attr).unwrap(), value))
      .collect();

    write!(f, "<Entity {:?}>", pretty_values)
  }
}

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

  pub fn e(e: EntityId) -> Self {
    Components(Some(e), None, None, None)
  }

  pub fn ea(e: EntityId, a: Attribute) -> Self {
    Components(Some(e), Some(a), None, None)
  }

  pub fn eav(e: EntityId, a: Attribute, v: Value) -> Self {
    Components(Some(e), Some(a), Some(v), None)
  }

  pub fn eavt(e: EntityId, a: Attribute, v: Value, t: TxId) -> Self {
    Components(Some(e), Some(a), Some(v), Some(t))
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

pub mod attr {
  #![allow(non_upper_case_globals)]
  use super::{Attribute, EntityId};
  pub const id:    Attribute = Attribute(EntityId(10));
  pub const ident: Attribute = Attribute(EntityId(11));
  pub const doc:   Attribute = Attribute(EntityId(12));
  // pub const valueType:   Attribute = Attribute(EntityId(12));
  // pub const cardinality: Attribute = Attribute(EntityId(13));
  // pub const unique:      Attribute = Attribute(EntityId(14));
}

fn seed_datoms() -> Datoms<'static> {
  // db/id
  let id = Datom {
    entity: attr::id.0,
    attribute: attr::ident,
    value: Value::Str("db/id".into()),
    tx: EntityId(0),
    status: Status::Added,
  };

  // db/ident
  let ident = Datom {
    entity: attr::ident.0,
    attribute: attr::ident,
    value: Value::Str("db/ident".into()),
    tx: EntityId(0),
    status: Status::Added,
  };

  let doc = Datom {
    entity: attr::doc.0,
    attribute: attr::ident,
    value: Value::Str("db/doc".into()),
    tx: EntityId(0),
    status: Status::Added,
  };

  let ident_doc = Datom {
    entity: attr::ident.0,
    attribute: attr::doc,
    value: Value::Str("Unique identifier for an entity.".into()),
    tx: EntityId(0),
    status: Status::Added,
  };

  let doc_doc = Datom {
    entity: attr::doc.0,
    attribute: attr::doc,
    value: Value::Str("Description of an attribute.".into()),
    tx: EntityId(0),
    status: Status::Added,
  };

  let datoms = vec![
    id,
    ident,
    doc,
    ident_doc,
    doc_doc];

  Cow::Owned(datoms)
}

#[derive(Debug, Clone)]
pub struct TransactionData {
  pub tx_id: TxId,
  pub tempid_mappings: BTreeMap<TempId, EntityId>
}

// TODO: Add `is_initialized?` and `initialize`
pub trait Db: Sized {
  #[cfg(test)]
  fn all_datoms<'a>(&'a self) -> Datoms<'a>;

  fn highest_eid(&mut self) -> EntityId {
    let n = self.datoms(Index::Eavt, Components::empty())
      .into_iter()
      .last()
      .map(|datom| datom.entity.0)
      .unwrap_or(0);

    EntityId(std::cmp::max(n, 1000))
  }

  fn transact<O: Into<Operation>, I: IntoIterator<Item=O>>(&mut self, tx: I) -> TransactionData {
    let tx_eid = self.highest_eid();

    let tx: Vec<Operation> = tx.into_iter().map(|op| op.into()).collect();

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

    let datoms = tx.into_iter()
      .map(|operation| {
        let (e, a, v, status) = match operation.into() {
          Operation::Assertion(eid, a, v)       => (eid,        a, v, Status::Added),
          Operation::Retraction(eid, a, v)      => (eid,        a, v, Status::Retracted(tx_eid)),
          Operation::TempidAssertion(tid, a, v) => (eids[&tid], a, v, Status::Added)
        };

        Datom {
          entity: e,
          attribute: a,
          value: v.clone(),
          tx: tx_eid,
          status: status
        }
      }).collect::<Vec<Datom>>();

    self.store_datoms(&datoms);

    TransactionData {
      tx_id: tx_eid,
        tempid_mappings: eids,
    }
  }

  fn datoms<'a, C: Borrow<Components>>(&'a self, index: Index, components: C) -> Datoms<'a>;

  fn entity<'a>(&'a self, entity: EntityId) -> Entity<'a, Self> {
    let datoms = self.datoms(Index::Eavt, Components::empty());
    let mut attrs: BTreeMap<Attribute, BTreeSet<&Datom>> = BTreeMap::new();

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

    let mut values = attrs.into_iter()
      .map(|(a, ds)| {
        let mut d: Vec<_> = ds.into_iter().collect();
        d.sort_by_key(|d| d.tx);
        (a, d.into_iter().map(|d| d.value.clone()).collect())
      }).collect::<BTreeMap<Attribute, Vec<Value>>>();

    values.insert(attr::id, vec![Value::Int(entity.0)]);

    Entity {
      db: self,
      values: values,
    }
  }

  fn store_datoms(&mut self, _datoms: &[Datom]);

  fn attribute(&self, attribute_name: &str) -> Option<Attribute> {
    // TODO: Use VAET
    self.datoms(Index::Eavt, Components(None, Some(attr::ident), Some(Value::Str(attribute_name.into())), None))
      .iter().next()
      .map(|d| Attribute::new(d.entity))
  }

  fn attribute_name<'a>(&'a self, attribute: &Attribute) -> Option<String> {
    self.datoms(Index::Eavt, Components::ea(attribute.0, attr::ident))
      .iter().next()
      .map(|d| d.value.clone())
      .and_then(|v| match v {
        Value::Str(s) => Some(s.clone()),
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

  #[macro_export]
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
        #[test] fn test_datoms() {super::db::test_datoms($t);}
        #[test] fn test_db_self_equality() {super::db::test_db_equality($t, $t);}
        #[test] fn test_db_fn_attribute() {super::db::test_fn_attribute($t)}
        #[test] fn test_db_metadata() { super::db::test_db_metadata($t) }
        #[test] fn test_string_attributes() { super::db::test_string_attributes($t) }

        #[test] fn test_usage_001() { super::usage::test_usage_001($t) }
      }
    }
  }

  test_db_impl!(sqlite_db,    ::sqlite::SqliteDb::new());
  test_db_impl!(in_memory_db, ::tests::in_memory::TestDb::new());
}
