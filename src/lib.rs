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

pub mod sqlite;
pub use sqlite::SqliteDb;

use failure::Error;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::{fmt, ops};
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

#[derive(Debug, PartialEq, Eq, Clone, PartialOrd, Ord, Serialize, Deserialize, From)]
pub enum Value {
  Str(String),
  Int(i64),
  Ref(EntityId),
  DateTime(chrono::DateTime<chrono::Utc>)
  // TODO: Ref
}

impl Value {
  pub fn as_str<'a>(&'a self) -> Option<&'a str> {
    if let &Value::Str(ref s) = self {
      Some(&s[..])
    } else {
      None
    }
  }

  pub fn as_string(&self) -> Option<String> {
    if let &Value::Str(ref s) = self {
      Some(s.clone())
    } else {
      None
    }
  }

  pub fn as_int(&self) -> Option<i64> {
    if let &Value::Int(i) = self {
      Some(i)
    } else {
      None
    }
  }

  pub fn as_datetime(&self) -> Option<chrono::DateTime<chrono::Utc>> {
    if let &Value::DateTime(i) = self {
      Some(i)
    } else {
      None
    }
  }

  pub fn follow_ref<'a, D: Db>(&self, db: &'a D) -> Option<Entity<'a, D>> {
    if let &Value::Ref(eid) = self {
      Some(db.entity(eid).unwrap()) // TODO
    } else {
      None
    }
  }
}

impl<'a> From<&'a str> for Value {
  fn from(s: &'a str) -> Value { Value::Str(s.into()) }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
pub enum Status {
  Added,
  Retracted(EntityId)
}

impl Status {
  fn is_retraction(&self) -> bool {
    match self {
      Status::Retracted(_) => true,
      _ => false
    }
  }

  fn is_assertion(&self) -> bool { *self == Status::Added }
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

#[derive(Debug)]
pub enum Operation {
  Assertion(EntityId, Attribute, Value),
  Retraction(EntityId, Attribute, Value),
  TempidAssertion(TempId, Attribute, Value)
}

pub struct Assert;
pub struct Retract;

pub trait ToOperation {
  fn to_operation<D: Db>(&self, db: &D) -> Operation;
}

impl<'a, V: Into<Value> + Clone, A: ToAttribute> ToOperation for &'a (Assert, TempId, A, V) {
  fn to_operation<D: Db>(&self, db: &D) -> Operation {
    let a = self.2.to_attribute(db)
      .expect("Unknown attribute in transaction");
    Operation::TempidAssertion(self.1, a, self.3.clone().into())
  }
}

impl<'a, V> ToOperation for &'a (Assert, EntityId, Attribute, V)
  where V: Into<Value> + Clone {
  fn to_operation<D: Db>(&self, _db: &D) -> Operation {
    Operation::Assertion(self.1, self.2, self.3.clone().into())
  }
}

impl<'a, V> ToOperation for &'a (Retract, EntityId, Attribute, V)
  where V: Into<Value> + Clone {
  fn to_operation<D: Db>(&self, _db: &D) -> Operation {
    Operation::Retraction(self.1, self.2, self.3.clone().into())
  }
}

#[allow(dead_code)]
pub struct Entity<'a, D: Db + 'a> {
  pub db: &'a D,
  pub eid: EntityId,
  pub values: BTreeMap<Attribute, Vec<Value>>,
}

impl<'a, D: Db> fmt::Debug for Entity<'a, D> {
  fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
    let pretty_values: BTreeMap<_, &Vec<Value>> = self.values.iter()
      .map(|(attr, value)| (self.db.attribute_name(*attr).unwrap(), value))
      .collect();

    write!(f, "<Entity {:?} {:?}>", self.eid, pretty_values)
  }
}

lazy_static! {
  static ref EMPTY_VEC: Vec<Value> = vec![];
}

impl<'a, D: Db> ops::Index<&'a str> for &'a Entity<'a, D> {
  type Output = Vec<Value>;
  fn index(&self, idx: &'a str) -> &Self::Output {
    if idx == "db/id" {
      unimplemented!("Value::Ref or Value::Eid")
    } else {
      self.db.attribute(idx)
        .and_then(|attr_id| self.values.get(&attr_id))
        .unwrap_or(&EMPTY_VEC)
    }
  }
}

// TODO: Get rid of duplication
impl<'a, D: Db> ops::Index<&'a str> for Entity<'a, D> {
  type Output = Vec<Value>;
  fn index(&self, idx: &'a str) -> &Self::Output {
    if idx == "db/id" {
      unimplemented!("Value::Ref or Value::Eid")
    } else {
      self.db.attribute(idx)
        .and_then(|attr_id| self.values.get(&attr_id))
        .unwrap_or(&EMPTY_VEC)
    }
  }
}


pub type Datoms<'a> = Vec<Datom>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexType {
  Eavt,
  Aevt,
  Avet
}

impl IndexType {
  // pub fn matches(&self, components: &Components, datom: &Datom) -> bool {
  //   let e = components.e;
  //   let a = components.a;
  //   let ref v = components.v;
  //   let t = components.t;

  //   let e = e.is_none() || e.unwrap() == datom.entity;
  //   let a = a.is_none() || a.unwrap() == datom.attribute;
  //   let v = v.is_none() || v.as_ref().unwrap() == &datom.value;
  //   let t = t.is_none() || t.unwrap() == datom.tx;

  //   return e && a && v && t;;
  // }

  pub fn e(self, e: EntityId)  -> Index { Index::new(self).e(e) }
  pub fn a(self, a: Attribute) -> Index { Index::new(self).a(a) }
  pub fn v(self, v: Value)     -> Index { Index::new(self).v(v) }
  pub fn t(self, t: TxId)      -> Index { Index::new(self).t(t) }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Index {
  index: IndexType,
  
  e: Option<EntityId>,
  a: Option<Attribute>,
  v: Option<Value>,
  t: Option<TxId>,
}

impl Index {
  pub fn new(index: IndexType) -> Self {
    Self { index: index, e: None, a: None, v: None, t: None }
  }
  
  pub fn e(mut self, e: EntityId)  -> Self { self.e = Some(e); self }
  pub fn a(mut self, a: Attribute) -> Self { self.a = Some(a); self }
  pub fn v(mut self, v: Value)     -> Self { self.v = Some(v); self }
  pub fn t(mut self, t: TxId)      -> Self { self.t = Some(t); self }

  fn eavt(&self) -> (Option<EntityId>, Option<Attribute>, Option<Value>, Option<TxId>) {
    (self.e, self.a, self.v.clone(), self.t)
  }

  pub fn matches(&self, datom: &Datom) -> bool {
    let e     = self.e;
    let a     = self.a;
    let ref v = self.v;
    let t     = self.t;

    let e = e.is_none() || e.unwrap() == datom.entity;
    let a = a.is_none() || a.unwrap() == datom.attribute;
    let v = v.is_none() || v.as_ref().unwrap() == &datom.value;
    let t = t.is_none() || t.unwrap() == datom.tx;

    return e && a && v && t;;
  }
}

impl From<IndexType> for Index {
  fn from(i: IndexType) -> Self {
    Index::new(i)
  }
}

// TODO: VAET

// VAET is used for navigating relations backwards and stores all
// datoms with *reference* attributes. Given VAET, you can not only find
// out whom John follows (“John” :follows ?x), but also efficiently
// lookup who follows John (?x :follows “John”).

// AEVT allows efficient access to all entities with a given attribute

// AVET provides efficient lookup by value and stores datoms with
// attributes marked as unique or index in schema. Attributes of this
// kind are good for external ids. AVET is the most problematic index
// in practice, and it’s better if you can manage to put monotonic
// values in it, or use it sparingly.

// #[derive(Debug)]
// pub struct Components(Option<EntityId>,
//                       Option<Attribute>,
//                       Option<Value>,
//                       Option<TxId>);

// impl Components {
//   pub fn empty() -> Self {
//     Components(None, None, None, None)
//   }

//   pub fn e(e: EntityId) -> Self {
//     Components(Some(e), None, None, None)
//   }

//   pub fn ea(e: EntityId, a: Attribute) -> Self {
//     Components(Some(e), Some(a), None, None)
//   }

//   pub fn eav(e: EntityId, a: Attribute, v: Value) -> Self {
//     Components(Some(e), Some(a), Some(v), None)
//   }

//   pub fn eavt(e: EntityId, a: Attribute, v: Value, t: TxId) -> Self {
//     Components(Some(e), Some(a), Some(v), Some(t))
//   }

//   pub fn matches(&self, datom: &Datom) -> bool {
//     let &Components(e, a, ref v, t) = self;

//     let e = e.is_none() || e.unwrap() == datom.entity;
//     let a = a.is_none() || a.unwrap() == datom.attribute;
//     let v = v.is_none() || v.as_ref().unwrap() == &datom.value;
//     let t = t.is_none() || t.unwrap() == datom.tx;

//     return e && a && v && t;;
//   }
// }

pub mod attr {
  #![allow(non_upper_case_globals)]
  use super::{Attribute, EntityId};
  pub const id:    Attribute = Attribute(EntityId(10));
  pub const ident: Attribute = Attribute(EntityId(11));
  pub const doc:   Attribute = Attribute(EntityId(12));
  pub const tx_instant:   Attribute = Attribute(EntityId(13));
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

  let tx_instant = Datom {
    entity: attr::tx_instant.0,
    attribute: attr::ident,
    value: "db/tx_instant".into(),
    tx: EntityId(0),
    status: Status::Added
  };

  let datoms = vec![
    id,
    ident,
    doc,
    tx_instant
  ];

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
    // TODO: Use Index's impl
    let n = self.datoms(IndexType::Eavt).unwrap() // TODO
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
      status: Status::Added
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
        Operation::Assertion(eid, a, v)       => (eid,        a, v, Status::Added),
        Operation::Retraction(eid, a, v)      => (eid,        a, v, Status::Retracted(tx_eid)),
        Operation::TempidAssertion(tid, a, v) => (eids[&tid], a, v, Status::Added)
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

  fn datoms<'a, I: Into<Index>>(&'a self, index: I) -> Result<Datoms<'a>, Error>;

  fn entity<'a>(&'a self, entity: EntityId) -> Result<Entity<'a, Self>, Error> {
    let datoms = self.datoms(IndexType::Eavt.e(entity))?;
    let mut attrs: BTreeMap<Attribute, BTreeSet<&Datom>> = BTreeMap::new();

    for d in datoms.iter() {
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
    self.datoms(IndexType::Avet.a(attr::ident).v(Value::Str(attribute_name.into())))
      .unwrap()
      .iter().next()
      .map(|d| Attribute::new(d.entity))
  }

  fn attribute_name<'a>(&'a self, attribute: Attribute) -> Option<String> {
    self.datoms(IndexType::Avet.e(attribute.0).a(attr::ident)).unwrap()
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
