#[cfg(test)] extern crate rand;
#[macro_use] extern crate derive_more;
#[macro_use] extern crate lazy_static;
#[macro_use] extern crate serde_derive;
extern crate chrono;
extern crate rusqlite;
extern crate serde;
extern crate serde_json;
#[macro_use] extern crate failure;

mod index;
pub use index::*;

mod transaction;
pub use transaction::{Assert, Retract, Operation, ToOperation, TransactionError, TransactionData};

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
    fn is_internal(&self) -> bool {
        let x = *self;

        x == attr::id
            || x == attr::ident
            || x == attr::doc
            || x == attr::cardinality_many
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
    pub const id:               Attribute = Attribute(EntityId(10));
    pub const ident:            Attribute = Attribute(EntityId(11));
    pub const doc:              Attribute = Attribute(EntityId(12));
    pub const tx_instant:       Attribute = Attribute(EntityId(13));
    pub const cardinality_many: Attribute = Attribute(EntityId(14));
}

fn seed_datoms() -> Datoms<'static> {
    [(attr::id,               "db/id"),
     (attr::ident,            "db/ident"),
     (attr::doc,              "db/doc"),
     (attr::tx_instant,       "db/tx_instant"),
     (attr::cardinality_many, "db.cardinality/many"),
    ].iter()
     .map(|(attr, ident)| {
         Datom {
             entity: attr.0,
             attribute: attr::ident,
             value: Value::Str(ident.to_string()),
             tx: EntityId(0),
             status: Status::Asserted,
         }
     }).collect::<Vec<Datom>>()
}

lazy_static! {
    static ref LATEST_TEMPID: atomic::AtomicIsize  = 100.into();
}

pub fn tempid() -> TempId {
    let i = LATEST_TEMPID.fetch_add(1, atomic::Ordering::SeqCst);
    TempId(i as i64)
}

#[repr(u64)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Partition {
    Db   = 2 << 10,
    Tx   = 2 << 32,
    User = 2 << 48,
}

impl Partition {
    fn contains(&self, eid: EntityId) -> bool {
        let i = *self as i64;
        (i & eid.0) == i
    }
}

#[derive(Debug)]
pub struct AttributeInfo {
    pub cardinality_many: bool,
}

// TODO: Add `is_initialized?` and `initialize`
pub trait Db: Sized {
    #[cfg(test)]
    fn all_datoms<'a>(&'a self) -> Datoms<'a>;

    fn highest_eid(&self, partition: Partition) -> EntityId {
        // TODO: Use FilteredIndex's impl
        let n = self.datoms(Index::Eavt).unwrap() // TODO
            .into_iter()
            .filter(|d| partition.contains(d.entity))
            .last()
            .map(|datom| datom.entity.0)
            .unwrap_or(0);

        EntityId(std::cmp::max(n, partition as i64))
    }

    fn tempid(&mut self) -> TempId {
        tempid()
    }

    fn transact<O: ToOperation, I: IntoIterator<Item=O>>(&mut self, tx: I) -> Result<TransactionData, Error> {
        let tx_eid = EntityId(self.highest_eid(Partition::Tx).0 + 1);

        let now = chrono::Utc::now();

        let mut datoms = vec![Datom {
            entity:    tx_eid,
            attribute: attr::tx_instant,
            value:     Value::DateTime(now),
            tx:        tx_eid,
            status:    Status::Asserted
        }];

        let tx = tx.into_iter()
            .map(|op| op.to_operation(self))
            .map(|op| op.map_err(|_| TransactionError::NonIdentAttributeTransacted))
            .collect::<Result<Vec<Operation>, TransactionError>>()?;

        datoms.reserve(tx.len());

        let eids = {
            let mut eids = BTreeMap::new();
            let mut highest_eid = self.highest_eid(Partition::User).0;
            let mut highest_db_eid = self.highest_eid(Partition::Db).0;

            for operation in &tx {
                if let Operation::TempidAssertion(tempid, attribute, _) = operation {
                    eids.entry(*tempid)
                        .or_insert_with(|| {
                            // If we're asserting an internal attribute (db/id,
                            // db/ident, db/doc, db.cardinality/many) we use the Db
                            // partition
                            if attribute.is_internal() {
                                highest_db_eid += 1;
                                EntityId(highest_db_eid)
                            } else {
                                highest_eid += 1;
                                EntityId(highest_eid)
                            }
                        });
                }
            }
            eids
        };

        for operation in tx {
            let (e, a, v, status) = match operation {
                Operation::Assertion(eid, a, v)       => (eid,        a, v, Status::Asserted),
                Operation::Retraction(eid, a, v)      => (eid,        a, v, Status::Retracted(tx_eid)),
                Operation::TempidAssertion(tid, a, v) => (eids[&tid], a, v, Status::Asserted)
            };

            if self.attribute_name(a).is_none() {
                return Err(TransactionError::NonIdentAttributeTransacted.into())
            }

            // If the operation is an assertion we have to handle the following things:
            //
            // - If the datom isn't db.cardinality/many we have to generate a retraction for the previous value
            //
            // - If the attribute of this datom is `db/ident` we have to make sure it isn't changing the schema
            //
            if status == Status::Asserted {
                if let Some(previous_datom) = self.datoms(Index::Eavt.e(e).a(a)).unwrap().iter().next() {
                    // Prevent database schema changes
                    if a == attr::ident && v != previous_datom.value {
                        let old_attribute_name = previous_datom.value.as_string().unwrap();
                        let new_attribute_name = v.as_string().unwrap();
                        return Err(TransactionError::ChangingIdentAttribute(old_attribute_name, new_attribute_name).into())
                    }

                    // Handle db.cardinality/many
                    let attribute_info = self.attribute_info(a)?;

                    if !attribute_info.cardinality_many {
                        let retraction = Datom {
                            entity: e,
                            attribute: a,
                            value: previous_datom.value.clone(),
                            tx: tx_eid,
                            status: Status::Retracted(tx_eid)
                        };

                        datoms.push(retraction);
                    }
                }
            }

            let datom = Datom {
                entity: e,
                attribute: a,
                value: v.clone(),

                tx: tx_eid,
                status
            };

            datoms.push(datom);
        }

        self.store_datoms(&datoms)?;

        Ok(TransactionData {
            tx_id: tx_eid,
            tempid_mappings: eids,
        })
    }

    fn datoms<I: Into<FilteredIndex>>(&self, index: I) -> Result<Datoms, Error>;

    fn entity(&self, entity: EntityId) -> Result<Entity<Self>, Error> {
        let datoms = self.datoms(Index::Eavt.e(entity))?;
        let mut attrs: BTreeMap<Attribute, BTreeSet<&Datom>> = BTreeMap::new();

        for datoms in &datoms {
            let entry = attrs.entry(datoms.attribute)
                .or_insert_with(BTreeSet::new);

            match datoms.status {
                Status::Asserted => {
                    entry.insert(&datoms);
                },
                Status::Retracted(_) if entry.contains(&datoms) => {
                    entry.remove(&datoms);
                },
                Status::Retracted(_) => {
                    panic!("Got retraction for non-existing value. Retraction: {:?}", datoms)
                }
            }
        }

        // Assert all datoms are of the same entity
        assert!(attrs.values().flat_map(|x| x).all(|datoms| datoms.entity == entity));

        let values = attrs.into_iter()
            .map(|(a, ds)| {
                let datoms: Vec<_> = ds.into_iter()
                    .map(|datoms| datoms.value.clone())
                    .collect();
                (a, datoms)
            }).collect::<BTreeMap<Attribute, Vec<Value>>>();

        let entity = Entity {
            db: self,
            eid: entity,
            values,
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
            .map(|d| Attribute(d.entity))
    }

    fn attribute_name(&self, attribute: Attribute) -> Option<String> {
        self.datoms(Index::Avet.e(attribute.0).a(attr::ident)).unwrap()
            .into_iter()
            .next()
            .and_then(|d| match d.value {
                Value::Str(ref s) => Some(s.clone()),
                _ => None
            })
    }

    fn attribute_info<A: ToAttribute>(&self, attribute: A) -> Result<AttributeInfo, Error> {
        let mut info = AttributeInfo {
            cardinality_many: false
        };

        let attribute_eid = attribute.to_attribute(self).unwrap().0;
        let attribute_datoms = self.datoms(Index::Eavt.e(attribute_eid))?;
        for datom in attribute_datoms {
            match datom.attribute {
                attr::cardinality_many => {
                    info.cardinality_many = datom.value != Value::Bool(false)
                },
                _ => ()
            }
        }

        Ok(info)
    }
}


#[allow(unused)]
macro_rules! test_impls {
    ( $placeholder:ident, $fns:tt ) => {
        test_impls!([(test_db, ::tests::in_memory::TestDb::new()),
                     (sqlite,  ::SqliteDb::new().unwrap())],
                    $placeholder,
                    $fns);
    };

    ([ $( ($mod:ident, $db:expr) ),* ], $placeholder:ident, $fns:tt) => {
        $(
            mod $mod {
                test_impls!($db, $placeholder, $fns);
            }
        )*
    };

    ( $db:expr, $placeholder:ident, { $( $fns:item )* }) => {
        $(
            #[allow(unused)]
            macro_rules! $placeholder { () => { $db } }
            $fns
        )*
    };
}

#[cfg(test)]
pub mod tests {
    mod db;
    mod data;
    mod in_memory;
    mod usage;

    // TODO: Move to separate module
    #[test]
    fn test_db_equality() {
        use ::*;

        let mut db1 = in_memory::TestDb::new();
        let mut db2 = ::SqliteDb::new().unwrap();

        db1.store_datoms(&data::make_test_data()).unwrap();
        db2.store_datoms(&data::make_test_data()).unwrap();

        assert_eq!(db1.all_datoms(), db2.all_datoms(),
                   "Equality of db1 and db2 for db.all_datoms()");

        use ::tests::data::person_name;
        for idx in [Index::Eavt.into(),
                    Index::Eavt.e(EntityId(1)),
                    Index::Eavt.e(EntityId(999)),
                    Index::Eavt.a(person_name)].iter() {

            assert_eq!(db1.datoms(idx.clone()).unwrap(),
                       db2.datoms(idx.clone()).unwrap(),
                       "Equality of db1 and db2 for the {:?} index", idx);
        }
    }
}
