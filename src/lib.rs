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
pub use transaction::{Assert, Retract, Operation, TransactionError, TransactionData};

mod entity;
pub use entity::Entity;

mod value;
pub use value::Value;

mod sqlite;
pub use sqlite::Db;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic;

#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EntityId(i64);

pub type TxId = EntityId;

#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord, Hash)]
pub struct Attribute(EntityId);
pub type AttributeName = String;

impl Attribute {
    fn is_internal(&self) -> bool {
        let x = *self;

        x == attr::id
            || x == attr::ident
            || x == attr::doc
            || x == attr::cardinality_many
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord, Hash)]
pub enum Status {
    Asserted,
    Retracted(EntityId)
}

impl Status {
    fn is_assertion(&self) -> bool {
        *self == Status::Asserted
    }

    fn is_retraction(&self) -> bool {
        match self {
            Status::Retracted(_) => true,
            _ => false
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, PartialOrd, Ord, Hash)]
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
    #[allow(unused)]
    fn contains(&self, eid: EntityId) -> bool {
        let i = *self as i64;
        (i & eid.0) == i
    }
}

#[derive(Debug)]
pub struct AttributeInfo {
    pub cardinality_many: bool,
}

#[allow(unused)]
macro_rules! test_impls {
    ( $placeholder:ident, $fns:tt ) => {
        test_impls!([(sqlite,  ::Db::new().unwrap())],
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
    mod usage;
}
