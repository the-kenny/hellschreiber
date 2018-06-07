use super::*;

use std::fmt;

/// Struct containing the `tx_id` of a successful transaction and
/// allows mapping from `TempId`s to `EntityId`s.
#[derive(Debug)]
pub struct TransactionData {
    pub tx_id: TxId,
    pub tempid_mappings: BTreeMap<TempId, EntityId>
}

// TODO: Use `String` to describe the attributes
#[derive(Debug, Fail, PartialEq, Eq)]
pub enum TransactionError {
    #[fail(display = "Tried to transact new value ({}) for existing db/ident attribute {}", _0, _1)]
    ChangingIdentAttribute(String, String),
    #[fail(display = "Tried to transact unknown attribute {}", _0)]
    UnknownAttribute(String),
    // TODO: Error for setting db.cardinality/many on db/ident
}

#[derive(Debug)]
pub enum Operation {
    Assertion(EntityId, AttributeName, Value),
    Retraction(EntityId, AttributeName, Value),
    TempidAssertion(TempId, AttributeName, Value)
}

impl Operation {
    pub(crate) fn attribute_name(&self) -> &str {
        match self {
            Operation::Assertion(_, a, _) => a,
            Operation::Retraction(_, a, _) => a,
            Operation::TempidAssertion(_, a, _) => a
        }
    }
}

pub struct Assert;
pub struct Retract;

#[derive(Debug, Fail, PartialEq, Eq)]
pub struct UnknownAttributeError;

impl fmt::Display for UnknownAttributeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Unknown attribute in Transaction")
    }
}

impl<'a, A, V> From<&'a (Assert, TempId, A, V)> for Operation
    where A: Into<AttributeName> + Clone, V: Into<Value> + Clone {
    fn from(o: &'a (Assert, TempId, A, V)) -> Operation {
        Operation::TempidAssertion(o.1, o.2.clone().into(), o.3.clone().into())
    }
}

impl<'a, A, V> From<&'a (Assert, EntityId, A, V)> for Operation
    where A: Into<AttributeName> + Clone, V: Into<Value> + Clone {
    fn from(o: &'a (Assert, EntityId, A, V)) -> Operation {
        Operation::Assertion(o.1, o.2.clone().into(), o.3.clone().into())
    }
}

impl<'a, A, V> From<&'a (Retract, EntityId, A, V)> for Operation
    where A: Into<AttributeName> + Clone,  V: Into<Value> + Clone {
    fn from(o: &'a (Retract, EntityId, A, V)) -> Operation {
        Operation::Retraction(o.1, o.2.clone().into(), o.3.clone().into())
    }
}
