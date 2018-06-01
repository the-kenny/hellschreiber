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
    #[fail(display = "Tried to transact fact for attribute without db/ident")]
    NonIdentAttributeTransacted,
    #[fail(display = "Tried to transact new value ({}) for existing db/ident attribute {}", _0, _1)]
    ChangingIdentAttribute(String, String),
    // TODO: Error for setting db.cardinality/many on db/ident
}

#[derive(Debug)]
pub enum Operation {
    Assertion(EntityId, Attribute, Value),
    Retraction(EntityId, Attribute, Value),
    TempidAssertion(TempId, Attribute, Value)
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

pub trait ToOperation {
    // TOOD: no_doc
    fn to_operation(&self, db: &Db) -> Result<Operation, UnknownAttributeError>;
}

impl<'a, V, A> ToOperation for &'a (Assert, TempId, A, V)
    where V: Into<Value> + Clone, A: ToAttribute {
    fn to_operation(&self, db: &Db) -> Result<Operation, UnknownAttributeError> {
        if let Some(a) = self.2.to_attribute(db) {
            Ok(Operation::TempidAssertion(self.1, a, self.3.clone().into()))
        } else {
            Err(UnknownAttributeError)
        }
    }
}

impl<'a, V, A> ToOperation for &'a (Assert, EntityId, A, V)
    where V: Into<Value> + Clone, A: ToAttribute {
    fn to_operation(&self, db: &Db) -> Result<Operation, UnknownAttributeError> {
        if let Some(a) = self.2.to_attribute(db) {
            Ok(Operation::Assertion(self.1, a, self.3.clone().into()))
        } else {
            Err(UnknownAttributeError)
        }
    }
}

impl<'a, V, A> ToOperation for &'a (Retract, EntityId, A, V)
    where V: Into<Value> + Clone, A: ToAttribute {
    fn to_operation(&self, db: &Db) -> Result<Operation, UnknownAttributeError> {
        if let Some(a) = self.2.to_attribute(db) {
            Ok(Operation::Retraction(self.1, a, self.3.clone().into()))
        } else {
            Err(UnknownAttributeError)
        }
    }
}
