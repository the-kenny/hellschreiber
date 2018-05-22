use super::*;

use std::fmt;

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
  fn to_operation<D: Db>(&self, db: &D) -> Result<Operation, UnknownAttributeError>;
}

impl<'a, V, A> ToOperation for &'a (Assert, TempId, A, V)
  where V: Into<Value> + Clone, A: ToAttribute {
  fn to_operation<D: Db>(&self, db: &D) -> Result<Operation, UnknownAttributeError> {
    if let Some(a) = self.2.to_attribute(db) {
      Ok(Operation::TempidAssertion(self.1, a, self.3.clone().into()))
    } else {
      Err(UnknownAttributeError)
    }
  }
}

impl<'a, V, A> ToOperation for &'a (Assert, EntityId, A, V)
  where V: Into<Value> + Clone, A: ToAttribute {
  fn to_operation<D: Db>(&self, db: &D) -> Result<Operation, UnknownAttributeError> {
    if let Some(a) = self.2.to_attribute(db) {
      Ok(Operation::Assertion(self.1, a, self.3.clone().into()))
    } else {
      Err(UnknownAttributeError)
    }
  }
}

impl<'a, V, A> ToOperation for &'a (Retract, EntityId, A, V)
  where V: Into<Value> + Clone, A: ToAttribute {
  fn to_operation<D: Db>(&self, db: &D) -> Result<Operation, UnknownAttributeError> {
    if let Some(a) = self.2.to_attribute(db) {
      Ok(Operation::Retraction(self.1, a, self.3.clone().into()))
    } else {
      Err(UnknownAttributeError)
    }
  }
}
