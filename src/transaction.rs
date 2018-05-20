use super::*;

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
