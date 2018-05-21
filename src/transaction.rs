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

impl<'a, V, A> ToOperation for &'a (Assert, TempId, A, V)
  where V: Into<Value> + Clone, A: ToAttribute {
  fn to_operation<D: Db>(&self, db: &D) -> Operation {
    let a = self.2.to_attribute(db).expect("Unknown attribute in transaction");
    Operation::TempidAssertion(self.1, a, self.3.clone().into())
  }
}

impl<'a, V, A> ToOperation for &'a (Assert, EntityId, A, V)
  where V: Into<Value> + Clone, A: ToAttribute {
  fn to_operation<D: Db>(&self, db: &D) -> Operation {
    let a = self.2.to_attribute(db).expect("Unknown attribute in transaction");
    Operation::Assertion(self.1, a, self.3.clone().into())
  }
}

impl<'a, V, A> ToOperation for &'a (Retract, EntityId, A, V)
  where V: Into<Value> + Clone, A: ToAttribute {
  fn to_operation<D: Db>(&self, db: &D) -> Operation {
    let a = self.2.to_attribute(db).expect("Unknown attribute in transaction");
    Operation::Retraction(self.1, a, self.3.clone().into())
  }
}
