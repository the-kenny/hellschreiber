use super::{Db, EntityId, Attribute, Value, ToAttribute};

use std::{fmt, ops};
use std::collections::BTreeMap;

#[allow(dead_code)]
pub struct Entity<'a, D: Db + 'a> {
  pub db: &'a D,
  pub eid: EntityId,
  pub values: BTreeMap<Attribute, Vec<Value>>,
}

impl<'a, D: Db> Entity<'a, D> {
  pub fn get<A: ToAttribute>(&'a self, attribute: A) -> Option<&'a Value> {
    self.get_many(attribute).next()
  }

  pub fn get_many<A: ToAttribute>(&'a self, attribute: A) -> impl Iterator<Item=&Value> {
    attribute.to_attribute(self.db)
      .and_then(|attribute| self.values.get(&attribute))
      .map(|x| x.iter())
      .unwrap_or(EMPTY_VEC.iter())
  }
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
  type Output = Value;
  fn index(&self, idx: &'a str) -> &Self::Output {
    self.get(idx).unwrap()
  }
}
