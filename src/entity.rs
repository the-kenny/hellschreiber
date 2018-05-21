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

  pub fn get_many<A: ToAttribute>(&'a self, attribute: A) -> impl Iterator<Item=&'a Value> {
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
  fn index(&self, idx: &str) -> &Self::Output {
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

#[cfg(test)]
mod tests {
  use ::*;
  use ::SqliteDb;

  const ONE: EntityId = EntityId(1000);
  const TWO: EntityId = EntityId(1001);

  fn test_db() -> impl Db {
    let mut db = SqliteDb::new().unwrap();
    db.transact(&[(Assert, ONE, "db/ident", Value::Str("foo".to_string()))]).unwrap();
    db.transact(&[(Assert, TWO, "db/ident", Value::Str("bar".to_string()))]).unwrap();
    db.transact(&[(Assert, TWO, "db/ident", Value::Str("baz".to_string()))]).unwrap();
    db
  }

  #[test]
  fn get() {
    let db = test_db();
    assert_eq!(db.entity(ONE).unwrap().get("db/ident"),
               Some(&Value::Str("foo".to_string())));
    assert_eq!(db.entity(TWO).unwrap().get("db/ident"),
                 Some(&Value::Str("bar".to_string())))
  }

  #[test]
  fn get_many() {
    let db = test_db();

    let one = db.entity(ONE).unwrap();
    assert_eq!(one.get_many("db/ident").collect::<Vec<_>>(),
               vec![&Value::Str("foo".to_string())]);

    let two = db.entity(TWO).unwrap();
    assert_eq!(two.get_many("db/ident").collect::<Vec<_>>(),
               vec![&Value::Str("bar".to_string()),
                    &Value::Str("baz".to_string())]);
  }

  #[test]
  fn index() {
    let db = test_db();
    assert_eq!(db.entity(ONE).unwrap()["db/ident"],
               Value::Str("foo".to_string()));

    assert_eq!(db.entity(TWO).unwrap()["db/ident"],
               Value::Str("bar".to_string()));
  }

  #[test]
  #[should_panic]
  fn index_panic() {
    let db = test_db();
    let _ = db.entity(ONE).unwrap()["asdasdf"];
  }
}
