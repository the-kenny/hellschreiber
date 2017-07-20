#[cfg(test)]
extern crate rand;
extern crate rusqlite;

#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
pub struct EntityId(u64);

#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
pub struct Attribute(EntityId);

#[derive(Debug, PartialEq, Eq, Clone, PartialOrd, Ord)]
pub enum Value {
  Str(String),
  Int(i64),
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
pub struct TxId(u64);

#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
pub enum Status {
  Added,
  Retracted
}

#[derive(Debug, PartialEq, Eq, Clone, PartialOrd, Ord)]
pub struct Datom {
  pub entity:    EntityId,
  pub attribute: Attribute,
  pub value:     Value,
  pub tx:        TxId,
  pub status:    Status,
}

type TempId = EntityId;
type Fact   = (TempId, Attribute, Value, Status);

use std::collections::BTreeMap;

type Entity<'a> = BTreeMap<Attribute, &'a Value>;

pub trait Db {
  fn transact<T: Into<Fact>>(&mut self, tx: &[T]) -> TxId;
  fn all_datoms<'a>(&'a self) -> &'a [Datom]; // TODO: impl Iter?
    
  fn entity<'a>(&'a self, entity: EntityId) -> Entity<'a> {
    let mut attrs: BTreeMap<Attribute, (TxId, &'a Datom)> = Default::default();
    let datoms = self.all_datoms().iter().filter(|d| d.entity == entity);
    for d in datoms {
      use std::collections::btree_map::Entry;
      
      let entry = (d.tx, d);
      match attrs.entry(d.attribute) {
        Entry::Vacant(e) => { e.insert(entry); }
        Entry::Occupied(mut e) => {
          let &(db_tx, db_d) = e.get();
          if d.tx > db_tx {
            match d.status {
              // Newer datom was added, replace it
              Status::Added => {
                e.insert(entry);
              },
              // If retracted and the retracted value matches the
              // datom's value, apply retraction
              Status::Retracted if d.value == db_d.value => {
                e.remove();
              },
              // Retraction with wrong value. Ignore. TODO: Log warning
              Status::Retracted => (),
            }
          }
        }
      }
    }
    
    attrs.values().into_iter()
      .map(|&(_, d)| (d.attribute, &d.value))
      .collect()
  }

  fn store_datoms(&mut self, _datoms: &[Datom]) {
    unimplemented!()
  }
}

#[cfg(test)]
mod test_data;

#[cfg(test)]
mod test {
  use super::*;
  
  #[derive(Debug)]
  struct TestDb(Vec<Datom>);

  impl Db for TestDb {
    fn transact<T: Into<Fact>>(&mut self, _tx: &[T]) -> TxId {
      unimplemented!()
    }

    fn store_datoms(&mut self, datoms: &[Datom]) {
      self.0.clear();
      self.0.extend_from_slice(datoms);
    }
    
    fn all_datoms<'a>(&'a self) -> &'a [Datom] {
      &self.0
    }
  }

  #[test]
  fn test_default_impl() {
    test_entity(TestDb(vec![]));
  }

  fn test_entity<D: Db>(mut db: D) {
    db.store_datoms(&test_data::make_test_data());

    assert_eq!(db.entity(EntityId(99999)).len(), 0);
    
    let heinz = db.entity(EntityId(1));
    assert_eq!(heinz.len(), 2);
    assert_eq!(heinz.get(&test_data::person_name), Some(&&Value::Str("Heinz".into())));
    assert_eq!(heinz.get(&test_data::person_age), Some(&&Value::Int(42)));
    assert_eq!(heinz.get(&test_data::album_name), None);

    let karl  = db.entity(EntityId(2));
    assert_eq!(karl.len(), 1);

    let nevermind = db.entity(EntityId(3));
    assert_eq!(nevermind.len(), 1);
    assert_eq!(nevermind.get(&test_data::album_name), Some(&&Value::Str("Nevermind".into())));
  }
}

// #[derive(Debug, Default)]
// struct DB {
//   last_eid: EntityId,
//   eavt: index::EavtIndex,
//   aevt: index::AevtIndex,
//   log:  index::LogIndex,
// }

// type Entity<'a> = Vec<&'a Datom>;

// impl DB {
//   pub fn transact<T: Into<Fact>>(&mut self, data: &[T]) -> TxId {
//     unimplemented!()
//   }

//   pub fn entity<'a>(&'a self, entity: EntityId) -> Entity<'a> {
//     let datoms = {
//       let mut aevt_bounds = self.aevt.datom_bounds();

//       let (mut start, mut end) = self.eavt.datom_bounds();
//       start.entity = entity;
//       end.entity = entity;

//       // Remove datoms where the last entry isn't `added`
//       self.eavt.datom_range(&start, &end)
//         .filter(|datom| datom.added)
//         .filter(|datom| {
//           aevt_bounds.0.entity = entity;
//           aevt_bounds.1.entity = entity;
//           self.aevt.datom_range(&aevt_bounds.0, &aevt_bounds.1)
//             .next_back().map(|karl| karl.added).unwrap_or(true)
//         }).collect()
//     };

//     datoms
//   }
// }

// #[cfg(test)]
// mod test_data;

// #[cfg(test)]
// mod tests {
//   use super::*;

//   #[test]
//   #[ignore]
//   fn test_transact() {
//     unimplemented!()
//   }
  
//   #[test]
//   #[ignore]
//   fn test_entity_api() {
//     unimplemented!();
//   }

//   #[test]
//   #[ignore]
//   fn test_eavt_entity_api_added_false() {
//     unimplemented!();
//   }
// }
