#![feature(conservative_impl_trait)]
#![feature(btree_range, collections_bound)]

#[cfg(test)]
extern crate rand;

mod index;

pub type EntityId  = u64;
pub type Attribute = String;        // TODO
pub type Value     = String;        // TODO
pub type TxId      = u64;

#[derive(Debug, Default, PartialEq, Eq, Clone, PartialOrd, Ord)]
pub struct Datom {
  pub entity:    EntityId,
  pub attribute: Attribute,
  pub value:     Value,
  pub tx:        TxId,
  pub added:     bool,
}

type TempId = EntityId;
type Fact = (TempId, Attribute, Value, bool); // TODO: added

#[derive(Debug, Default)]
struct DB {
  last_eid: EntityId,
  eavt: index::EavtIndex,
  aevt: index::AevtIndex,
  log:  index::LogIndex,
}

type Entity<'a> = Vec<&'a Datom>;

impl DB {
  pub fn transact<T: Into<Fact>>(&mut self, data: &[T]) -> TxId {
    unimplemented!()
  }

  pub fn entity<'a>(&'a self, entity: EntityId) -> Entity<'a> {
    let datoms = {
      let mut aevt_bounds = self.aevt.datom_bounds();

      let (mut start, mut end) = self.eavt.datom_bounds();
      start.entity = entity;
      end.entity = entity;

      // Remove datoms where the last entry isn't `added`
      self.eavt.datom_range(&start, &end)
        .filter(|datom| datom.added)
        .filter(|datom| {
          aevt_bounds.0.entity = entity;
          aevt_bounds.1.entity = entity;
          self.aevt.datom_range(&aevt_bounds.0, &aevt_bounds.1)
            .next_back().map(|e| e.added).unwrap_or(true)
        }).collect()
    };

    datoms
  }
}

#[cfg(test)]
mod test_data;

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  #[ignore]
  fn test_transact() {
    unimplemented!()
  }
  
  #[test]
  #[ignore]
  fn test_entity_api() {
    unimplemented!();
  }

  #[test]
  #[ignore]
  fn test_eavt_entity_api_added_false() {
    unimplemented!();
  }
}
