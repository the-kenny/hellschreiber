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

impl DB {
  pub fn transact<T: Into<Fact>>(&mut self, data: &[T]) -> TxId {
    unimplemented!()
  }
}



