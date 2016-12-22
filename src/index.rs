use super::{Datom, EntityId};

use std::collections::*;

use std::cmp;
pub trait Indexable: AsRef<Datom> + Ord + From<Datom> {}

macro_rules! index_member {
  ($t:ident, ( $($field:ident),*)) => {
    impl Indexable for $t {}

    impl AsRef<Datom> for $t {
      fn as_ref<'a>(&'a self) -> &'a Datom {
        &self.0
      }
    }

    impl From<Datom> for $t {
      fn from(other: Datom) -> $t {
        $t(other)
      }
    }

    impl Ord for $t {
      fn cmp(&self, other: &Self) -> cmp::Ordering {
        let ref x = ( $(&(&self.0).$field,)* );
        let ref y = ( $(&(&other.0).$field,)* );

        match x.cmp(&y) {
          cmp::Ordering::Equal => self.0.cmp(&other.0),
          x => x
        }
      }
    }

    // Order first by $field, then order by "normal" Datom order
    impl PartialOrd for $t {
      fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
      }
    }

    impl Eq for $t {}

    impl PartialEq for $t {
      fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
      }
    }
  }
}

#[derive(Debug)]
#[doc(hidden)]
pub struct EAVT(Datom);
index_member!(EAVT, (entity, attribute, value, tx, added));

#[derive(Debug)]
#[doc(hidden)]
pub struct AEVT(Datom);
index_member!(AEVT, (attribute, entity, value, tx, added));

#[derive(Debug)]
#[doc(hidden)]
pub struct Log(Datom);
index_member!(Log, (tx, entity, attribute, value, added));

#[derive(Debug)]
pub struct Index<T: Indexable> {
  datoms: BTreeSet<T>,
  // We need to track upper datom bound for correct BTreeSet::range
  // access
  upper_bound: Datom,
}

impl<T: Indexable> Default for Index<T> {
  fn default() -> Self {
    Index {
      upper_bound: Datom::default(),
      datoms: Default::default(),
    }
  }
}

impl<T: Indexable> Index<T> {
  pub fn all_datoms<'a>(&'a self) -> impl Iterator<Item=&'a Datom> {
    self.datoms.iter().map(|x| x.as_ref())
  }

  fn update_bounds(&mut self, datom: &Datom) {
    macro_rules! check {
      ($name:ident) => {
        if self.upper_bound.$name < datom.$name {
          self.upper_bound.$name = datom.$name.clone();
        }
      }
    }

    check!(entity);
    check!(attribute);
    check!(value);
    check!(tx);
    check!(added);
  }

  pub fn insert(&mut self, datom: &Datom) {
    self.update_bounds(datom);
    self.datoms.insert(datom.clone().into());
  }

  fn datom_bounds(&self) -> (Datom, Datom) {
    (Datom::default(), self.upper_bound.clone())
  }
  
  fn datom_range<'a>(&'a self, start: Datom, end: Datom) -> impl Iterator<Item=&'a Datom> {
    use std::collections::Bound::Included;
    self.datoms.range(Included(&start.into()), Included(&end.into()))
      .map(|d| d.as_ref())
      .filter(|d| d.added == true)
  }
}

pub type EavtIndex = Index<EAVT>;
pub type AevtIndex = Index<AEVT>;
pub type LogIndex  = Index<Log>;

impl EavtIndex {
  pub fn entity<'a>(&'a self, e: EntityId) -> impl Iterator<Item=&'a Datom> {
    let (mut start, mut end) = self.datom_bounds();

    start.entity = e;
    end.entity = e;

    self.datom_range(start, end)
  }

}

// TODO: AVET, VAET

// VAET is used for navigating relations backwards and stores all
// datoms with reference attributes. Given VAET, you can not only find
// out whom John follows (“John” :follows ?x), but also efficiently
// lookup who follows John (?x :follows “John”).

// AVET provides efficient lookup by value and stores datoms with
// attributes marked as unique or index in schema. Attributes of this
// kind are good for external ids. AVET is the most problematic index
// in practice, and it’s better if you can manage to put monotonic
// values in it, or use it sparingly.

#[cfg(test)]
mod tests {
  use super::*;

  fn make_datoms() -> Vec<Datom> {
    let d1 = Datom {
      entity: 1,
      attribute: "person/name".into(),
      value: "Heinz".into(),
      tx: 2,
      added: true,
    };
    let d2 = Datom {
      entity: 1,
      attribute: "person/age".into(),
      value: "42".into(),
      tx: 2,
      added: true,
    };
    let d3 = Datom {
      entity: 2,
      attribute: "person/name".into(),
      value: "Karl".into(),
      tx: 1,
      added: true,
    };

    let d4 = Datom {
      entity: 3,
      attribute: "album/name".into(),
      value: "Nevermind".into(),
      tx: 3,
      added: true,
    };

    use rand::{thread_rng, Rng};
    let mut v = vec![d1, d2, d3, d4];
    thread_rng().shuffle(v.as_mut_slice());
    v
  }

  #[test]
  #[ignore]
  fn test_added_ignoring() {
    unimplemented!()
  }

  #[test]
  fn test_index_sorted() {
    // TODO: Generalize over all index types
    
    let mut eavt = EavtIndex::default();
    for d in make_datoms() { eavt.insert(&d); }

    let entries: Vec<_> = eavt.all_datoms().collect();
    let mut entries2 = entries.clone(); entries2.sort();
    assert_eq!(entries, entries2);

    let eids = entries.iter().map(|d| d.entity).collect::<Vec<_>>();
    let mut eids2 = eids.clone(); eids2.sort();
    assert_eq!(eids, eids2);
  }
  
  #[test]
  fn test_eavt_entity_api() {
    let mut eavt = EavtIndex::default();
    for d in make_datoms() { eavt.insert(&d); }

    assert_eq!(eavt.all_datoms().map(|d| d.entity).collect::<Vec<_>>(),
               vec![1, 1, 2, 3]);

    assert_eq!(eavt.entity(1).map(|d| &d.value[..]).collect::<Vec<_>>(),
               vec!["42", "Heinz"]);
    
    assert_eq!(eavt.entity(2).map(|d| &d.value[..]).collect::<Vec<_>>(),
               vec!["Karl"]);
    
    assert!(eavt.entity(999).collect::<Vec<_>>().is_empty());
  }

  #[test]
  #[ignore]
  fn test_eavt_entity_api_added_false() {
    let mut eavt = EavtIndex::default();
    for d in make_datoms() { eavt.insert(&d); }

    // Get the age datom, then re-insert it with `added: false`
    let mut age = eavt.entity(1).next().unwrap().clone();
    assert_eq!(age.attribute, "person/age");
    age.added = false;
    eavt.insert(&age);
    println!("{:?}", eavt.entity(1).collect::<Vec<_>>());
    assert_eq!(eavt.entity(1).map(|d| &d.value[..]).collect::<Vec<_>>(),
               vec!["Heinz"]);

  }

  #[test]
  fn test_aevt() {
    let mut eavt = AevtIndex::default();
    for d in make_datoms() { eavt.insert(&d); }

    assert_eq!(eavt.all_datoms().map(|d| &d.attribute[..]).collect::<Vec<_>>(),
               vec!["album/name", "person/age","person/name","person/name"]);
  }

  #[test]
  fn test_log() {
    let mut eavt = LogIndex::default();
    for d in make_datoms() { eavt.insert(&d); }

    assert_eq!(eavt.all_datoms().map(|d| d.tx).collect::<Vec<_>>(),
               vec![1,2,2,3]);
  }

}
