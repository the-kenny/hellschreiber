use ::*;

#[derive(Debug)]
pub struct TestDb(Vec<Datom>);

impl TestDb {
  pub fn new() -> Self {
    TestDb(vec![])
  }
}

impl Db for TestDb {
  fn transact<T: Into<Fact>>(&mut self, _tx: &[T]) -> TxId {
    unimplemented!()
  }

  fn store_datoms(&mut self, datoms: &[Datom]) {
    self.0.clear();
    self.0.extend_from_slice(datoms);
  }

  #[cfg(test)]
  fn all_datoms<'a>(&'a self) -> Datoms<'a> {
    let mut datoms = self.0.clone();
    datoms.sort_by_key(|d| (d.tx, d.attribute, d.status));
    Cow::Owned(datoms)
  }

  fn datoms<'a, C: Borrow<Components>>(&'a self, index: Index, components: C) -> Datoms {
    let mut raw_datoms = self.0.clone();
    raw_datoms.retain(|d| components.borrow().matches(&d));
    raw_datoms.sort_by_key(|d| d.tx);

    #[derive(Debug)]
    struct EavEquality(Datom);

    impl PartialEq for EavEquality {
      fn eq(&self, rhs: &EavEquality) -> bool {
        let EavEquality(ref lhs) = *self;
        let EavEquality(ref rhs) = *rhs;
        lhs.entity == rhs.entity
          && lhs.attribute == rhs.attribute
          && lhs.value == rhs.value
      }
    }
    impl Eq for EavEquality {}

    use std::cmp;
    impl Ord for EavEquality {
      fn cmp(&self, o: &EavEquality) -> cmp::Ordering {
        self.0.entity.cmp(&o.0.entity)
          .then(self.0.attribute.cmp(&o.0.attribute))
          .then(self.0.value.cmp(&o.0.value))
      }
    }
    impl PartialOrd for EavEquality {
      fn partial_cmp(&self, o: &EavEquality) -> Option<cmp::Ordering> {
        Some(self.cmp(&o))
      }
    }

    use std::collections::BTreeSet;
    let mut datoms: BTreeSet<EavEquality> = Default::default();

    for d in raw_datoms.into_iter() {
      let d = EavEquality(d);
      match d.0.status {
        Status::Added => {
          datoms.insert(d);
        },
        Status::Retracted(_) if datoms.contains(&d) => {
          datoms.remove(&d);
        },
        Status::Retracted(_) => {
          unreachable!("Tried to retract non-existing datum: {:?} (have: {:?})", d.0, datoms)
        }
      }
    }

    let mut datoms = datoms.into_iter()
      .map(|EavEquality(d)| d)
      .collect::<Vec<Datom>>();

    datoms.sort_by(|l,r| {
      use std::cmp::Ordering;
      macro_rules! cmp {
        ($i:ident) => (l.$i.cmp(&r.$i));
        ($($i:ident),*) => {
          [$(cmp!($i)),*].into_iter().fold(Ordering::Equal, |o, x| o.then(*x))

        };
      }

      match index {
        Index::Eavt => cmp!(entity, tx, attribute, value),
      }
    });

    Cow::Owned(datoms)
  }
}


pub fn validate_datoms(datoms: &[Datom]) {
  use std::collections::{BTreeMap,BTreeSet};
  // TODO: This logic is duplicated in `Db::entity`
  let mut values: BTreeMap<(EntityId, Attribute), BTreeSet<&Value>> = BTreeMap::new();
  for d in datoms {
    let mut entry = values.entry((d.entity, d.attribute))
      .or_insert_with(|| BTreeSet::new());

    match d.status {
      Status::Added => {
        entry.insert(&d.value);
      },
      Status::Retracted(_) => {
        if !entry.contains(&d.value) {
          panic!("Found Retraction on non-existing value: {:?}", d);
        } else {
          entry.remove(&d.value);
        }
      }
    }
  }

  // TODO: Add more tests
}

#[test]
fn test_datom_test_set() {
  let datoms = tests::data::make_test_data();
  validate_datoms(&datoms);
}

#[test]
#[should_panic]
fn test_invalid_datom_set() {
  let mut datoms = tests::data::make_test_data();

  // Clone last added datom, make it a retraction, change its value
  let mut retraction = datoms.iter().filter(|d| d.status == Status::Added).last().unwrap().clone();
  retraction.status = Status::Retracted(retraction.tx);
  retraction.value = Value::Str("xxxxxxxxxx".into());
  datoms.push(retraction);

  validate_datoms(&datoms);
}

#[test]
pub fn test_components() {
  let d = Datom {
    entity:    EntityId(42),
    attribute: Attribute(EntityId(1)),
    value:     Value::Int(23),
    tx:        TxId(EntityId(10)),
    status:    Status::Added,
  };

  assert_eq!(true, Components(None, None, None, None).matches(&d));

  assert_eq!(true,  Components(Some(d.entity), None,              None,                  None).matches(&d));
  assert_eq!(true,  Components(None,           Some(d.attribute), None,                  None).matches(&d));
  assert_eq!(true,  Components(None,           None,              Some(d.value.clone()), None).matches(&d));
  assert_eq!(true,  Components(None,           None,              None,                  Some(d.tx)).matches(&d));
  assert_eq!(true,  Components(Some(d.entity), Some(d.attribute), None,                  None).matches(&d));
  assert_eq!(true,  Components(Some(d.entity), Some(d.attribute), Some(d.value.clone()), None).matches(&d));
  assert_eq!(true,  Components(Some(d.entity), Some(d.attribute), Some(d.value.clone()), Some(d.tx)).matches(&d));

  assert_eq!(false, Components(Some(EntityId(999)), None, None, None).matches(&d));
  assert_eq!(false, Components(None, Some(Attribute(EntityId(999))), None, None).matches(&d));
  assert_eq!(false, Components(None, None, Some(Value::Int(1000)),None).matches(&d));
  assert_eq!(false, Components(None, None, None, Some(TxId(EntityId(999)))).matches(&d));
}


pub fn test_entity<D: Db>(mut db: D) {
  use tests::data::*;

  db.store_datoms(&tests::data::make_test_data());
  validate_datoms(&db.all_datoms());

  assert_eq!(db.entity(EntityId(99999)).values.len(), 0);

  let heinz = db.entity(EntityId(1)).values;
  assert_eq!(heinz.len(), 2);
  assert_eq!(heinz.get(&person_name), Some(&vec![Value::Str("Heinz".into())]));
  assert_eq!(heinz.get(&person_age), Some(&vec![Value::Int(42)]));
  assert_eq!(heinz.get(&album_name), None);

  let karl  = db.entity(EntityId(2)).values;
  assert_eq!(karl.len(), 2);
  assert_eq!(karl[&person_name], vec![Value::Str("Karl".into())]);
  assert_eq!(karl[&person_children], vec![Value::Str("Philipp".into()),
                                          Value::Str("Jens".into())]);

  let nevermind = db.entity(EntityId(3)).values;
  assert_eq!(nevermind.len(), 1);
  assert_eq!(nevermind.get(&tests::data::album_name), Some(&vec![Value::Str("Nevermind".into())]));
}

pub fn test_datoms<D: Db>(mut db: D) {
  db.store_datoms(&tests::data::make_test_data());

  let pn = tests::data::person_name;
  let pa = tests::data::person_age;
  let an = tests::data::album_name;
  let pc = tests::data::person_children;

  let heinz     = EntityId(1);
  let karl      = EntityId(2);
  let nevermind = EntityId(3);

  let eavt = db.datoms(Index::Eavt, Components::empty());
  assert_eq!(eavt.iter().map(|d| (d.attribute, d.entity)).collect::<Vec<_>>(),
             vec![(pn, heinz),
                  (pa, heinz),
                  (pn, karl),
                  (pc, karl),
                  (pc, karl),
                  (an, nevermind)]);

  // None
  let eavt = db.datoms(Index::Eavt, Components(Some(EntityId(99999)), None, None, None));
  assert!(eavt.iter().map(|d| d.entity).collect::<Vec<_>>().is_empty());

  // Heinz
  let eavt = db.datoms(Index::Eavt, Components(Some(heinz), None, None, None));
  assert_eq!(eavt.iter().map(|d| d.entity).collect::<Vec<_>>(),
             vec![heinz,heinz]);
  assert_eq!(eavt.iter().map(|d| d.attribute).collect::<Vec<_>>(),
             vec![pn, pa]);

  // Heinz, just person/age
  let eavt = db.datoms(Index::Eavt, Components(Some(heinz), Some(pa), None, None));
  assert_eq!(eavt.iter().map(|d| d.entity).collect::<Vec<_>>(),
             vec![heinz]);
  assert_eq!(eavt.iter().map(|d| d.attribute).collect::<Vec<_>>(),
             vec![pa]);

  // Nevermind
  let eavt = db.datoms(Index::Eavt, Components(Some(nevermind), None, None, None));
  assert_eq!(eavt.iter().map(|d| d.entity).collect::<Vec<_>>(),
             vec![nevermind]);
  assert_eq!(eavt.iter().map(|d| d.attribute).collect::<Vec<_>>(),
             vec![an]);

  // Nevermind, person/age
  let eavt = db.datoms(Index::Eavt, Components(Some(nevermind), Some(pa), None, None));
  assert_eq!(eavt.iter().map(|d| d.entity).collect::<Vec<_>>(),
             vec![]);
  assert_eq!(eavt.iter().map(|d| d.attribute).collect::<Vec<_>>(),
             vec![]);

}

pub fn test_db_equality<D: Db, E: Db>(mut db1: D, mut db2: E) {
  db1.store_datoms(&tests::data::make_test_data());
  db2.store_datoms(&tests::data::make_test_data());

  /*
  for (a,b) in db1.all_datoms().iter().zip(db2.all_datoms().iter()) {
    println!("lhs: {:?}", a);
    println!("rhs: {:?}", b);
    println!("==========================");
  }
   */


  assert_eq!(db1.all_datoms(), db2.all_datoms());

  use ::tests::data::person_name;
  for &idx in [Index::Eavt].into_iter() {
    for c in [Components(None,                None,              None, None),
              Components(Some(EntityId(1)),   None,              None, None),
              Components(Some(EntityId(999)), None,              None, None),
              Components(None,                Some(person_name), None, None)].into_iter() {

      assert_eq!(db1.datoms(idx, c),
                 db2.datoms(idx, c));
    }
  }
}
