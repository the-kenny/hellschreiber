use ::*;

#[test]
pub fn test_components() {
  let d = Datom {
    entity:    EntityId(42),
    attribute: Attribute(EntityId(1)),
    value:     Value::Int(23),
    tx:        TxId(10),
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
  assert_eq!(false, Components(None, None, None, Some(TxId(999))).matches(&d));
}

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

  fn datoms<'a, C: Borrow<Components>>(&'a self, index: Index, components: C) -> Datoms {
    let mut datoms = self.0.clone();
    datoms.retain(|d| components.borrow().matches(&d));

    datoms.sort_by(|l,r| {
      use std::cmp::Ordering;
      macro_rules! cmp {
        ($i:ident) => (l.$i.cmp(&r.$i));
        ($($i:ident),*) => {
          [$(cmp!($i)),*].into_iter().fold(Ordering::Equal, |o, x| o.then(*x))

        };
      }

      match index {
        Index::Eavt => cmp!(tx, entity, attribute, value),
      }
    });

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

    use std::collections::BTreeMap;
    use std::collections::btree_map::Entry;

    let mut ds: BTreeMap<(EntityId,Attribute), (&Value, TxId, Status)> = BTreeMap::new();

    for d in datoms.iter() {
      let item = (d.entity, d.attribute);

      match ds.entry(item) {
        Entry::Vacant(_) if d.status == Status::Retracted => { }
        Entry::Vacant(e)       => { e.insert((&d.value, d.tx, d.status)); }
        Entry::Occupied(mut e) => {
          let (value, tx, _) = *e.get();
          if d.tx > tx {
            match d.status {
              // Newer datom was added, replace it
              Status::Added => {
                e.insert((&d.value, d.tx, d.status));
              },
              // If retracted and the retracted value matches the
              // datom's value, apply retraction
              Status::Retracted if *value == d.value => {
                e.remove();
              },
              Status::Retracted => {}
            }
          }
        }
      }
    }

    let datoms = ds.into_iter()
      .map(|((e,a),(v, tx, s))| {
        Datom {
          entity: e,
          attribute: a,
          value: v.clone(),
          tx: tx,
          status: s,
        }
      })
      .collect();

    Cow::Owned(datoms)
  }
}

pub fn test_entity<D: Db>(mut db: D) {
  db.store_datoms(&tests::data::make_test_data());

  assert_eq!(db.entity(EntityId(99999)).values.len(), 0);

  let heinz = db.entity(EntityId(1)).values;
  assert_eq!(heinz.len(), 2);
  assert_eq!(heinz.get(&tests::data::person_name), Some(&Value::Str("Heinz".into())));
  assert_eq!(heinz.get(&tests::data::person_age), Some(&Value::Int(42)));
  assert_eq!(heinz.get(&tests::data::album_name), None);

  let karl  = db.entity(EntityId(2)).values;
  assert_eq!(karl.len(), 1);

  let nevermind = db.entity(EntityId(3)).values;
  assert_eq!(nevermind.len(), 1);
  assert_eq!(nevermind.get(&tests::data::album_name), Some(&Value::Str("Nevermind".into())));
}

pub fn test_datoms<D: Db>(mut db: D) {
  db.store_datoms(&tests::data::make_test_data());

  let pn = tests::data::person_name;
  let pa = tests::data::person_age;
  let an = tests::data::album_name;

  let heinz     = EntityId(1);
  let karl      = EntityId(2);
  let nevermind = EntityId(3);

  let eavt = db.datoms(Index::Eavt, Components::empty());
  assert_eq!(eavt.iter().map(|d| d.entity).collect::<Vec<_>>(),
             vec![heinz,heinz,karl,nevermind]);
  assert_eq!(eavt.iter().map(|d| d.attribute).collect::<Vec<_>>(),
             vec![pn, pa, pn, an]);

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
