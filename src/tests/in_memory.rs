use ::*;

#[derive(Debug)]
pub struct TestDb(Vec<Datom>);

impl TestDb {
  pub fn new() -> Self {
    let mut db = TestDb(vec![]);
    db.store_datoms(&seed_datoms());
    db
  }
}

impl Db for TestDb {
  fn store_datoms(&mut self, datoms: &[Datom]) {
    self.0.extend_from_slice(datoms);
  }

  // fn transact(&mut self, facts: &[Fact]) -> TxId {
  //   for fact in facts {
  //     println!("Handling {:?}", fact);
  //   }
      
  //   unimplemented!("TestDb::transact")
  // }

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
