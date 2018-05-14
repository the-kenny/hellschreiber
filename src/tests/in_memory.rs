use ::*;

#[derive(Debug)]
#[allow(unused)]
pub struct TestDb(Vec<Datom>);

impl TestDb {
  #[allow(unused)]
  pub fn new() -> Self {
    let mut db = TestDb(vec![]);
    db.store_datoms(&seed_datoms()).unwrap();
    db
  }
}

impl Db for TestDb {
  fn store_datoms(&mut self, datoms: &[Datom]) -> Result<(), Error> {
    self.0.extend_from_slice(datoms);
    Ok(())
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
    datoms
  }

  fn datoms<I: Into<Index>>(&self, index: I) -> Result<Datoms, Error> {
    let index = index.into();
    
    let mut raw_datoms = self.0.clone();
    raw_datoms.retain(|d| index.matches(&d));
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

    let indexed_attributes = self.indexed_attributes();

    let mut datoms = datoms.into_iter()
      .map(|EavEquality(d)| d)
      .filter(|d| {
        // Handle special-case for AVET index (which only contains indexed datoms)
        match index.index {
          IndexType::Avet => indexed_attributes.contains(&d.attribute),
          _ => true
        }
      })
      .collect::<Vec<Datom>>();

    datoms.sort_by(|l,r| {
      use std::cmp::Ordering;
      macro_rules! cmp {
        ($i:ident) => (l.$i.cmp(&r.$i));
        ($($i:ident),*) => {
          [$(cmp!($i)),*].into_iter().fold(Ordering::Equal, |o, x| o.then(*x))
        };
      }

      match index.index {
        IndexType::Eavt => cmp!(entity, attribute, value, tx),
        IndexType::Aevt => cmp!(attribute, entity, value, tx),
        IndexType::Avet => cmp!(attribute, value, entity, tx),
      }
    });

    Ok(datoms)
  }
}
