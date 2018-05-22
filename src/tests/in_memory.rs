use ::*;

use std::fmt;

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

  #[cfg(test)]
  fn all_datoms<'a>(&'a self) -> Datoms<'a> {
    let mut datoms = self.0.clone();
    datoms.sort_by_key(|d| (d.tx, d.attribute, d.status));
    datoms
  }

  fn datoms<I: Into<FilteredIndex>>(&self, index: I) -> Result<Datoms, Error> {
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

    let mut datoms_by_tx = self.0.clone();
    datoms_by_tx.sort_by_key(|d| d.tx);

    for d in datoms_by_tx {
      let d = EavEquality(d);
      match d.0.status {
        Status::Asserted => {
          datoms.insert(d);
        },
        Status::Retracted(_) if datoms.contains(&d) => {
          datoms.remove(&d);
        },
        Status::Retracted(_) => {
          unreachable!("Tried to retract non-existing datum: {:#?} (datoms: {:#?})", d.0, datoms)
        }
      }
    }

    let index = index.into();
    let indexed_attributes = self.indexed_attributes();

    let mut datoms = datoms.into_iter()
      .filter(|d| index.matches(&d.0))
      .map(|EavEquality(d)| d)
      .filter(|d| {
        // Handle special-case for AVET index (which only contains indexed datoms)
        match index.index {
          Index::Avet => indexed_attributes.contains(&d.attribute),
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
        Index::Eavt => cmp!(entity, attribute, value, tx),
        Index::Aevt => cmp!(attribute, entity, value, tx),
        Index::Avet => cmp!(attribute, value, entity, tx),
      }
    });

    Ok(datoms)
  }
}

impl fmt::Debug for TestDb {
  fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
    f.debug_struct("TestDb")
      .field("datoms", &self.0)
      .finish()
  }
}
