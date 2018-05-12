use ::*;

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

/*
#[test]
pub fn test_components() {
  let d = Datom {
    entity:    EntityId(42),
    attribute: Attribute::new(EntityId(1)),
    value:     Value::Int(23),
    tx:        EntityId(10),
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
  assert_eq!(false, Components(None, Some(Attribute::new(EntityId(999))), None, None).matches(&d));
  assert_eq!(false, Components(None, None, Some(Value::Int(1000)),None).matches(&d));
  assert_eq!(false, Components(None, None, None, Some(EntityId(999))).matches(&d));
}
 */

pub fn test_seed_datoms<D: Db>(db: D) {
  assert!(db.attribute("db/id")    == Some(attr::id));
  assert!(db.attribute("db/ident") == Some(attr::ident));
  assert!(db.attribute("db/doc")   == Some(attr::doc));
  assert!(db.attribute("db/tx_instant")   == Some(attr::tx_instant));

  // TODO: Check if `db/doc` is set for all entities
}

pub fn test_entity<D: Db>(mut db: D) {
  use tests::data::*;

  db.store_datoms(&tests::data::make_test_data()).unwrap();
  validate_datoms(&db.all_datoms());

  assert_eq!(db.entity(EntityId(99999)).unwrap().values.len(), 1);
  assert_eq!(db.entity(EntityId(99999)).unwrap().values[&attr::id], vec![Value::Int(99999)]);

  let heinz = db.entity(EntityId(1)).unwrap().values;
  assert_eq!(heinz.len(), 3);   // name + age + db/id
  assert_eq!(heinz.get(&attr::id), Some(&vec![Value::Int(1)]));
  assert_eq!(heinz.get(&person_name), Some(&vec![Value::Str("Heinz".into())]));
  assert_eq!(heinz.get(&person_age), Some(&vec![Value::Int(42)]));
  assert_eq!(heinz.get(&album_name), None);

  let karl  = db.entity(EntityId(2)).unwrap().values;
  assert_eq!(karl.len(), 3);    // name + children + db/id
  assert_eq!(karl[&person_name], vec![Value::Str("Karl".into())]);
  assert_eq!(karl[&person_children], vec![Value::Str("Philipp".into()),
                                          Value::Str("Jens".into())]);

  let nevermind = db.entity(EntityId(3)).unwrap().values;
  assert_eq!(nevermind.len(), 2);
  assert_eq!(nevermind.get(&tests::data::album_name), Some(&vec![Value::Str("Nevermind".into())]));
}

pub fn test_eavt_datoms<D: Db>(mut db: D) {
  db.store_datoms(&tests::data::make_test_data()).unwrap();

  let pn = tests::data::person_name;
  let pa = tests::data::person_age;
  let an = tests::data::album_name;
  let pc = tests::data::person_children;

  let heinz     = EntityId(1);
  let karl      = EntityId(2);
  let nevermind = EntityId(3);

  let eavt = db.datoms(Index::Eavt(None, None, None, None)).unwrap(); // TODO
  let pairs = eavt.iter()
    .filter(|d| d.tx != EntityId(0))
    .map(|d| (d.attribute, d.entity))
    .collect::<Vec<_>>();
  assert_eq!(pairs, vec![(pn, heinz),
                         (pa, heinz),
                         (pn, karl),
                         (pc, karl),
                         (pc, karl),
                         (an, nevermind)]);

  // None
  let eavt = db.datoms(Index::Eavt(Some(EntityId(99999)), None, None, None)).unwrap(); // TODO
  assert!(eavt.iter().map(|d| d.entity).collect::<Vec<_>>().is_empty());

  // Heinz
  let eavt = db.datoms(Index::Eavt(Some(heinz), None, None, None)).unwrap(); // TODO
  assert_eq!(eavt.iter().map(|d| d.entity).collect::<Vec<_>>(),
             vec![heinz,heinz]);
  assert_eq!(eavt.iter().map(|d| d.attribute).collect::<Vec<_>>(),
             vec![pn, pa]);

  // Heinz, just person/age
  let eavt = db.datoms(Index::Eavt(Some(heinz), Some(pa), None, None)).unwrap(); // TODO
  assert_eq!(eavt.iter().map(|d| d.entity).collect::<Vec<_>>(),
             vec![heinz]);
  assert_eq!(eavt.iter().map(|d| d.attribute).collect::<Vec<_>>(),
             vec![pa]);

  // Nevermind
  let eavt = db.datoms(Index::Eavt(Some(nevermind), None, None, None)).unwrap(); // TODO
  assert_eq!(eavt.iter().map(|d| d.entity).collect::<Vec<_>>(),
             vec![nevermind]);
  assert_eq!(eavt.iter().map(|d| d.attribute).collect::<Vec<_>>(),
             vec![an]);

  // Nevermind, person/age
  let eavt = db.datoms(Index::Eavt(Some(nevermind), Some(pa), None, None)).unwrap(); // TODO
  assert_eq!(eavt.iter().map(|d| d.entity).collect::<Vec<_>>(),
             vec![]);
  assert_eq!(eavt.iter().map(|d| d.attribute).collect::<Vec<_>>(),
             vec![]);

}

pub fn test_aevt_datoms<D: Db>(mut db: D) {
  let tx = &[(Assert, db.tempid(), "db/ident", "person/name"),
             (Assert, db.tempid(), "db/ident", "person/age")];
  db.transact(tx).unwrap();

  let karl = db.tempid();
  let heinz = db.tempid();
  
  let data_tx = &[(Assert, karl, "person/name", Value::Str("Karl".into())),
                  (Assert, karl, "person/age", 42.into()),
                  (Assert, heinz, "person/name", "Heinz".into())];
  db.transact(data_tx).unwrap();

  let person_name_attr = db.attribute("person/name").unwrap();
  let person_age_attr = db.attribute("person/age").unwrap();

  assert_eq!(2, db.datoms(Index::Aevt(Some(person_name_attr), None, None, None)).unwrap().len());
  assert_eq!(1, db.datoms(Index::Aevt(Some(person_age_attr),  None, None, None)).unwrap().len());
}

pub fn test_db_equality<D: Db, E: Db>(mut db1: D, mut db2: E) {
  db1.store_datoms(&tests::data::make_test_data()).unwrap();
  db2.store_datoms(&tests::data::make_test_data()).unwrap();

  // TODO: Randomize test data

  assert_eq!(db1.all_datoms(), db2.all_datoms(),
             "Equality of db1 and db2 for db.all_datoms()");

  use ::tests::data::person_name;
  for idx in [Index::Eavt(None,                None,              None, None),
              Index::Eavt(Some(EntityId(1)),   None,              None, None),
              Index::Eavt(Some(EntityId(999)), None,              None, None),
              Index::Eavt(None,                Some(person_name), None, None)].iter() {

    assert_eq!(db1.datoms(idx.clone()).unwrap(),
               db2.datoms(idx.clone()).unwrap(),
               "Equality of db1 and db2 for the {:?} index", idx);
  }
}

pub fn test_fn_attribute<D: Db>(mut db: D) {
  // TODO: Use `str` as db/ident
  let schema = &[(Assert, TempId(42), attr::ident, Value::Str("person_name".into())),
                 (Assert, TempId(42), attr::doc, Value::Str("The name of a person".into()))];
  db.transact(schema).unwrap();
  assert!(db.attribute("person_name").is_some());
}

pub fn test_db_metadata<D: Db>(db: D) {
  let Attribute(ident_eid) = "db/ident".to_attribute(&db).unwrap();
  let Attribute(doc_eid) = "db/doc".to_attribute(&db).unwrap();

  assert!(!db.entity(ident_eid).unwrap().values.is_empty());
  assert!(!db.entity(doc_eid).unwrap().values.is_empty());
}

pub fn test_string_attributes<D: Db>(mut db: D) {
  let tx = [(Assert, db.tempid(), "db/ident", "xx")];
  db.transact(&tx).unwrap();
}

pub fn test_highest_eid<D: Db>(mut db: D) {
  assert_eq!(db.highest_eid(), EntityId(1000));

  db.transact(&[(Assert, TempId(42), attr::ident, "foo/bar")]).unwrap();
  assert_eq!(db.highest_eid(), EntityId(1001));
}

pub fn test_transact_panics_for_unknown_attributes<D: Db>(mut db: D) {
  let tx = [(Assert, TempId(42), Attribute(db.highest_eid()), "xx")];
  db.transact(&tx).unwrap();
}

pub fn test_entity_index_trait<D: Db>(db: D) {
  let entity = db.entity(attr::ident.0).unwrap();
  assert_eq!(false, entity["db/ident"].is_empty());
  assert_eq!(true,  entity["unknown/attribute"].is_empty());
}

/*
pub fn test_ref_follow<D: Db>(db: D) {
  let entity = db.entity(attr::ident.0);

}
*/
