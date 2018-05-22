use ::*;

pub fn test_usage_001<D: Db>(mut db: D) {
  let tid = db.tempid();
  let schema = &[(Assert, tid, "db/ident", "person/name".into()),
                 (Assert, tid, "db/doc",   "The name of a person")];
  db.transact(schema).unwrap();

  let persons = &[(Assert, TempId(0), "person/name", "Karl".to_string()),
                  (Assert, TempId(1), "person/name", "Heinz".to_string())];
  let heinz = db.transact(persons).unwrap().tempid_mappings[&TempId(1)];

  let retract_heinz_name = &[(Retract, heinz, "person/name", Value::Str("Heinz".into()))];
  db.transact(retract_heinz_name).unwrap();

  assert!(db.entity(heinz).unwrap().get("person/name").is_none());
}
