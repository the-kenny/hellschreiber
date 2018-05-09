use ::*;

pub fn test_usage_001<D: Db>(mut db: D) {
  let schema = &[(Assert, TempId(0), attr::ident, "person_name".into()),
                 (Assert, TempId(0), attr::doc, "The name of a person")];
  db.transact(schema);
  
  let person_name = db.attribute("person_name").unwrap();

  let persons = &[(Assert, TempId(0), person_name, "Karl".to_string()),
                  (Assert, TempId(1), person_name, "Heinz".to_string())];
  let heinz = db.transact(persons).tempid_mappings[&TempId(1)];

  let retract_heinz_name = &[(Retract, heinz, person_name, Value::Str("Heinz".into()))];
  db.transact(retract_heinz_name);

  assert!(db.entity(heinz)["person/name"].is_empty());
}


