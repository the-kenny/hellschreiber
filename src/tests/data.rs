use ::*;

// Mappings
// 1 -> person/name
// 2 -> person/age
// 3 -> album/name

#[allow(non_upper_case_globals)]
pub const person_name: Attribute = Attribute(EntityId(1));
#[allow(non_upper_case_globals)]
pub const person_age: Attribute =  Attribute(EntityId(2));
#[allow(non_upper_case_globals)]
pub const album_name: Attribute =  Attribute(EntityId(3));

pub fn make_test_data() -> Vec<Datom> {
  [
    // Simple person named 'Heinz'
    (1, person_name, Value::Str("Heinz".into()),     1, Status::Added),
    // Add age, change it in next tx
    (1, person_age,  Value::Int(23),                 1, Status::Added),
    (1, person_age,  Value::Int(42),                 2, Status::Added),
    // Add attribute, retract it in the next tx
    (1, album_name,  Value::Str("Nevermind".into()), 2, Status::Added),
    (1, album_name,  Value::Str("Nevermind".into()), 3, Status::Retracted),
    // New entity
    (2, person_name, Value::Str("Karl".into()),      4, Status::Added),
    (3, album_name,  Value::Str("Nevermind".into()), 5, Status::Added),
    // Retract with wrong value
    (3, album_name,  Value::Str("xxx".into()),       6, Status::Retracted),
  ].into_iter()
    .map(|&(e, a, ref v, t, status)| {
      Datom {
        entity: EntityId(e),
        attribute: a,
        value: v.clone(),
        tx: TxId(t),
        status: status,
      }
    }).collect()
}
