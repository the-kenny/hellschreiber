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

pub fn make_txs() -> Vec<Vec<Fact>> {
  let make_tx = |xs: &Vec<(i64, Attribute, Value, Status)>| {
    xs.into_iter().map(|&(e, a, ref v, status)| {
      (EntityId(e), a, v.clone(), status)
    }).collect::<Vec<Fact>>()
  };

  [vec![(1, person_name, Value::Str("Heinz".into()),     Status::Added),
        (1, person_age,  Value::Int(23),                 Status::Added)],
   vec![(1, person_age,  Value::Int(42),                 Status::Added),
        (1, album_name,  Value::Str("Nevermind".into()), Status::Added)],
   vec![(1, album_name,  Value::Str("Nevermind".into()), Status::Retracted)],
   vec![(2, person_name, Value::Str("Karl".into()),      Status::Added)],
   vec![(3, album_name,  Value::Str("Nevermind".into()), Status::Added)],
  ].into_iter().map(make_tx).collect()
}
