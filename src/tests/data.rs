use ::*;

// Mappings
// 1 -> person/name
// 2 -> person/age
// 3 -> album/name

#[allow(non_upper_case_globals)]
pub const person_name:     Attribute = Attribute(EntityId(1));
#[allow(non_upper_case_globals)]
pub const person_age:      Attribute = Attribute(EntityId(2));
#[allow(non_upper_case_globals)]
pub const person_children: Attribute = Attribute(EntityId(3));
#[allow(non_upper_case_globals)]
pub const album_name:      Attribute = Attribute(EntityId(4));

pub fn make_test_data() -> Vec<Datom> {
  [
    // Simple person named 'Heinz'
    (1, person_name,     Value::Str("Heinz".into()),     1, Status::Asserted),
    // Add age, change it in next tx
    (1, person_age,      Value::Int(23),                 1, Status::Asserted),
    (1, person_age,      Value::Int(23),                 2, Status::Retracted(EntityId(2))),
    (1, person_age,      Value::Int(42),                 2, Status::Asserted),
    // Add attribute, retract it in the next tx
    (1, album_name,      Value::Str("Nevermind".into()), 2, Status::Asserted),
    (1, album_name,      Value::Str("Nevermind".into()), 3, Status::Retracted(EntityId(3))),
    // New entity
    (2, person_name,     Value::Str("Karl".into()),      4, Status::Asserted),
    (2, person_children, Value::Str("Philipp".into()),   4, Status::Asserted),
    (3, album_name,      Value::Str("Nevermind".into()), 5, Status::Asserted),
    (2, person_children, Value::Str("Jens".into()),      6, Status::Asserted),
    (2, person_children, Value::Str("Jochen".into()),    6, Status::Asserted),
    (2, person_children, Value::Str("Jochen".into()),    7, Status::Retracted(EntityId(7))),
  ].into_iter()
    .map(|&(e, a, ref v, t, status)| {
      Datom {
        entity: EntityId(e),
        attribute: a,
        value: v.clone(),
        tx: EntityId(t),
        status: status,
      }
    }).collect()
}

/*
pub fn make_txs() -> Vec<Vec<Fact>> {
  let make_tx = |xs: &Vec<(i64, Attribute, Value, Status)>| {
    xs.into_iter().map(|&(e, a, ref v, status)| {
      (EntityId(e), a, v.clone(), status)
    }).collect::<Vec<Fact>>()
  };

  [vec![(1, person_name,     Value::Str("Heinz".into()),     Status::Asserted),
        (1, person_age,      Value::Int(23),                 Status::Asserted)],
   vec![(1, person_age,      Value::Int(23),                 Status::Retracted),
        (1, person_age,      Value::Int(42),                 Status::Asserted),
        (1, album_name,      Value::Str("Nevermind".into()), Status::Asserted)],
   vec![(1, album_name,      Value::Str("Nevermind".into()), Status::Retracted)],
   vec![(2, person_name,     Value::Str("Karl".into()),      Status::Asserted),
        (2, person_children, Value::Str("Philipp".into()),   Status::Asserted)],
   vec![(3, album_name,      Value::Str("Nevermind".into()), Status::Asserted)],
   vec![(2, person_children, Value::Str("Jens".into()),      Status::Asserted),
        (2, person_children, Value::Str("Jochen".into()),    Status::Asserted)],
   vec![(2, person_children, Value::Str("Jochen".into()),    Status::Retracted)]]
    .into_iter().map(make_tx).collect()
}
*/
