extern crate rusqlite;

use super::*;

pub struct SqliteDb {
  conn: rusqlite::Connection,
}

impl SqliteDb {
  pub fn new() -> Self {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    // let conn = rusqlite::Connection::open("test.sqlite").unwrap();

    conn.execute_batch(include_str!("schema.sql"))
      .unwrap();

    SqliteDb { conn: conn }
  }

  fn attribute_value(&self,
                     entity: EntityId,
                     attribute: Attribute)
                     -> Option<(Value,TxId)> {
    let mut value_query = self.conn.prepare(
      "select v,t,retracted from datoms where e = ?1 and a = ?2 order by t desc"
    ).unwrap();

    // Query all datoms for attribute a, ordered by descending
    // tx. If the first datom has Status::Added we just return
    // this, else we search for the first non-retracted value
    let values = value_query.query_map(&[&entity.0, &(attribute.0).0], |row| {
      let v: Value = row.get(0);
      let t: TxId = TxId(row.get(1));
      let status = match row.get(2) {
        0 => Status::Added,
        1 => Status::Retracted,
        _ => unreachable!()
      };

      (v,t,status)
    }).unwrap().flat_map(|x| x);

    // Collect all retractions until we find an assertion.
    // If that assertion matches a retraction, we return
    // None, else we return the value

    use std::collections::BTreeSet;
    values.fold((None, BTreeSet::new()), |(ret, mut retractions), row| {
      if ret.is_some() {
        (ret, retractions)
      } else {
        match row {
          (v, _, Status::Retracted) => {
            retractions.insert(v);
            (None, retractions)
          }
          (ref v, _, Status::Added) if retractions.contains(&v) => {
            (Some(None), retractions)
          },
          (v, tx, Status::Added) => {
            (Some(Some((v, tx))), retractions)
          }
        }
      }
    }).0.unwrap_or(None)
  }
}

impl Db for SqliteDb {
  fn transact<T: Into<Fact>>(&mut self, _tx: &[T]) -> TxId {
    unimplemented!()
  }

  fn datoms<'a, C: Borrow<Components>>(&'a self, index: Index, components: C) -> Datoms {
    assert!(index == Index::Eavt);

    let components = components.borrow();
    match *components {
      Components(_, _, Some(_), _) => panic!("Components(_, _, Some(...), _) isn't implemented yet"),
      Components(_, _, _, Some(_)) => panic!("Components(_, _, _, Some(...)) isn't implemented yet"),
      _ => ()
    }

    let mut entity_query = self.conn.prepare(
      "select distinct e
       from datoms
       where case when ?1 NOTNULL then e == ?1 else 1 end
       order by t asc ").unwrap();

    let mut attribute_query = self.conn.prepare(
      "select distinct a
       from datoms
       where e = ?1
       and case when ?2 NOTNULL then a == ?2 else 1 end
       order by t asc").unwrap();

    let entity_query_input = match components.0 {
      Some(EntityId(i)) => rusqlite::types::Value::Integer(i),
      None              => rusqlite::types::Value::Null,
    };
    let entities = entity_query.query_map(&[&entity_query_input], |row| {
      let e: EntityId = EntityId(row.get(0));

      let attribute_query_input = match components.1 {
        Some(Attribute(EntityId(i))) => rusqlite::types::Value::Integer(i),
        None                         => rusqlite::types::Value::Null,
      };
      let datoms = attribute_query.query_map(&[&e.0, &attribute_query_input], |attr_row| {
        Attribute(EntityId(attr_row.get(0)))
      }).unwrap().map(|x| x.unwrap())
        .filter_map(|a| self.attribute_value(e, a).map(|(v, tx)| (a, v, tx)))
        .map(|(a, v, tx)| Datom {
          entity: e,
          attribute: a,
          value: v,
          tx: tx,
          status: Status::Added,
        });
      datoms.collect::<Vec<_>>()
    }).unwrap()
      .flat_map(|x| x.unwrap())
      .collect::<Vec<Datom>>();

    Cow::Owned(entities)
  }

  fn store_datoms(&mut self, datoms: &[Datom]) {
    let mut stmt = self.conn.prepare("insert into datoms (e,a,v,t,retracted) values (?1, ?2, ?3, ?4, ?5)")
      .unwrap();
    for d in datoms {
      let retracted = match d.status {
        Status::Added => 0,
        Status::Retracted => 1,
      };
      stmt.execute(&[&(d.entity.0),
                     &(d.attribute.0).0,
                     &d.value,
                     &d.tx.0,
                     &retracted])
        .unwrap();
    }
  }
}

impl rusqlite::types::FromSql for Value {
  fn column_result(value: rusqlite::types::ValueRef) -> rusqlite::types::FromSqlResult<Self> {
    use rusqlite::types::ValueRef;

    match value {
      ValueRef::Text(t)    => Ok(Value::Str(t.into())),
      ValueRef::Integer(i) => Ok(Value::Int(i)),
      _                    => unreachable!() // TODO
    }
  }
}

impl rusqlite::types::ToSql for Value {
  fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput> {
    use rusqlite::types::{ValueRef, ToSqlOutput};
    match self {
      &Value::Str(ref s) => Ok(ToSqlOutput::Borrowed(ValueRef::Text(s))),
      &Value::Int(i) => Ok(ToSqlOutput::Owned(rusqlite::types::Value::Integer(i)))
    }
  }
}
