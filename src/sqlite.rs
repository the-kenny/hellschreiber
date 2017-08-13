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

  #[cfg(test)]
  fn make_datom(row: &rusqlite::Row) -> Datom {
    Datom {
      entity: EntityId(row.get(0)),
      attribute: Attribute(EntityId(row.get(1))),
      value: row.get(2),
      tx: TxId(row.get(3)),
      status: row.get(4),
    }
  }

  fn attribute_values(&self,
                      entity: EntityId,
                      attribute: Attribute)
                      -> Vec<(Attribute, Value, TxId)> {
    let mut value_query = self.conn.prepare(
      "select v, t
       from datoms
       where e = ?1 and a = ?2 and retracted_tx is null
       order by t asc"
    ).unwrap();

    let rows = value_query.query_map(&[&entity.0, &(attribute.0).0], |row| {
      let v: Value = row.get(0);
      let t: TxId = TxId(row.get(1));

      (attribute, v, t)
    }).unwrap().map(|x| x.unwrap());

    rows.collect()
  }
}


impl Db for SqliteDb {
  #[cfg(test)]
  fn all_datoms<'a>(&'a self) -> Datoms<'a> {
    let mut added_query = self.conn.prepare(
      "select * from datoms
       where retracted_tx is null
       order by t asc"
    ).unwrap();

    let mut retracted_query = self.conn.prepare(
      "select * from datoms
       where retracted_tx is not null
       order by t asc"
    ).unwrap();

    let added = added_query
      .query_map(&[], SqliteDb::make_datom).unwrap()
      .flat_map(|x| x)
      .collect::<Vec<Datom>>();

    let retracted = retracted_query
      .query_map(&[], SqliteDb::make_datom).unwrap()
      .flat_map(|x| x)
      .collect::<Vec<Datom>>();

    let mut datoms = added;

    // For each retracted datom create two new datoms in our final
    // data set. One assertion and one retraction.
    for d in retracted {
      assert!(d.status.is_retraction());

      let mut added = d.clone();
      added.status = Status::Added;

      let mut retracted = d.clone();
      retracted.tx = retracted.status.retraction_tx().unwrap();

      datoms.push(added);
      datoms.push(retracted);
    }

    // TODO: Move to helper
    datoms.sort_by_key(|d| (d.tx, d.attribute, d.status));

    Cow::Owned(datoms)
  }

  fn transact<T: Into<Fact>>(&mut self, _tx: &[T]) -> TxId {
    unimplemented!()
  }

  fn datoms<'a, C: Borrow<Components>>(&'a self, index: Index, components: C) -> Datoms {
    assert!(index == Index::Eavt);

    // TODO: Add validity tests

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
        .flat_map(|a| self.attribute_values(e, a))
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
    let tx = self.conn.transaction().unwrap();

    {
      let mut insert = tx.prepare(
        "insert into datoms (e,a,v,t,retracted_tx) values (?1, ?2, ?3, ?4, ?5)"
      ).unwrap();

      let mut retract = tx.prepare(
        "update datoms set retracted_tx = ?1
         where e = ?2 and a = ?3 and v = ?4"
      ).unwrap();

      let added     = datoms.iter().filter(|d| d.status.is_assertion());
      let retracted = datoms.iter().filter(|d| d.status.is_retraction());

      for d in added {
        assert!(d.status == Status::Added);
        insert.execute(&[&(d.entity.0),
                         &(d.attribute.0).0,
                         &d.value,
                         &d.tx.0,
                         &d.status])
          .unwrap();
      }

      for d in retracted {
        assert!(d.status.is_retraction());
        let retracted_tx = d.status.retraction_tx().unwrap();
        retract.execute(&[&retracted_tx.0,
                          &(d.entity.0),
                          &(d.attribute.0).0,
                          &d.value])
          .unwrap();
      }
    }

    tx.commit().unwrap()
  }
}

mod type_impls {
  use super::*;

  use rusqlite::types;
  use rusqlite::types::{ValueRef, ToSqlOutput, FromSqlResult};

  impl types::FromSql for Status {
    fn column_result(value: types::ValueRef) -> FromSqlResult<Self> {
      match value {
        ValueRef::Null       => Ok(Status::Added),
        ValueRef::Integer(tx) => Ok(Status::Retracted(TxId(EntityId(tx)))),
        _                    => unimplemented!()
      }
    }
  }

  impl types::ToSql for Status {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput> {
      match *self {
        Status::Added => Ok(types::Null.into()),
        Status::Retracted(TxId(EntityId(tx))) => Ok(tx.into()),
      }
    }
  }

  impl types::FromSql for Value {
    fn column_result(value: ValueRef) -> FromSqlResult<Self> {
      match value {
        ValueRef::Text(t)    => Ok(Value::Str(t.into())),
        ValueRef::Integer(i) => Ok(Value::Int(i)),
        _                    => unreachable!() // TODO
      }
    }
  }

  impl types::ToSql for Value {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput> {
      match self {
        &Value::Str(ref s) => Ok(ToSqlOutput::Borrowed(ValueRef::Text(s))),
        &Value::Int(i)     => Ok(ToSqlOutput::Owned(types::Value::Integer(i)))
      }
    }
  }

  impl types::FromSql for EntityId {
    fn column_result(value: types::ValueRef) -> FromSqlResult<Self> {
      value.as_i64().map(EntityId)
    }
  }

  impl types::ToSql for EntityId {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput> {
      Ok(types::Value::Integer(self.0).into())
    }
  }
}
