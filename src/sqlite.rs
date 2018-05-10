extern crate rusqlite;
extern crate serde_json;

use super::*;

use std::path::Path;

#[derive(Debug)]
pub struct SqliteDb {
  conn: rusqlite::Connection,
}

impl SqliteDb {
  pub fn new() -> Self {
    let conn = rusqlite::Connection::open_in_memory().unwrap();

    conn.execute_batch(include_str!("schema.sql"))
      .unwrap();

    let mut db = SqliteDb { conn: conn };
    db.store_datoms(&seed_datoms());
    db
  }

  pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, rusqlite::Error> {
    let conn = rusqlite::Connection::open(path).unwrap();

    if !Self::has_sqlite_table(&conn, "datoms")? {
      conn.execute_batch(include_str!("schema.sql"))
        .unwrap();
    }

    let mut db = SqliteDb { conn: conn };

    if db.attribute("db/ident").is_none() {
      db.store_datoms(&seed_datoms());
    }

    Ok(db)
  }

  fn has_sqlite_table(conn: &rusqlite::Connection, table: &str) -> Result<bool, rusqlite::Error> {
    match conn.query_row("SELECT name FROM sqlite_master WHERE type='table' AND name=?1", &[&table], |_| true) {
      Ok(b) => Ok(b),
      Err(rusqlite::Error::QueryReturnedNoRows) => Ok(false),
      err => err
    }
  }

  // TODO: Get rid of this
  #[cfg(test)]
  fn make_datom(row: &rusqlite::Row) -> Datom {
    Datom {
      entity: EntityId(row.get(0)),
      attribute: Attribute::new(EntityId(row.get(1))),
      value: row.get(2),
      tx: row.get(3),
      status: row.get(4),
    }
  }

  fn attribute_values(&self,
                      entity: EntityId,
                      attribute: Attribute)
                      -> Vec<(EntityId, Attribute, Value, EntityId)> {
    let mut value_query = self.conn.prepare(
      "select v, t
       from datoms
       where e = ?1 and a = ?2 and retracted_tx is null
       order by t asc"
    ).unwrap();

    let rows = value_query.query_map(&[&entity.0, &(attribute.0).0], |row| {
      let v: Value = row.get(0);
      let t: EntityId = row.get(1);

      (entity, attribute, v, t)
    }).unwrap().map(|x| x.unwrap());

    rows.collect()
  }

  fn sort_datoms(datoms: &mut Vec<Datom>, index: Index) {
    // TODO: Get rid of this sort-by step
    datoms.sort_by(|l,r| {
      use std::cmp::Ordering;
      macro_rules! cmp {
        ($i:ident) => (l.$i.cmp(&r.$i));
        ($($i:ident),*) => {
          [$(cmp!($i)),*].into_iter().fold(Ordering::Equal, |o, x| o.then(*x))
        };
      }

      match index {
        Index::Eavt => cmp!(entity, tx, attribute, value),
      }
    });
  }

  fn eavt_datoms<'a, C: Borrow<Components>>(&'a self, components: C) -> Datoms {
    let components = components.borrow();

    let mut query = self.conn.prepare(
      "select distinct e, a
       from datoms
       where case when ?1 NOTNULL then e == ?1 else 1 end
         and case when ?2 NOTNULL then a == ?2 else 1 end
         and case when ?3 NOTNULL then v == ?3 else 1 end
         and case when ?4 NOTNULL then t == ?4 else 1 end
       order by t asc").unwrap();


    let entity_query_input = match components.0 {
      Some(EntityId(i)) => rusqlite::types::Value::Integer(i),
      None              => rusqlite::types::Value::Null,
    };

    let attribute_query_input = match components.1 {
      Some(Attribute(EntityId(i))) => rusqlite::types::Value::Integer(i),
      None              => rusqlite::types::Value::Null,
    };

    use rusqlite::types::{ToSql,ToSqlOutput};
    let value_query_input = match components.2 {
      Some(ref v) => v.to_sql().expect("Failed to convert to SQL type"),
      None        => ToSqlOutput::Owned(rusqlite::types::Value::Null),
    };

    let tx_query_input = match components.3 {
      Some(EntityId(i)) => rusqlite::types::Value::Integer(i),
      None                    => rusqlite::types::Value::Null,
    };

    let mut datoms = query.query_map(&[&entity_query_input, &attribute_query_input, &value_query_input, &tx_query_input], |row| {
      let e = EntityId(row.get(0));
      let a = Attribute::new(EntityId(row.get(1)));
      (e,a)
    }).unwrap().map(|r| r.unwrap())
      .flat_map(|(e, a)| self.attribute_values(e, a))
      .map(|(e, a, v, tx)| Datom {
        entity: e,
        attribute: a,
        value: v,
        tx: tx,
        status: Status::Added,
      })
      .collect::<Vec<_>>();

    // TODO: Get rid of this step
    Self::sort_datoms(&mut datoms, Index::Eavt);

    Cow::Owned(datoms)
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

  // fn transact<T: Into<Fact>>(&mut self, _tx: &[T]) -> TxId {
  //   // Note: This storage uses a special behavior for transactions: If
  //   // a datom is retracted, we just set the `retracted_tx` in sqlite.
  //   // This allows efficient querying of values as well as recreating
  //   // the history of retractions. Care must be taken when returning a
  //   // "history database" which contains all assertions and
  //   // retractions

  //   unimplemented!("Transact isn't implemented")
  // }

  fn datoms<'a, C: Borrow<Components>>(&'a self, index: Index, components: C) -> Datoms {
    match index {
      Index::Eavt => self.eavt_datoms(components),
    }
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
                         &d.attribute.0,
                         &d.value,
                         &d.tx.0,
                         &d.status])
          .unwrap();
      }

      for d in retracted {
        assert!(d.status.is_retraction());
        let retracted_tx = d.status.retraction_tx().unwrap();
        retract.execute(&[&retracted_tx.0,
                          &d.entity.0,
                          &d.attribute.0,
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
        ValueRef::Null        => Ok(Status::Added),
        ValueRef::Integer(tx) => Ok(Status::Retracted(EntityId(tx))),
        _                     => unreachable!()
      }
    }
  }

  impl types::ToSql for Status {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput> {
      match *self {
        Status::Added => Ok(types::Null.into()),
        Status::Retracted(EntityId(tx)) => Ok(tx.into()),
      }
    }
  }

  impl types::FromSql for Value {
    fn column_result(value: ValueRef) -> FromSqlResult<Self> {
      if let ValueRef::Text(json) = value {
        // ValueRef::Text(t)    => Ok(Value::Str(t.into())),
        serde_json::from_str(json)
          .map_err(|err| types::FromSqlError::Other(Box::new(err)))
      } else {
        Err(types::FromSqlError::InvalidType)
      }
    }
  }

  impl types::ToSql for Value {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput> {
      let json = serde_json::to_string(self).unwrap();
      Ok(ToSqlOutput::Owned(types::Value::Text(json)))
    }
  }

  
  /*
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
        &Value::Int(i)     => Ok(ToSqlOutput::Owned(types::Value::Integer(i))),
        &Value::Ref(eid)   => Ok(ToSqlOutput::Owned(types::Value::Integer(eid.0))),
      }
    }
  }
   */

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
