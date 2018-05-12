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

  fn highest_eid(&self) -> EntityId {
    let n = self.conn.query_row("select max(e) from datoms", &[], |row| row.get(0))
      .unwrap_or(0);

    EntityId(std::cmp::max(n, 1000))
  }

  fn datoms<'a>(&'a self, index: Index) -> Datoms {
    let (e, a, v, t) = index.unwrap();
    
    let order_statement = match index {
      Index::Eavt(_, _, _, _) => "e, a, v, t asc",
      Index::Aevt(_, _, _, _) => "a, e, v, t asc",
    };

    let mut query = self.conn.prepare_cached(&format!(
      "select distinct e, a, v, t
       from datoms
       where retracted_tx is null
         and case when ?1 notnull then e == ?1 else 1 end
         and case when ?2 notnull then a == ?2 else 1 end
         and case when ?3 notnull then v == ?3 else 1 end
         and case when ?4 notnull then t == ?4 else 1 end
       order by {}", order_statement)).unwrap();

    let entity_query_input = match e {
      Some(EntityId(i)) => rusqlite::types::Value::Integer(i),
      None              => rusqlite::types::Value::Null,
    };

    let attribute_query_input = match a {
      Some(Attribute(EntityId(i))) => rusqlite::types::Value::Integer(i),
      None              => rusqlite::types::Value::Null,
    };

    use rusqlite::types::{ToSql,ToSqlOutput};
    let value_query_input = match v {
      Some(ref v) => v.to_sql().expect("Failed to convert to SQL type"),
      None        => ToSqlOutput::Owned(rusqlite::types::Value::Null),
    };

    let tx_query_input = match t {
      Some(EntityId(i)) => rusqlite::types::Value::Integer(i),
      None              => rusqlite::types::Value::Null,
    };

    let datoms = query.query_map(&[&entity_query_input,
                                   &attribute_query_input,
                                   &value_query_input,
                                   &tx_query_input], |row| {
      let e = EntityId(row.get(0));
      let a = Attribute::new(EntityId(row.get(1)));
      let v: Value = row.get(2);
      let t: TxId = row.get(3);
      (e, a, v, t)
    }).unwrap().map(|r| r.unwrap())
      .map(|(e, a, v, tx)| Datom {
        entity: e,
        attribute: a,
        value: v,
        tx: tx,
        status: Status::Added,
      })
      .collect::<Vec<_>>();

    Cow::Owned(datoms)
  }

  fn store_datoms(&mut self, datoms: &[Datom]) {
    let tx = self.conn.transaction().unwrap();

    {
      let mut insert = tx.prepare_cached(
        "insert into datoms (e,a,v,t,retracted_tx) values (?1, ?2, ?3, ?4, ?5)"
      ).unwrap();

      let mut retract = tx.prepare_cached(
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
