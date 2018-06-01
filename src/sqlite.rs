extern crate rusqlite;
extern crate serde_json;

use super::*;

use std::path::Path;

#[derive(Debug, Fail, From)]
pub enum Error {
    #[fail(display="Sqlite Error: {}", _0)]
    Sqlite(rusqlite::Error),
    #[fail(display="Transaction Error: {}", _0)]
    TransactionError(transaction::TransactionError)
}

#[derive(Debug)]
pub struct SqliteDb {
    conn: rusqlite::Connection,
}

// TODO: Implement dynamic `Db::indexed_attributes`
impl SqliteDb {
    pub fn new() -> Result<Self, Error> {
        let conn = rusqlite::Connection::open_in_memory().unwrap();

        let mut db = SqliteDb { conn };
        db.initialize()?;
        Ok(db)
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let conn = rusqlite::Connection::open(path).unwrap();

        let mut db = SqliteDb { conn };
        db.initialize()?;
        Ok(db)
    }

    fn has_sqlite_table(conn: &rusqlite::Connection, table: &str) -> Result<bool, rusqlite::Error> {
        conn.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?1")?.exists(&[&table])
    }

    fn initialize(&mut self) -> Result<(), Error> {
        if !Self::has_sqlite_table(&self.conn, "datoms")? {
            self.conn.execute_batch(include_str!("schema.sql"))?
        }

        if self.attribute("db/ident").is_none() {
            self.store_datoms(&seed_datoms())?;
        }

        for unique in self.indexed_attributes() {
            self.conn.execute("insert or ignore into unique_attributes (e) values (?1)", &[&unique.0])?;
        }

        self.conn.execute("pragma foreign_keys = on", &[])?;

        Ok(())
    }

    // TODO: Get rid of this
    #[cfg(test)]
    fn make_datom(row: &rusqlite::Row) -> Datom {
        Datom {
            entity: EntityId(row.get(0)),
            attribute: Attribute(EntityId(row.get(1))),
            value: row.get(2),
            tx: row.get(3),
            status: row.get(4),
        }
    }
}

impl Db for SqliteDb {
    type Error = Error;

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
            added.status = Status::Asserted;

            let mut retracted = d.clone();
            retracted.tx = match retracted.status {
                Status::Retracted(tx) => tx,
                _ => unreachable!()
            };

            datoms.push(added);
            datoms.push(retracted);
        }

        // TODO: Move to helper
        datoms.sort_by_key(|d| (d.tx, d.attribute, d.status));

        datoms
    }

    fn highest_eid(&self, partition: Partition) -> EntityId {
        let partition_mask = partition as i64;
        let mut stmt = self.conn.prepare_cached(
            "select coalesce(max(e), 0) from datoms
             where e >= ?1
               and (e & ?1) == ?1"
        ).unwrap();

        let n = stmt.query_row(&[&partition_mask], |row| row.get(0))
            .unwrap_or(0);

        EntityId(std::cmp::max(n, partition as i64))
    }

    fn datoms<I: Into<FilteredIndex>>(&self, index: I) -> Result<Datoms, Self::Error> {
        let index = index.into();
        let (e, a, v, t) = index.eavt();

        let order_statement = match index.index {
            Index::Eavt => "order by datoms.e, datoms.a, datoms.v, datoms.t asc",
            Index::Aevt => "order by datoms.a, datoms.e, datoms.v, datoms.t asc",
            Index::Avet => "order by datoms.a, datoms.v, datoms.e, datoms.t asc",
        };

        let join_clause = match index.index {
            Index::Avet => "join unique_attributes on unique_attributes.e = datoms.a",
            _ => ""
        };

        let mut query = self.conn.prepare_cached(&format!(
            "select distinct datoms.e, datoms.a, datoms.v, datoms.t
             from datoms
             {}
             where retracted_tx is null
               and case when ?1 notnull then datoms.e == ?1 else 1 end
               and case when ?2 notnull then datoms.a == ?2 else 1 end
               and case when ?3 notnull then datoms.v == ?3 else 1 end
               and case when ?4 notnull then datoms.t == ?4 else 1 end
             {}
      ", join_clause, order_statement))?;

        let entity_query_input = match e {
            Some(EntityId(id)) => rusqlite::types::Value::Integer(id),
            None              => rusqlite::types::Value::Null,
        };

        let attribute_query_input = match a {
            Some(Attribute(EntityId(id))) => rusqlite::types::Value::Integer(id),
            None              => rusqlite::types::Value::Null,
        };

        use rusqlite::types::{ToSql,ToSqlOutput};
        let value_query_input = match v {
            Some(ref value) => value.to_sql().expect("Failed to convert to SQL type"),
            None        => ToSqlOutput::Owned(rusqlite::types::Value::Null),
        };

        let tx_query_input = match t {
            Some(EntityId(id)) => rusqlite::types::Value::Integer(id),
            None              => rusqlite::types::Value::Null,
        };

        let datoms = query.query_map(&[&entity_query_input,
                                       &attribute_query_input,
                                       &value_query_input,
                                       &tx_query_input], |row| {
            Datom {
                entity:    EntityId(row.get(0)),
                attribute: Attribute(EntityId(row.get(1))),
                value:     row.get(2),
                tx:        row.get(3),
                status:    Status::Asserted,
            }
        })?
        .map(|r| r.map_err(|e| e.into()))
        .collect::<Result<Vec<_>, _>>();

        datoms
    }

    fn store_datoms(&mut self, datoms: &[Datom]) -> Result<(), Self::Error> {
        let tx = self.conn.transaction()?;

        {
            // A single transaction can assert and retract the same value so
            // we have to persist all assertions before doing any
            // retractions as our implementation will set the `retracted_tx`
            // attribute on the database row.

            let (asserted, retracted): (Vec<&Datom>, Vec<&Datom>) = datoms.iter()
                .partition(|d| d.status.is_assertion());

            let mut insert = tx.prepare_cached(
                "insert into datoms (e,a,v,t) values (?1, ?2, ?3, ?4)"
            )?;

            for d in asserted {
                assert!(d.status.is_assertion());
                insert.execute(&[&(d.entity.0),
                                 &d.attribute.0,
                                 &d.value,
                                 &d.tx.0])?;
            }

            // To retract we set the `retracted_tx` column on our datom. We
            // have to make sure we aren't updating any datoms from our
            // current transactions which were inserted earlier, so we
            // explicitly check for `datoms.t != d.tx`. This must affect
            // exactly one single row. If an UPDATE affects multiple rows we
            // just panic to bail out.
            let mut retract = tx.prepare_cached(
                "update datoms set retracted_tx = ?1
                 where e = ?2
                   and a = ?3
                   and v = ?4
                   and t != ?5
                   and retracted_tx is null"
            ).unwrap();


            for d in retracted {
                assert!(d.status.is_retraction());
                let retracted_tx = match d.status {
                    Status::Retracted(tx) => tx,
                    _ => unreachable!()
                };

                let row_count = retract.execute(&[&retracted_tx.0,
                                                  &d.entity.0,
                                                  &d.attribute.0,
                                                  &d.value,
                                                  &d.tx])?;
                if row_count != 1 {
                    panic!("UPDATE to change datoms.retracted_tx affected more than one row. Datom: {:?}", d);
                }
            }
        }

        tx.commit()?;

        Ok(())
    }
}

mod type_impls {
    use super::*;

    use rusqlite::types;
    use rusqlite::types::{ValueRef, ToSqlOutput, FromSqlResult};

    impl types::FromSql for Status {
        fn column_result(value: types::ValueRef) -> FromSqlResult<Self> {
            match value {
                ValueRef::Null        => Ok(Status::Asserted),
                ValueRef::Integer(tx) => Ok(Status::Retracted(EntityId(tx))),
                _                     => unreachable!()
            }
        }
    }

    impl types::ToSql for Status {
        fn to_sql(&self) -> rusqlite::Result<ToSqlOutput> {
            match *self {
                Status::Asserted => Ok(types::Null.into()),
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
