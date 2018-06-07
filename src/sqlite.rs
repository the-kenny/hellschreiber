extern crate rusqlite;
extern crate serde_json;

use super::*;

use std::path::Path;
use std::collections::{HashSet, HashMap};

#[derive(Debug, Fail, From)]
pub enum Error {
    #[fail(display="Sqlite Error: {}", _0)]
    Sqlite(rusqlite::Error),
    #[fail(display="Transaction Error: {}", _0)]
    TransactionError(transaction::TransactionError)
}

#[derive(Debug)]
pub struct Db {
    conn: rusqlite::Connection,
}

const INDEXED_ATTRIBUTES: &[Attribute] = &[attr::ident];

impl Db {
    pub fn new() -> Result<Self, Error> {
        let conn = rusqlite::Connection::open_in_memory().unwrap();

        let mut db = Db { conn };
        db.initialize()?;
        Ok(db)
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let conn = rusqlite::Connection::open(path).unwrap();

        let mut db = Db { conn };
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

        for unique in INDEXED_ATTRIBUTES {
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

impl Db {
    #[cfg(test)]
    pub(crate) fn all_datoms<'a>(&'a self) -> Datoms<'a> {
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
            .query_map(&[], Db::make_datom).unwrap()
            .flat_map(|x| x)
            .collect::<Vec<Datom>>();

        let retracted = retracted_query
            .query_map(&[], Db::make_datom).unwrap()
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

    pub(crate) fn highest_eid(&self, partition: Partition) -> EntityId {
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

    pub fn datoms<I: Into<FilteredIndex>>(&self, index: I) -> Result<Datoms, Error> {
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

    pub(crate) fn store_datoms(&mut self, datoms: &[Datom]) -> Result<(), Error> {
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

    pub fn entity(&self, entity: EntityId) -> Result<Entity, Error> {
        let datoms = self.datoms(Index::Eavt.e(entity))?;
        let mut attrs: BTreeMap<Attribute, BTreeSet<&Datom>> = BTreeMap::new();

        for datoms in &datoms {
            let entry = attrs.entry(datoms.attribute)
                .or_insert_with(BTreeSet::new);

            match datoms.status {
                Status::Asserted => {
                    entry.insert(&datoms);
                },
                Status::Retracted(_) if entry.contains(&datoms) => {
                    entry.remove(&datoms);
                },
                Status::Retracted(_) => {
                    panic!("Got retraction for non-existing value. Retraction: {:?}", datoms)
                }
            }
        }

        // Assert all datoms are of the same entity
        assert!(attrs.values().flat_map(|x| x).all(|datoms| datoms.entity == entity));

        let values = attrs.into_iter()
            .map(|(a, ds)| {
                let datoms: Vec<_> = ds.into_iter()
                    .map(|datoms| datoms.value.clone())
                    .collect();
                (a, datoms)
            }).collect::<BTreeMap<Attribute, Vec<Value>>>();

        let entity = Entity {
            db: self,
            eid: entity,
            values,
        };

        Ok(entity)
    }
}

impl Db {
    pub fn tempid(&mut self) -> TempId {
        tempid()
    }

    pub fn transact<O: Into<Operation>, I: IntoIterator<Item=O>>(&mut self, tx: I) -> Result<TransactionData, Error> {
        let tx_eid = EntityId(self.highest_eid(Partition::Tx).0 + 1);

        let now = chrono::Utc::now();

        let mut datoms = vec![Datom {
            entity:    tx_eid,
            attribute: attr::tx_instant,
            value:     Value::DateTime(now),
            tx:        tx_eid,
            status:    Status::Asserted
        }];

        let tx = tx.into_iter()
            .map(|op| op.into())
            .collect::<Vec<Operation>>();

        datoms.reserve(tx.len());

        let attribute_ids = {
            let deduped_attribute_names = tx.iter()
            .map(Operation::attribute_name)
                .collect::<HashSet<_>>();

            deduped_attribute_names.into_iter()
                .map(|attribute_name| {
                    if let Some(attribute) = self.attribute(&attribute_name) {
                        Ok((attribute_name.into(), attribute))
                    } else {
                        Err(TransactionError::UnknownAttribute(attribute_name.to_string()))
                    }
                })
                .collect::<Result<HashMap<AttributeName, Attribute>, _>>()?
        };
        
        let eids = {
            let mut eids = BTreeMap::new();
            let mut highest_eid = self.highest_eid(Partition::User).0;
            let mut highest_db_eid = self.highest_eid(Partition::Db).0;

            for operation in &tx {
                if let Operation::TempidAssertion(tempid, attribute_name, _) = operation {
                    let attribute = attribute_ids[attribute_name];
                    eids.entry(*tempid)
                        .or_insert_with(|| {
                            // If we're asserting an internal attribute (db/id,
                            // db/ident, db/doc, db.cardinality/many) we use the Db
                            // partition
                            if attribute.is_internal() {
                                highest_db_eid += 1;
                                EntityId(highest_db_eid)
                            } else {
                                highest_eid += 1;
                                EntityId(highest_eid)
                            }
                        });
                }
            }
            eids
        };

        for operation in tx {
            let (e, a, v, status) = match operation {
                Operation::Assertion(eid, a, v)       => (eid,        a, v, Status::Asserted),
                Operation::Retraction(eid, a, v)      => (eid,        a, v, Status::Retracted(tx_eid)),
                Operation::TempidAssertion(tid, a, v) => (eids[&tid], a, v, Status::Asserted)
            };

            let attribute = attribute_ids[&a];

            // If the operation is an assertion we have to handle the following things:
            //
            // - If the datom isn't db.cardinality/many we have to generate a retraction for the previous value
            //
            // - If the attribute of this datom is `db/ident` we have to make sure it isn't changing the schema
            //
            if status == Status::Asserted {
                if let Some(previous_datom) = self.datoms(Index::Eavt.e(e).a(attribute)).unwrap().iter().next() {
                    // Prevent database schema changes
                    if attribute == attr::ident && v != previous_datom.value {
                        let old_attribute_name = previous_datom.value.as_string().unwrap();
                        let new_attribute_name = v.as_string().unwrap();
                        return Err(TransactionError::ChangingIdentAttribute(old_attribute_name, new_attribute_name).into())
                    }

                    // Handle db.cardinality/many
                    let attribute_info = self.attribute_info(a)?;

                    if !attribute_info.cardinality_many {
                        let retraction = Datom {
                            entity: e,
                            attribute: attribute,
                            value: previous_datom.value.clone(),
                            tx: tx_eid,
                            status: Status::Retracted(tx_eid)
                        };

                        datoms.push(retraction);
                    }
                }
            }

            let datom = Datom {
                entity: e,
                attribute: attribute,
                value: v.clone(),

                tx: tx_eid,
                status
            };

            datoms.push(datom);
        }

        self.store_datoms(&datoms)?;

        Ok(TransactionData {
            tx_id: tx_eid,
            tempid_mappings: eids,
        })
    }
}

impl Db {
    pub fn has_attribute(&self, attribute_name: &str) -> bool {
        self.attribute(attribute_name).is_some()
    }

    pub fn attribute(&self, attribute_name: &str) -> Option<Attribute> {
        self.datoms(Index::Avet.a(attr::ident).v(Value::Str(attribute_name.into())))
            .unwrap()
            .iter().next()
            .map(|d| Attribute(d.entity))
    }

    pub fn attribute_name(&self, attribute: Attribute) -> Option<String> {
        self.datoms(Index::Avet.e(attribute.0).a(attr::ident)).unwrap()
            .into_iter()
            .next()
            .and_then(|d| match d.value {
                Value::Str(ref s) => Some(s.clone()),
                _ => None
            })
    }

    pub fn attribute_info(&self, attribute: AttributeName) -> Result<AttributeInfo, Error> {
        let mut info = AttributeInfo {
            cardinality_many: false
        };

        let attribute_eid = self.attribute(&attribute).unwrap().0;
        let attribute_datoms = self.datoms(Index::Eavt.e(attribute_eid))?;
        for datom in attribute_datoms {
            match datom.attribute {
                attr::cardinality_many => {
                    info.cardinality_many = datom.value != Value::Bool(false)
                },
                _ => ()
            }
        }

        Ok(info)
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
