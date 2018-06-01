use super::{Db, EntityId, Entity};
use chrono;

#[derive(Debug, PartialEq, Eq, Clone, PartialOrd, Ord, Serialize, Deserialize, From)]
pub enum Value {
    Bool(bool),
    Str(String),
    Int(i64),
    Ref(EntityId),
    DateTime(chrono::DateTime<chrono::Utc>)
}

impl Value {
    pub fn as_str(&self) -> Option<&str> {
        if let Value::Str(ref s) = self {
            Some(&s[..])
        } else {
            None
        }
    }

    pub fn as_string(&self) -> Option<String> {
        if let Value::Str(ref s) = self {
            Some(s.clone())
        } else {
            None
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        if let Value::Int(i) = self {
            Some(*i)
        } else {
            None
        }
    }

    pub fn as_datetime(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        if let Value::DateTime(i) = self {
            Some(*i)
        } else {
            None
        }
    }

    pub fn follow_ref<'a>(&self, db: &'a Db) -> Option<Entity<'a>> {
        if let Value::Ref(eid) = self {
            Some(db.entity(*eid).unwrap()) // TODO
        } else {
            None
        }
    }
}

impl<'a> From<&'a str> for Value {
    fn from(s: &'a str) -> Value { Value::Str(s.into()) }
}
