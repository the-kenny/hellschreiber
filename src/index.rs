use super::{EntityId, Attribute, Value, TxId, Datom};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Index {
    Eavt,
    Aevt,
    Avet,
}

impl Index {
    pub fn e(self, e: EntityId)  -> FilteredIndex { FilteredIndex::new(self).e(e) }
    pub fn a(self, a: Attribute) -> FilteredIndex { FilteredIndex::new(self).a(a) }
    pub fn v(self, v: Value)     -> FilteredIndex { FilteredIndex::new(self).v(v) }
    pub fn t(self, t: TxId)      -> FilteredIndex { FilteredIndex::new(self).t(t) }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilteredIndex {
    pub index: Index,
    
    pub e: Option<EntityId>,
    pub a: Option<Attribute>,
    pub v: Option<Value>,
    pub t: Option<TxId>,
}

impl FilteredIndex {
    pub fn new(index: Index) -> Self {
        Self { index, e: None, a: None, v: None, t: None }
    }
    
    pub fn e(mut self, e: EntityId)  -> Self { self.e = Some(e); self }
    pub fn a(mut self, a: Attribute) -> Self { self.a = Some(a); self }
    pub fn v(mut self, v: Value)     -> Self { self.v = Some(v); self }
    pub fn t(mut self, t: TxId)      -> Self { self.t = Some(t); self }

    pub(crate) fn eavt(&self) -> (Option<EntityId>, Option<Attribute>, Option<Value>, Option<TxId>) {
        (self.e, self.a, self.v.clone(), self.t)
    }

    pub fn matches(&self, datom: &Datom) -> bool {
        let e = self.e;
        let a = self.a;
        let v = &self.v;
        let t = self.t;

        let e = e.is_none() || e.unwrap() == datom.entity;
        let a = a.is_none() || a.unwrap() == datom.attribute;
        let v = v.is_none() || v.as_ref().unwrap() == &datom.value;
        let t = t.is_none() || t.unwrap() == datom.tx;

        e && a && v && t
    }
}

impl From<Index> for FilteredIndex {
    fn from(i: Index) -> Self {
        FilteredIndex::new(i)
    }
}
