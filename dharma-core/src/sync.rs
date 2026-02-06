use crate::cbor;
use crate::error::DharmaError;
use crate::fabric::types::Advertisement;
use crate::types::{AssertionId, EnvelopeId, HpkePublicKey, IdentityKey, SubjectId};
use crate::value::{expect_array, expect_map, expect_text, expect_uint, expect_bytes, map_get};
use ciborium::value::Value;

#[derive(Clone, Debug, PartialEq)]
pub enum SyncMessage {
    Hello(Hello),
    Inv(Inventory),
    Get(Get),
    Obj(Obj),
    Ad(Advertisement),
    Ads(Ads),
    GetAds(GetAds),
    Err(ErrMsg),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Hello {
    pub v: u64,
    pub peer_id: IdentityKey,
    pub hpke_pk: HpkePublicKey,
    pub suites: Vec<u64>,
    pub caps: Vec<String>,
    pub subs: Option<Subscriptions>,
    pub subject: Option<SubjectId>,
    pub note: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Inventory {
    Subjects(Vec<SubjectInventory>),
    Objects(Vec<EnvelopeId>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct SubjectInventory {
    pub sub: SubjectId,
    pub frontier: Vec<AssertionId>,
    pub overlay: Vec<AssertionId>,
    pub since_seq: Option<u64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Subscriptions {
    pub all: bool,
    pub subjects: Vec<SubjectId>,
    pub namespaces: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ObjectRef {
    Assertion(AssertionId),
    Envelope(EnvelopeId),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Get {
    pub ids: Vec<ObjectRef>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Obj {
    pub id: ObjectRef,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Ads {
    pub ads: Vec<Advertisement>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GetAds;

#[derive(Clone, Debug, PartialEq)]
pub struct ErrMsg {
    pub message: String,
}

impl SyncMessage {
    pub fn to_cbor(&self) -> Result<Vec<u8>, DharmaError> {
        cbor::encode_canonical_value(&self.to_value())
    }

    pub fn from_cbor(bytes: &[u8]) -> Result<Self, DharmaError> {
        let value = cbor::ensure_canonical(bytes)?;
        parse_message(&value)
    }

    pub fn to_value(&self) -> Value {
        let (t, payload) = match self {
            SyncMessage::Hello(hello) => ("hello", hello.to_value()),
            SyncMessage::Inv(inv) => ("inv", inv.to_value()),
            SyncMessage::Get(get) => ("get", get.to_value()),
            SyncMessage::Obj(obj) => ("obj", obj.to_value()),
            SyncMessage::Ad(ad) => ("ad", ad.to_value()),
            SyncMessage::Ads(ads) => ("ads", ads.to_value()),
            SyncMessage::GetAds(get) => ("get_ads", get.to_value()),
            SyncMessage::Err(err) => ("err", err.to_value()),
        };
        Value::Map(vec![
            (Value::Text("t".to_string()), Value::Text(t.to_string())),
            (Value::Text("p".to_string()), payload),
        ])
    }
}

impl Hello {
    fn to_value(&self) -> Value {
        let mut entries = vec![
            (Value::Text("v".to_string()), Value::Integer(self.v.into())),
            (
                Value::Text("peer_id".to_string()),
                Value::Bytes(self.peer_id.as_bytes().to_vec()),
            ),
            (
                Value::Text("hpke_pk".to_string()),
                Value::Bytes(self.hpke_pk.as_bytes().to_vec()),
            ),
            (
                Value::Text("suites".to_string()),
                Value::Array(self.suites.iter().map(|s| Value::Integer((*s).into())).collect()),
            ),
            (
                Value::Text("caps".to_string()),
                Value::Array(self.caps.iter().map(|c| Value::Text(c.clone())).collect()),
            ),
        ];
        if let Some(subject) = &self.subject {
            entries.push((
                Value::Text("subject".to_string()),
                Value::Bytes(subject.as_bytes().to_vec()),
            ));
        }
        if let Some(subs) = &self.subs {
            entries.push((Value::Text("subs".to_string()), subs.to_value()));
        }
        if let Some(note) = &self.note {
            entries.push((Value::Text("note".to_string()), Value::Text(note.clone())));
        }
        Value::Map(entries)
    }
}

impl Inventory {
    fn to_value(&self) -> Value {
        match self {
            Inventory::Subjects(subjects) => {
                Value::Map(vec![(
                    Value::Text("subjects".to_string()),
                    Value::Array(subjects.iter().map(|s| s.to_value()).collect()),
                )])
            }
            Inventory::Objects(objects) => Value::Map(vec![(
                Value::Text("objects".to_string()),
                Value::Array(objects.iter().map(|o| Value::Bytes(o.as_bytes().to_vec())).collect()),
            )]),
        }
    }
}

impl SubjectInventory {
    fn to_value(&self) -> Value {
        let mut entries = vec![
            (Value::Text("sub".to_string()), Value::Bytes(self.sub.as_bytes().to_vec())),
            (
                Value::Text("frontier".to_string()),
                Value::Array(
                    self.frontier
                        .iter()
                        .map(|id| Value::Bytes(id.as_bytes().to_vec()))
                        .collect(),
                ),
            ),
        ];
        if !self.overlay.is_empty() {
            entries.push((
                Value::Text("overlay".to_string()),
                Value::Array(
                    self.overlay
                        .iter()
                        .map(|id| Value::Bytes(id.as_bytes().to_vec()))
                        .collect(),
                ),
            ));
        }
        if let Some(since) = self.since_seq {
            entries.push((
                Value::Text("since_seq".to_string()),
                Value::Integer((since as u64).into()),
            ));
        }
        Value::Map(entries)
    }
}

impl ObjectRef {
    fn to_value(&self) -> Value {
        match self {
            ObjectRef::Assertion(id) => Value::Map(vec![
                (Value::Text("t".to_string()), Value::Text("assertion".to_string())),
                (Value::Text("id".to_string()), Value::Bytes(id.as_bytes().to_vec())),
            ]),
            ObjectRef::Envelope(id) => Value::Map(vec![
                (Value::Text("t".to_string()), Value::Text("envelope".to_string())),
                (Value::Text("id".to_string()), Value::Bytes(id.as_bytes().to_vec())),
            ]),
        }
    }

    fn from_value(value: &Value) -> Result<Self, DharmaError> {
        let map = expect_map(value)?;
        let kind = expect_text(
            map_get(map, "t").ok_or_else(|| DharmaError::Validation("missing ref type".to_string()))?,
        )?;
        let id_val =
            map_get(map, "id").ok_or_else(|| DharmaError::Validation("missing ref id".to_string()))?;
        let bytes = expect_bytes(id_val)?;
        match kind.as_str() {
            "assertion" => Ok(ObjectRef::Assertion(AssertionId::from_slice(&bytes)?)),
            "envelope" => Ok(ObjectRef::Envelope(EnvelopeId::from_slice(&bytes)?)),
            _ => Err(DharmaError::Validation("invalid ref type".to_string())),
        }
    }
}

impl Subscriptions {
    pub fn all() -> Self {
        Subscriptions {
            all: true,
            subjects: Vec::new(),
            namespaces: Vec::new(),
        }
    }

    pub fn allows_subject(&self, subject: &SubjectId, namespace: Option<&str>) -> bool {
        if self.all {
            return true;
        }
        if self.subjects.iter().any(|s| s == subject) {
            return true;
        }
        if let Some(ns) = namespace {
            if self.namespaces.iter().any(|n| n == ns) {
                return true;
            }
        }
        false
    }

    fn to_value(&self) -> Value {
        let mut entries = Vec::new();
        if self.all {
            entries.push((Value::Text("all".to_string()), Value::Bool(true)));
        }
        if !self.subjects.is_empty() {
            entries.push((
                Value::Text("subjects".to_string()),
                Value::Array(
                    self.subjects
                        .iter()
                        .map(|s| Value::Bytes(s.as_bytes().to_vec()))
                        .collect(),
                ),
            ));
        }
        if !self.namespaces.is_empty() {
            entries.push((
                Value::Text("namespaces".to_string()),
                Value::Array(self.namespaces.iter().map(|n| Value::Text(n.clone())).collect()),
            ));
        }
        Value::Map(entries)
    }
}

impl Get {
    fn to_value(&self) -> Value {
        Value::Map(vec![(
            Value::Text("ids".to_string()),
            Value::Array(self.ids.iter().map(ObjectRef::to_value).collect()),
        )])
    }
}

impl Obj {
    fn to_value(&self) -> Value {
        Value::Map(vec![
            (Value::Text("id".to_string()), self.id.to_value()),
            (Value::Text("bytes".to_string()), Value::Bytes(self.bytes.clone())),
        ])
    }
}

impl Ads {
    fn to_value(&self) -> Value {
        Value::Map(vec![(
            Value::Text("ads".to_string()),
            Value::Array(self.ads.iter().map(|ad| ad.to_value()).collect()),
        )])
    }
}

impl GetAds {
    fn to_value(&self) -> Value {
        Value::Map(Vec::new())
    }
}

impl ErrMsg {
    fn to_value(&self) -> Value {
        Value::Map(vec![(
            Value::Text("message".to_string()),
            Value::Text(self.message.clone()),
        )])
    }
}

fn parse_message(value: &Value) -> Result<SyncMessage, DharmaError> {
    let map = expect_map(value)?;
    let t = expect_text(map_get(map, "t").ok_or_else(|| DharmaError::Validation("missing t".to_string()))?)?;
    let p = map_get(map, "p").ok_or_else(|| DharmaError::Validation("missing p".to_string()))?;
    match t.as_str() {
        "hello" => Ok(SyncMessage::Hello(parse_hello(p)?)),
        "inv" => Ok(SyncMessage::Inv(parse_inventory(p)?)),
        "get" => Ok(SyncMessage::Get(parse_get(p)?)),
        "obj" => Ok(SyncMessage::Obj(parse_obj(p)?)),
        "ad" => Ok(SyncMessage::Ad(parse_ad(p)?)),
        "ads" => Ok(SyncMessage::Ads(parse_ads(p)?)),
        "get_ads" => Ok(SyncMessage::GetAds(GetAds)),
        "err" => Ok(SyncMessage::Err(parse_err(p)?)),
        _ => Err(DharmaError::Validation("unknown message".to_string())),
    }
}

fn parse_hello(value: &Value) -> Result<Hello, DharmaError> {
    let map = expect_map(value)?;
    let v = expect_uint(map_get(map, "v").ok_or_else(|| DharmaError::Validation("missing v".to_string()))?)?;
    let peer = expect_bytes(map_get(map, "peer_id").ok_or_else(|| DharmaError::Validation("missing peer_id".to_string()))?)?;
    let hpke = expect_bytes(map_get(map, "hpke_pk").ok_or_else(|| DharmaError::Validation("missing hpke_pk".to_string()))?)?;
    let suites_val = map_get(map, "suites").ok_or_else(|| DharmaError::Validation("missing suites".to_string()))?;
    let suites_array = expect_array(suites_val)?;
    let mut suites = Vec::new();
    for item in suites_array {
        suites.push(expect_uint(item)?);
    }
    let caps_val = map_get(map, "caps").ok_or_else(|| DharmaError::Validation("missing caps".to_string()))?;
    let caps_array = expect_array(caps_val)?;
    let mut caps = Vec::new();
    for item in caps_array {
        caps.push(expect_text(item)?);
    }
    let subject = map_get(map, "subject")
        .map(|v| expect_bytes(v))
        .transpose()?
        .map(|bytes| SubjectId::from_slice(&bytes))
        .transpose()?;
    let subs = map_get(map, "subs").map(parse_subscriptions).transpose()?;
    let note = map_get(map, "note").map(|v| expect_text(v)).transpose()?;
    Ok(Hello {
        v,
        peer_id: IdentityKey::from_slice(&peer)?,
        hpke_pk: HpkePublicKey::from_slice(&hpke)?,
        suites,
        caps,
        subs,
        subject,
        note,
    })
}

fn parse_subscriptions(value: &Value) -> Result<Subscriptions, DharmaError> {
    let map = expect_map(value)?;
    let all = match map_get(map, "all") {
        Some(Value::Bool(flag)) => *flag,
        _ => false,
    };
    let mut subjects = Vec::new();
    if let Some(subjects_val) = map_get(map, "subjects") {
        let subjects_array = expect_array(subjects_val)?;
        for item in subjects_array {
            let bytes = expect_bytes(item)?;
            subjects.push(SubjectId::from_slice(&bytes)?);
        }
    }
    let mut namespaces = Vec::new();
    if let Some(namespaces_val) = map_get(map, "namespaces") {
        let namespaces_array = expect_array(namespaces_val)?;
        for item in namespaces_array {
            namespaces.push(expect_text(item)?);
        }
    }
    Ok(Subscriptions {
        all,
        subjects,
        namespaces,
    })
}

fn parse_inventory(value: &Value) -> Result<Inventory, DharmaError> {
    let map = expect_map(value)?;
    if let Some(subjects_val) = map_get(map, "subjects") {
        let subjects_array = expect_array(subjects_val)?;
        let mut subjects = Vec::new();
        for item in subjects_array {
            subjects.push(parse_subject_inventory(item)?);
        }
        return Ok(Inventory::Subjects(subjects));
    }
    if let Some(objects_val) = map_get(map, "objects") {
        let objects_array = expect_array(objects_val)?;
        let mut objects = Vec::new();
        for item in objects_array {
            let bytes = expect_bytes(item)?;
            objects.push(EnvelopeId::from_slice(&bytes)?);
        }
        return Ok(Inventory::Objects(objects));
    }
    Err(DharmaError::Validation("invalid inventory".to_string()))
}

fn parse_subject_inventory(value: &Value) -> Result<SubjectInventory, DharmaError> {
    let map = expect_map(value)?;
    let sub_bytes = expect_bytes(map_get(map, "sub").ok_or_else(|| DharmaError::Validation("missing sub".to_string()))?)?;
    let frontier_val = map_get(map, "frontier").ok_or_else(|| DharmaError::Validation("missing frontier".to_string()))?;
    let frontier_array = expect_array(frontier_val)?;
    let mut frontier = Vec::new();
    for item in frontier_array {
        let bytes = expect_bytes(item)?;
        frontier.push(AssertionId::from_slice(&bytes)?);
    }
    let mut overlay = Vec::new();
    if let Some(overlay_val) = map_get(map, "overlay") {
        let overlay_array = expect_array(overlay_val)?;
        for item in overlay_array {
            let bytes = expect_bytes(item)?;
            overlay.push(AssertionId::from_slice(&bytes)?);
        }
    }
    let since_seq = map_get(map, "since_seq")
        .map(|v| expect_uint(v))
        .transpose()?;
    Ok(SubjectInventory {
        sub: SubjectId::from_slice(&sub_bytes)?,
        frontier,
        overlay,
        since_seq,
    })
}

fn parse_get(value: &Value) -> Result<Get, DharmaError> {
    let map = expect_map(value)?;
    let ids_val = map_get(map, "ids").ok_or_else(|| DharmaError::Validation("missing ids".to_string()))?;
    let ids_array = expect_array(ids_val)?;
    let mut ids = Vec::new();
    for item in ids_array {
        ids.push(ObjectRef::from_value(item)?);
    }
    Ok(Get { ids })
}

fn parse_obj(value: &Value) -> Result<Obj, DharmaError> {
    let map = expect_map(value)?;
    let id_val = map_get(map, "id").ok_or_else(|| DharmaError::Validation("missing id".to_string()))?;
    let bytes = expect_bytes(map_get(map, "bytes").ok_or_else(|| DharmaError::Validation("missing bytes".to_string()))?)?;
    Ok(Obj {
        id: ObjectRef::from_value(id_val)?,
        bytes,
    })
}

fn parse_ad(value: &Value) -> Result<Advertisement, DharmaError> {
    Advertisement::from_value(value)
}

fn parse_ads(value: &Value) -> Result<Ads, DharmaError> {
    let map = expect_map(value)?;
    let ads_val = map_get(map, "ads").ok_or_else(|| DharmaError::Validation("missing ads".to_string()))?;
    let ads_array = expect_array(ads_val)?;
    let mut ads = Vec::with_capacity(ads_array.len());
    for item in ads_array {
        ads.push(Advertisement::from_value(item)?);
    }
    Ok(Ads { ads })
}

fn parse_err(value: &Value) -> Result<ErrMsg, DharmaError> {
    let map = expect_map(value)?;
    let message = expect_text(map_get(map, "message").ok_or_else(|| DharmaError::Validation("missing message".to_string()))?)?;
    Ok(ErrMsg { message })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hello_roundtrip() {
        let msg = SyncMessage::Hello(Hello {
            v: 1,
            peer_id: IdentityKey::from_bytes([1u8; 32]),
            hpke_pk: HpkePublicKey::from_bytes([2u8; 32]),
            suites: vec![1, 2],
            caps: vec!["sync.range".to_string()],
            subs: Some(Subscriptions {
                all: false,
                subjects: vec![SubjectId::from_bytes([9u8; 32])],
                namespaces: vec!["std.invoice".to_string()],
            }),
            subject: Some(SubjectId::from_bytes([8u8; 32])),
            note: Some("hi".to_string()),
        });
        let bytes = msg.to_cbor().unwrap();
        let parsed = SyncMessage::from_cbor(&bytes).unwrap();
        assert_eq!(msg, parsed);
    }

    #[test]
    fn inv_subjects_roundtrip() {
        let msg = SyncMessage::Inv(Inventory::Subjects(vec![SubjectInventory {
            sub: SubjectId::from_bytes([3u8; 32]),
            frontier: vec![AssertionId::from_bytes([4u8; 32])],
            overlay: vec![AssertionId::from_bytes([9u8; 32])],
            since_seq: Some(42),
        }]));
        let bytes = msg.to_cbor().unwrap();
        let parsed = SyncMessage::from_cbor(&bytes).unwrap();
        assert_eq!(msg, parsed);
    }

    #[test]
    fn inv_objects_roundtrip() {
        let msg = SyncMessage::Inv(Inventory::Objects(vec![EnvelopeId::from_bytes([5u8; 32])])) ;
        let bytes = msg.to_cbor().unwrap();
        let parsed = SyncMessage::from_cbor(&bytes).unwrap();
        assert_eq!(msg, parsed);
    }

    #[test]
    fn get_roundtrip() {
        let msg = SyncMessage::Get(Get {
            ids: vec![
                ObjectRef::Assertion(AssertionId::from_bytes([6u8; 32])),
                ObjectRef::Envelope(EnvelopeId::from_bytes([7u8; 32])),
            ],
        });
        let bytes = msg.to_cbor().unwrap();
        let parsed = SyncMessage::from_cbor(&bytes).unwrap();
        assert_eq!(msg, parsed);
    }

    #[test]
    fn obj_roundtrip() {
        let msg = SyncMessage::Obj(Obj {
            id: ObjectRef::Assertion(AssertionId::from_bytes([7u8; 32])),
            bytes: vec![1, 2, 3],
        });
        let bytes = msg.to_cbor().unwrap();
        let parsed = SyncMessage::from_cbor(&bytes).unwrap();
        assert_eq!(msg, parsed);
    }

    #[test]
    fn err_roundtrip() {
        let msg = SyncMessage::Err(ErrMsg { message: "oops".to_string() });
        let bytes = msg.to_cbor().unwrap();
        let parsed = SyncMessage::from_cbor(&bytes).unwrap();
        assert_eq!(msg, parsed);
    }
}
