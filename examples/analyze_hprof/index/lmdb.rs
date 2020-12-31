use super::*;

// fingerprint keys
const FP_TIMESTAMP: &str = "__hprof_header_fingerprint_timestamp";
const FP_RECORD_COUNT: &str = "__hprof_header_fingerprint_record_count";

// tree names
const FINGERPRINT: &str = "fingerprint";
const OBJ_ID_CLASS_ID: &str = "obj_id_class_id";
const OBJ_ID_PRIM_TYPE: &str = "obj_id_prim_type";

pub(crate) struct LmdbIndex {}

impl Index for LmdbIndex {
    fn open_with_fingerprint(
        _fingerprint: &HprofFingerprint,
        _source: &Path,
    ) -> Result<Self, anyhow::Error> {
        unimplemented!()
    }

    fn get_class_id(&self, _obj_id: Id) -> Result<Option<Id>, anyhow::Error> {
        unimplemented!()
    }

    fn get_prim_array_type(
        &self,
        _obj_id: Id,
    ) -> Result<Option<PrimitiveArrayType>, anyhow::Error> {
        unimplemented!()
    }
}
