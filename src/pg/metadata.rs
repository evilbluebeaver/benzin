use std::borrow::Cow;

use diesel::pg::{FailedToLookupTypeError, PgMetadataCacheKey, PgMetadataLookup, PgTypeMetadata};

pub struct PgAsyncMetadataLookup {
    pub unresolved_types: Vec<(Option<String>, String)>,
}

impl PgAsyncMetadataLookup {
    pub fn new() -> Self {
        Self {
            unresolved_types: Vec::new(),
        }
    }
}

impl PgMetadataLookup for PgAsyncMetadataLookup {
    fn lookup_type(&mut self, type_name: &str, schema: Option<&str>) -> PgTypeMetadata {
        let cache_key =
            PgMetadataCacheKey::new(schema.map(Cow::Borrowed), Cow::Borrowed(type_name));

        let cache_key = cache_key.into_owned();
        self.unresolved_types
            .push((schema.map(ToOwned::to_owned), type_name.to_owned()));
        PgTypeMetadata::from_result(Err(FailedToLookupTypeError::new(cache_key)))
    }
}
