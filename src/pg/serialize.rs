use diesel::pg::PgTypeMetadata;
use tokio_postgres::types::{private::BytesMut, IsNull, Type, WrongType};

#[derive(Debug)]
pub(super) struct ToSqlHelper(pub(super) PgTypeMetadata, pub(super) Option<Vec<u8>>);

impl tokio_postgres::types::ToSql for ToSqlHelper {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        if let Some(ref bytes) = self.1 {
            out.extend_from_slice(bytes);
            Ok(IsNull::No)
        } else {
            Ok(IsNull::Yes)
        }
    }

    fn accepts(_ty: &Type) -> bool
    where
        Self: Sized,
    {
        // this should be called anymore
        true
    }

    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if Type::from_oid(self.0.oid()?)
            .map(|d| ty != &d)
            .unwrap_or(false)
        {
            return Err(Box::new(WrongType::new::<Self>(ty.clone())));
        }
        self.to_sql(ty, out)
    }
}

#[cfg(test)]
mod test {

    use tokio_postgres::types::ToSql;

    use super::*;

    #[test]
    fn test_to_sql_helper() {
        let helper = ToSqlHelper(
            PgTypeMetadata::new(20, 0),
            Some(vec![42, 0, 0, 0, 0, 0, 0, 0]),
        );
        let mut bytes = BytesMut::new();
        helper.to_sql(&Type::INT8, &mut bytes).unwrap();
        assert_eq!(bytes, vec![42, 0, 0, 0, 0, 0, 0, 0]);
    }
    #[test]
    fn test_to_sql_helper_none() {
        let helper = ToSqlHelper(PgTypeMetadata::new(20, 0), None);
        let mut bytes = BytesMut::new();
        helper.to_sql(&Type::INT8, &mut bytes).unwrap();
        assert_eq!(bytes, vec![]);
    }
    #[test]
    fn test_to_sql_helper_error() {
        let helper = ToSqlHelper(PgTypeMetadata::new(20, 0), None);
        let mut bytes = BytesMut::new();
        let res = helper.to_sql_checked(&Type::INT4, &mut bytes);
        assert!(res.is_err());
    }
}
