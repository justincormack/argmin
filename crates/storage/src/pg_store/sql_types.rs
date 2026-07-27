//! SQLite codecs for storage domain types.
//!
//! These implementations are deliberately owned by PgStore so the general
//! storage types do not depend on the selected metadata database backend.

use crate::types::{BucketName, ObjectKey, SessionId, UploadId};
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};

macro_rules! sqlite_validated_string {
    ($type:ty) => {
        impl ToSql for $type {
            fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
                self.as_str().to_sql()
            }
        }

        impl FromSql for $type {
            fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
                let value = String::column_result(value)?;
                Self::try_from(value).map_err(|error| FromSqlError::Other(Box::new(error)))
            }
        }
    };
}

sqlite_validated_string!(BucketName);
sqlite_validated_string!(ObjectKey);
sqlite_validated_string!(UploadId);
sqlite_validated_string!(SessionId);

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_invalid_text_row<T: FromSql + std::fmt::Debug>(column: &str, value: &str) {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        let create = format!("CREATE TABLE test_value ({column} TEXT NOT NULL)");
        conn.execute(&create, []).unwrap();
        let insert = format!("INSERT INTO test_value ({column}) VALUES (?1)");
        conn.execute(&insert, [value]).unwrap();
        let select = format!("SELECT {column} FROM test_value");

        let error = conn
            .query_row(&select, [], |row| row.get::<_, T>(0))
            .unwrap_err();
        assert!(matches!(
            error,
            rusqlite::Error::FromSqlConversionFailure(_, rusqlite::types::Type::Text, _)
        ));
    }

    #[test]
    fn bucket_name_from_sql_rejects_invalid_rows() {
        assert_invalid_text_row::<BucketName>("name", "BadBucket");
    }

    #[test]
    fn object_key_from_sql_rejects_invalid_rows() {
        assert_invalid_text_row::<ObjectKey>("key", "");
    }

    #[test]
    fn upload_id_from_sql_rejects_invalid_rows() {
        assert_invalid_text_row::<UploadId>("upload_id", "short");
    }
}
