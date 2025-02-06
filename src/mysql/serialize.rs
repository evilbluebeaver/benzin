use diesel::{
    data_types,
    deserialize::FromSql,
    mysql::{Mysql, MysqlType, MysqlValue},
    sql_types,
};
use mysql_async::{Params, Value};

pub(super) struct ToSqlHelper {
    pub(super) metadata: Vec<MysqlType>,
    pub(super) binds: Vec<Option<Vec<u8>>>,
}

fn parse<S, T>(value: MysqlValue) -> T
where
    T: FromSql<S, Mysql>,
{
    FromSql::<S, Mysql>::from_sql(value).expect("This does not fail")
}

fn to_value((metadata, bind): (MysqlType, Option<Vec<u8>>)) -> Value {
    match bind {
        Some(bind) => {
            let value = MysqlValue::new(&bind, metadata);
            match metadata {
                MysqlType::Tiny => parse::<sql_types::TinyInt, i8>(value).into(),
                MysqlType::Short => parse::<sql_types::SmallInt, i16>(value).into(),
                MysqlType::Long => parse::<sql_types::Integer, i32>(value).into(),
                MysqlType::LongLong => parse::<sql_types::BigInt, i64>(value).into(),
                MysqlType::UnsignedTiny => {
                    parse::<sql_types::Unsigned<sql_types::TinyInt>, u8>(value).into()
                }
                MysqlType::UnsignedShort => {
                    parse::<sql_types::Unsigned<sql_types::SmallInt>, u16>(value).into()
                }
                MysqlType::UnsignedLong => {
                    parse::<sql_types::Unsigned<sql_types::Integer>, u32>(value).into()
                }
                MysqlType::UnsignedLongLong => {
                    parse::<sql_types::Unsigned<sql_types::BigInt>, u64>(value).into()
                }

                MysqlType::Float => parse::<sql_types::Float, f32>(value).into(),
                MysqlType::Double => parse::<sql_types::Double, f64>(value).into(),

                MysqlType::Time => {
                    let time = parse::<sql_types::Time, data_types::MysqlTime>(value);
                    Value::Time(
                        time.neg,
                        time.day as _,
                        time.hour as _,
                        time.minute as _,
                        time.second as _,
                        time.second_part as _,
                    )
                }
                MysqlType::Date | MysqlType::DateTime | MysqlType::Timestamp => {
                    let time = parse::<sql_types::Timestamp, data_types::MysqlTime>(value);
                    Value::Date(
                        time.year as _,
                        time.month as _,
                        time.day as _,
                        time.hour as _,
                        time.minute as _,
                        time.second as _,
                        time.second_part as _,
                    )
                }
                MysqlType::Numeric
                | MysqlType::Set
                | MysqlType::Enum
                | MysqlType::String
                | MysqlType::Blob => Value::Bytes(bind),
                MysqlType::Bit => unimplemented!(),
                _ => unreachable!(),
            }
        }
        None => Value::NULL,
    }
}

impl From<ToSqlHelper> for Params {
    fn from(ToSqlHelper { metadata, binds }: ToSqlHelper) -> Self {
        let values = metadata.into_iter().zip(binds).map(to_value).collect();
        Params::Positional(values)
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_from() {
        let helper = ToSqlHelper {
            metadata: vec![MysqlType::Long, MysqlType::String],
            binds: vec![Some(vec![42, 0, 0, 0]), Some(b"Hello".to_vec())],
        };
        let params: Params = helper.into();
        assert_eq!(
            params,
            Params::Positional(vec![Value::Int(42), Value::Bytes(b"Hello".to_vec()),])
        );
    }
    #[test]
    fn test_to_value() {
        assert_eq!(Value::Int(42), to_value((MysqlType::Tiny, Some(vec![42]))));
        assert_eq!(
            Value::Int(42),
            to_value((MysqlType::Short, Some(vec![42, 0])))
        );
        assert_eq!(
            Value::Int(42),
            to_value((MysqlType::Long, Some(vec![42, 0, 0, 0])))
        );
        assert_eq!(
            Value::Int(42),
            to_value((MysqlType::LongLong, Some(vec![42, 0, 0, 0, 0, 0, 0, 0])))
        );
        assert_eq!(
            Value::UInt(42),
            to_value((MysqlType::UnsignedTiny, Some(vec![42])))
        );
        assert_eq!(
            Value::UInt(42),
            to_value((MysqlType::UnsignedShort, Some(vec![42, 0])))
        );
        assert_eq!(
            Value::UInt(42),
            to_value((MysqlType::UnsignedLong, Some(vec![42, 0, 0, 0])))
        );
        assert_eq!(
            Value::UInt(42),
            to_value((
                MysqlType::UnsignedLongLong,
                Some(vec![42, 0, 0, 0, 0, 0, 0, 0])
            ))
        );
        assert_eq!(
            Value::Float(42.0),
            to_value((MysqlType::Float, Some(vec![0, 0, 0x28, 0x42])))
        );
        assert_eq!(
            Value::Double(42.0),
            to_value((MysqlType::Double, Some(vec![0, 0, 0, 0, 0, 0, 0x45, 0x40])))
        );
        assert_eq!(
            Value::Bytes(vec![1, 2, 3, 4]),
            to_value((MysqlType::Blob, Some(vec![1, 2, 3, 4])))
        );
        assert_eq!(
            Value::Bytes(vec![1, 2, 3, 4]),
            to_value((MysqlType::Numeric, Some(vec![1, 2, 3, 4])))
        );
        assert_eq!(
            Value::Bytes(vec![1, 2, 3, 4]),
            to_value((MysqlType::Set, Some(vec![1, 2, 3, 4])))
        );
        assert_eq!(
            Value::Bytes(vec![1, 2, 3, 4]),
            to_value((MysqlType::Enum, Some(vec![1, 2, 3, 4])))
        );
        assert_eq!(
            Value::Bytes(vec![1, 2, 3, 4]),
            to_value((MysqlType::String, Some(vec![1, 2, 3, 4])))
        );
        assert_eq!(
            Value::Time(false, 0, 3, 14, 15, 0),
            to_value((
                MysqlType::Time,
                Some(vec![
                    0, 0, 0, 0, // year
                    0, 0, 0, 0, // month
                    0, 0, 0, 0, // day
                    3, 0, 0, 0, // hour
                    14, 0, 0, 0, // minute
                    15, 0, 0, 0, // second
                    0, 0, 0, 0, 0, 0, 0, 0, // second_part
                    0, 0, 0, 0, // neg
                    2, 0, 0, 0, // type
                    0, 0, 0, 0, 0, 0, 0, 0 //other
                ])
            ))
        );
        assert_eq!(
            Value::Date(1, 1, 1, 3, 14, 15, 0),
            to_value((
                MysqlType::Timestamp,
                Some(vec![
                    1, 0, 0, 0, // year
                    1, 0, 0, 0, // month
                    1, 0, 0, 0, // day
                    3, 0, 0, 0, // hour
                    14, 0, 0, 0, // minute
                    15, 0, 0, 0, // second
                    0, 0, 0, 0, 0, 0, 0, 0, // second_part
                    0, 0, 0, 0, // neg
                    1, 0, 0, 0, // type
                    0, 0, 0, 0, 0, 0, 0, 0 //other
                ])
            ))
        );
        assert_eq!(Value::NULL, to_value((MysqlType::Long, None)));
    }
    #[test]
    #[should_panic]
    fn test_to_value_bit() {
        let _ = to_value((MysqlType::Bit, Some(vec![1, 2, 3, 4])));
    }
}
