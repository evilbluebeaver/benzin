use std::fmt::Debug;

use diesel::prelude::*;

#[cfg(feature = "mysql")]
mod mysql;
#[cfg(feature = "postgres")]
mod pg;
mod pooling;

diesel::table! {
    users {
        id -> Integer,
        name -> Text,
    }
}

#[derive(Debug, PartialEq, Queryable, Selectable)]
struct User {
    id: i32,
    name: String,
}

#[derive(AsChangeset, Identifiable)]
#[diesel(table_name = users)]
struct UserForm<'a> {
    id: i32,
    name: &'a str,
}
