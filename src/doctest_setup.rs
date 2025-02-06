#[allow(unused_imports)]
use diesel::prelude::{
    AsChangeset, ExpressionMethods, Identifiable, IntoSql, QueryDsl, QueryResult, Queryable,
    QueryableByName,
};

use benzin::TestConnection;

mod schema {
    use diesel::prelude::*;

    table! {
        animals {
            id -> Integer,
            species -> VarChar,
            legs -> Integer,
            name -> Nullable<VarChar>,
        }
    }

    table! {
        users {
            id -> Integer,
            name -> VarChar,
        }
    }
}

fn establish_connection() -> benzin::TestConnection {
    benzin::TestConnection::default()
}
