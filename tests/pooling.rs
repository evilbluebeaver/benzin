#[cfg(feature = "bb8")]
mod bb8 {
    #[tokio::test]
    async fn test_basic_insert_and_load() {
        use benzin::{
            pooled_connection::{bb8::Pool, AsyncDieselConnectionManager},
            RunQueryDsl,
        };
        use diesel::prelude::*;

        use super::super::users;

        let config = AsyncDieselConnectionManager::<benzin::TestConnection>::new("test");
        let pool = Pool::builder().max_size(1).build(config).await.unwrap();

        let conn = pool.get().await.unwrap();
        drop(conn);
        let mut conn = pool.get().await.unwrap();
        conn.expected_return_count(1);
        diesel::insert_into(users::table)
            .values(users::name.eq("John"))
            .execute(&mut *conn)
            .await
            .unwrap();
        conn.expected_load(vec![vec![String::from("John")]]);
        let all_names = users::table
            .select(users::name)
            .load::<String>(&mut *conn)
            .await
            .unwrap();
        assert_eq!(all_names, vec!["John"]);
        drop(conn);
    }
}

#[cfg(feature = "deadpool")]
mod deadpool {
    #[tokio::test]
    async fn test_basic_insert_and_load() {
        use benzin::{
            pooled_connection::{deadpool::Pool, AsyncDieselConnectionManager},
            RunQueryDsl,
        };
        use diesel::prelude::*;

        use super::super::users;

        let config = AsyncDieselConnectionManager::<benzin::TestConnection>::new("test");
        let pool = Pool::builder(config).max_size(1).build().unwrap();

        let conn = pool.get().await.unwrap();
        drop(conn);
        let mut conn = pool.get().await.unwrap();
        conn.expected_return_count(1);
        diesel::insert_into(users::table)
            .values(users::name.eq("John"))
            .execute(&mut *conn)
            .await
            .unwrap();
        conn.expected_load(vec![vec![String::from("John")]]);
        let all_names = users::table
            .select(users::name)
            .load::<String>(&mut *conn)
            .await
            .unwrap();
        assert_eq!(all_names, vec!["John"]);
        drop(conn);
    }
}
