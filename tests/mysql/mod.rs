use std::fmt::Debug;

use benzin::*;
use diesel::{prelude::*, QueryResult};

use crate::{users, User, UserForm};

type TestConnection = AsyncMysqlConnection;

diesel::define_sql_function!(fn pg_sleep(interval: diesel::sql_types::Double));

async fn setup(connection: &mut TestConnection) {
    diesel::sql_query(
        "CREATE TEMPORARY TABLE users (
                id INTEGER PRIMARY KEY AUTO_INCREMENT,
                name TEXT NOT NULL
            ) CHARACTER SET utf8mb4",
    )
    .execute(connection)
    .await
    .unwrap();
}

async fn connection() -> TestConnection {
    let db_url = std::env::var("MYSQL_DATABASE_URL").expect("MYSQL_DATABASE_URL");
    let mut conn = TestConnection::establish(&db_url).await.unwrap();
    setup(&mut conn).await;
    conn
}

#[tokio::test]
async fn test_basic_insert_and_load() -> QueryResult<()> {
    let conn = &mut connection().await;
    let res = diesel::insert_into(users::table)
        .values([users::name.eq("John Doe"), users::name.eq("Jane Doe")])
        .execute(conn)
        .await;
    assert_eq!(res, Ok(2), "User count does not match");
    let users = users::table
        .order_by(users::name)
        .load::<User>(conn)
        .await?;
    assert_eq!(&users[0].name, "Jane Doe");
    assert_eq!(&users[1].name, "John Doe");

    Ok(())
}

#[tokio::test]
async fn test_save_changes() -> QueryResult<()> {
    let conn = &mut connection().await;
    let res = diesel::insert_into(users::table)
        .values([users::name.eq("John Doe"), users::name.eq("Jane Doe")])
        .execute(conn)
        .await;
    assert_eq!(res, Ok(2), "User count does not match");
    let user = UserForm {
        id: 1,
        name: "Jack Doe",
    };
    let changed_user: User = user.save_changes(conn).await?;
    let expected_user = User {
        id: 1,
        name: String::from("Jack Doe"),
    };
    assert_eq!(changed_user, expected_user);

    Ok(())
}

#[tokio::test]
async fn test_first() -> QueryResult<()> {
    let conn = &mut connection().await;
    let res = diesel::insert_into(users::table)
        .values([users::name.eq("John Doe"), users::name.eq("Jane Doe")])
        .execute(conn)
        .await;
    assert_eq!(res, Ok(2), "User count does not match");
    let user = users::table
        .order_by(users::name)
        .first::<User>(conn)
        .await?;
    assert_eq!(&user.name, "Jane Doe");

    Ok(())
}

#[tokio::test]
async fn test_update() -> QueryResult<()> {
    let conn = &mut connection().await;
    let res = diesel::insert_into(users::table)
        .values([users::name.eq("John Doe"), users::name.eq("Jane Doe")])
        .execute(conn)
        .await;
    assert_eq!(res, Ok(2), "User count does not match");
    diesel::update(users::table)
        .filter(users::name.eq("John Doe"))
        .set(users::name.eq("Jack Doe"))
        .execute(conn)
        .await?;
    let users = users::table
        .order_by(users::name)
        .load::<User>(conn)
        .await?;
    assert_eq!(&users[0].name, "Jack Doe");
    assert_eq!(&users[1].name, "Jane Doe");

    Ok(())
}

#[tokio::test]
async fn test_transaction() -> QueryResult<()> {
    let mut conn = connection().await;
    let res = conn
        .transaction(|transaction| {
            Box::pin(async move {
                let res = diesel::insert_into(users::table)
                    .values([users::name.eq("John Doe"), users::name.eq("Jane Doe")])
                    .execute(transaction)
                    .await;
                assert_eq!(res, Ok(2), "User count does not match");
                let users = users::table
                    .order_by(users::name)
                    .load::<User>(transaction)
                    .await?;
                assert_eq!(&users[0].name, "Jane Doe");
                assert_eq!(&users[1].name, "John Doe");
                Err::<(), _>(diesel::result::Error::RollbackTransaction)
            })
        })
        .await;
    assert_eq!(
        res,
        Err(diesel::result::Error::RollbackTransaction),
        "Failed to rollback transaction"
    );
    Ok(())
}
