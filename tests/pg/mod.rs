mod custom_types;

use std::fmt::Debug;

use benzin::*;
use diesel::{prelude::*, QueryResult};

use crate::{users, User, UserForm};

type TestConnection = AsyncPgConnection;

diesel::define_sql_function!(fn pg_sleep(interval: diesel::sql_types::Double));

#[tokio::test]
async fn postgres_cancel_token() {
    use std::time::Duration;

    use diesel::result::{DatabaseErrorKind, Error};

    let mut conn = connection().await;

    let token = conn.cancel_token();

    // execute a query that runs for a long time
    // move the connection into the spawned task
    let task = tokio::spawn(async move {
        diesel::select(pg_sleep(5.0))
            .execute(&mut conn)
            .await
            .expect_err("query should have been canceled.")
    });

    // let the task above have some time to actually start...
    tokio::time::sleep(Duration::from_millis(500)).await;

    // invoke the cancellation token.
    token.cancel_query(tokio_postgres::NoTls).await.unwrap();

    // make sure the query task resulted in a cancellation error
    let err = task.await.unwrap();
    match err {
        Error::DatabaseError(DatabaseErrorKind::Unknown, v)
            if v.message() == "canceling statement due to user request" => {}
        _ => panic!("unexpected error: {:?}", err),
    }
}

async fn setup(connection: &mut TestConnection) {
    diesel::sql_query(
        "CREATE TEMPORARY TABLE users (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL
            )",
    )
    .execute(connection)
    .await
    .unwrap();
}

async fn connection() -> TestConnection {
    let db_url = std::env::var("PG_DATABASE_URL").expect("PG_DATABASE_URL");
    let mut conn = TestConnection::establish(&db_url).await.unwrap();
    setup(&mut conn).await;
    conn
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
                Err::<usize, _>(diesel::result::Error::RollbackTransaction)
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

#[tokio::test]
async fn test_nested_transaction() -> QueryResult<()> {
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

                let res = transaction
                    .transaction(|inner_transaction| {
                        Box::pin(async move {
                            let res = diesel::insert_into(users::table)
                                .values(users::name.eq("Eve"))
                                .execute(inner_transaction)
                                .await;
                            assert_eq!(res, Ok(1));
                            Err::<usize, _>(diesel::result::Error::RollbackTransaction)
                        })
                    })
                    .await;

                assert_eq!(res, Err(diesel::result::Error::RollbackTransaction),);

                let users = users::table
                    .order_by(users::name)
                    .load::<User>(transaction)
                    .await?;
                assert_eq!(&users[0].name, "Jane Doe");
                assert_eq!(&users[1].name, "John Doe");

                let res = transaction
                    .transaction(|inner_transaction| {
                        Box::pin(async move {
                            let res = diesel::insert_into(users::table)
                                .values(users::name.eq("Eve"))
                                .execute(inner_transaction)
                                .await?;
                            Ok::<usize, diesel::result::Error>(res)
                        })
                    })
                    .await;

                assert_eq!(res, Ok(1));

                let users = users::table
                    .order_by(users::name)
                    .load::<User>(transaction)
                    .await?;
                assert_eq!(&users[0].name, "Eve");
                assert_eq!(&users[1].name, "Jane Doe");
                assert_eq!(&users[2].name, "John Doe");
                Err::<usize, _>(diesel::result::Error::RollbackTransaction)
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

#[tokio::test]
async fn test_nested_transaction_flat() -> QueryResult<()> {
    let mut conn = connection().await;
    let mut transaction = conn.begin_transaction().await?;
    let res = diesel::insert_into(users::table)
        .values([users::name.eq("John Doe"), users::name.eq("Jane Doe")])
        .execute(&mut transaction)
        .await;
    assert_eq!(res, Ok(2), "User count does not match");
    let users = users::table
        .order_by(users::name)
        .load::<User>(&mut transaction)
        .await?;
    assert_eq!(&users[0].name, "Jane Doe");
    assert_eq!(&users[1].name, "John Doe");
    let mut inner_transaction = transaction.begin_transaction().await?;
    let res = diesel::insert_into(users::table)
        .values(users::name.eq("Eve"))
        .execute(&mut inner_transaction)
        .await;
    assert_eq!(res, Ok(1));
    inner_transaction.rollback().await?;

    let mut inner_transaction = transaction.begin_transaction().await?;
    let res = diesel::insert_into(users::table)
        .values(users::name.eq("Eve"))
        .execute(&mut inner_transaction)
        .await;
    assert_eq!(res, Ok(1));
    inner_transaction.commit().await?;

    let users = users::table
        .order_by(users::name)
        .load::<User>(&mut transaction)
        .await?;
    assert_eq!(&users[0].name, "Eve");
    assert_eq!(&users[1].name, "Jane Doe");
    assert_eq!(&users[2].name, "John Doe");

    transaction.rollback().await?;

    let count = users::table.count().get_result::<i64>(&mut conn).await?;
    assert_eq!(count, 0, "user got committed, but transaction rolled back");

    Ok(())
}
