use anyhow::Result;

use std::path::PathBuf;
use postgresql_embedded::{PostgreSQL, Settings};

use tokio_postgres::{Client, Config, NoTls};

const ROOT_DIR: &str = "C:/postgres";
const DB_NAME: &str = "example_db";

#[tokio::main]
async fn main() -> Result<()> {
    // Get database credentials from .env
    let user = dotenvy::var("DB_USER")?;
    let pw = dotenvy::var("DB_PW")?;

    let pg = create_db(&user, &pw).await?;

    let cli = DBClient::connect("127.0.0.1", pg.settings().port, &user, &pw).await?;

    // Execute some statements and queries on the database
    cli.client.execute("
        CREATE TABLE IF NOT EXISTS test_table (
            name TEXT UNIQUE,
            age INTEGER
        );",
        &[]).await?;

    cli.client.execute("
        INSERT INTO test_table (name, age) VALUES ($1::TEXT, $2::INTEGER) ON CONFLICT DO NOTHING",
        &[&"Hellowardo", &23]).await?;

    let query = cli.client.query("SELECT * FROM test_table", &[]).await?;

    for row in query {
        let name: String = row.get("name");
        let age: i32 = row.get("age");

        println!("name: {} age: {}", name, age);
    }
    

    // Ensure that the client disconnects and the server stops before returning from main
    cli.disconnect().await?;
    pg.stop().await?;
    
    Ok(())
}

async fn create_db(user: &String, pw: &String) -> Result<PostgreSQL> {
    let inst_dir = PathBuf::from(format!("{}/inst", ROOT_DIR));
    let data_dir = PathBuf::from(format!("{}/data", ROOT_DIR));

    let mut postgresql = PostgreSQL::new(Settings {
        username: user.clone(),
        password: pw.clone(),
        installation_dir: inst_dir.clone(),
        data_dir: data_dir.clone(),
        port: 5432,
        ..Default::default()
    });

    // Install Postgres if it hasn't been already
    if !data_dir.join("PG_VERSION").exists() {
        postgresql.setup().await?;
    }

    // Start the Postgres server
    postgresql.start().await?;

    // Create the database if it doesn't exist
    if !postgresql.database_exists(DB_NAME).await? {
        postgresql.create_database(DB_NAME).await?;
    }

    Ok(postgresql)
}

struct DBClient {
    pub client: Client,
    connection_handle: tokio::task::JoinHandle<()>
}

impl DBClient {
    async fn connect(ip: &str, port: u16, user: &String, pw: &String) -> Result<Self> {
        let (client, connection) = Config::default()
            .host(ip)
            .port(port)
            .user(user)
            .password(pw)
            .dbname(DB_NAME)
            .connect(NoTls).await?;

        // Spawn a new thread to wait for updates on the connection
        let connection_handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {:?}", e);
            }
        });

        Ok(Self { client, connection_handle })
    }

    async fn disconnect(self) -> Result<()> {
        drop(self.client);
        self.connection_handle.abort();

        Ok(())
    }
}