use futures::SinkExt;
use itertools::Itertools as _;
use std::ops::DerefMut;
use tokio_postgres::CopyInSink;

use sqlx::{
    PgConnection, PgPool,
    postgres::{PgArgumentBuffer, PgCopyIn, Postgres},
};

const BUFFER_SIZE: usize = 4096;
struct CopyDataSink<C: DerefMut<Target = PgConnection>> {
    encode_buf: PgArgumentBuffer,
    data_buf: Vec<u8>,
    copy_in: PgCopyIn<C>,
}

type BoxError = Box<dyn std::error::Error + 'static + Send + Sync>;

impl<C: DerefMut<Target = PgConnection>> CopyDataSink<C> {
    fn new(copy_in: PgCopyIn<C>) -> Self {
        let mut data_buf = Vec::with_capacity(BUFFER_SIZE * 2);
        const COPY_SIGNATURE: &[u8] = &[
            b'P', b'G', b'C', b'O', b'P', b'Y', b'\n', // "PGCOPY\n"
            0xFF,  // \377 (8進数) = 0xFF (16進数)
            b'\r', b'\n', // "\r\n"
            0x00,  // \0
        ];

        assert_eq!(COPY_SIGNATURE.len(), 11);
        data_buf.extend_from_slice(COPY_SIGNATURE); // 署名
        data_buf.extend_from_slice(&0_i32.to_be_bytes()); // フラグフィールド
        data_buf.extend_from_slice(&0_i32.to_be_bytes()); // ヘッダ拡張領域長

        CopyDataSink {
            encode_buf: Default::default(),
            data_buf,
            copy_in,
        }
    }

    async fn send(&mut self) -> Result<(), BoxError> {
        let _copy_in = self.copy_in.send(self.data_buf.as_slice()).await?;

        self.data_buf.clear();
        Ok(())
    }

    async fn finish(mut self) -> Result<u64, BoxError> {
        const COPY_TRAILER: &[u8] = &(-1_i16).to_be_bytes();

        self.data_buf.extend(COPY_TRAILER);
        self.send().await?;
        self.copy_in.finish().await.map_err(|e| e.into())
    }

    fn insert_row(&mut self) {
        let num_col = self.copy_in.num_columns() as i16;
        self.data_buf.extend(num_col.to_be_bytes());
    }

    async fn add<'q, T>(&mut self, value: T) -> Result<(), BoxError>
    where
        T: sqlx::Encode<'q, Postgres> + sqlx::Type<Postgres>,
    {
        let is_null = value.encode_by_ref(&mut self.encode_buf)?;

        match is_null {
            sqlx::encode::IsNull::Yes => {
                self.data_buf.extend((-1_i32).to_be_bytes());
            }
            sqlx::encode::IsNull::No => {
                self.data_buf
                    .extend((self.encode_buf.len() as i32).to_be_bytes());
                self.data_buf.extend_from_slice(self.encode_buf.as_slice());
            }
        }

        self.encode_buf.clear();

        if self.data_buf.len() > BUFFER_SIZE {
            self.send().await?;
        }

        Ok(())
    }
}
#[derive(Debug, Clone, sqlx::FromRow)]
struct Author {
    id: i64,
    name: String,
    bio: Option<String>,
}

impl Author {
    fn to_text(&self) -> String {
        let bio = self.bio.as_deref().unwrap_or("\\N");
        format!("{}\t{}\t{}\n", self.id, self.name, bio)
    }

    fn to_csv(&self) -> String {
        let bio = self.bio.as_deref().unwrap_or("");
        format!("{},{},{}\n", self.id, self.name, bio)
    }
}

struct AuthorGenerator {
    current: usize,
}

impl AuthorGenerator {
    fn new(current: usize) -> Self {
        Self { current }
    }

    fn generate_author(index: usize) -> Author {
        Author {
            id: index as i64,
            name: format!("Author {}", index),
            bio: if index % 3 == 0 {
                Some(format!(
                    "Biography of author {} with some longer text content",
                    index
                ))
            } else {
                None
            },
        }
    }
}

impl Iterator for AuthorGenerator {
    type Item = Author;

    fn next(&mut self) -> Option<Self::Item> {
        let author = Self::generate_author(self.current);
        self.current += 1;
        Some(author)
    }
}

async fn buffered_copy_in<F, C>(generator: impl Iterator<Item = Author>, coverter: C, mut f: F)
where
    C: Fn(Author) -> String,
    F: AsyncFnMut(bytes::Bytes),
{
    let mut buf = bytes::BytesMut::with_capacity(BUFFER_SIZE * 2);
    for author in generator {
        let data = coverter(author);
        buf.extend_from_slice(data.as_bytes());

        if buf.len() >= BUFFER_SIZE {
            let bytes = buf.split().freeze();
            f(bytes).await;
        }
    }
    if !buf.is_empty() {
        let bytes = buf.split().freeze();
        f(bytes).await;
    }
}

async fn sqlx_binary_copy_in(pool: &PgPool, count: usize) -> std::time::Duration {
    let mut conn = pool.acquire().await.unwrap();

    // テーブルをクリア
    sqlx::query("TRUNCATE authors")
        .execute(&mut *conn)
        .await
        .unwrap();

    let start = std::time::Instant::now();

    let copy_in = conn
        .copy_in_raw("COPY authors (id, name, bio) FROM STDIN (FORMAT BINARY)")
        .await
        .unwrap();
    let mut sink = CopyDataSink::new(copy_in);

    // ストリーミングでデータを処理
    let generator = AuthorGenerator::new(count).take(count);
    for author in generator {
        sink.insert_row();
        sink.add(author.id).await.unwrap();
        sink.add(author.name.as_str()).await.unwrap();
        sink.add(author.bio.as_deref()).await.unwrap();
    }

    sink.finish().await.unwrap();

    start.elapsed()
}

async fn sqlx_text_copy_in(pool: &PgPool, count: usize) -> std::time::Duration {
    let mut conn = pool.acquire().await.unwrap();

    // テーブルをクリア
    sqlx::query("TRUNCATE authors")
        .execute(&mut *conn)
        .await
        .unwrap();

    let start = std::time::Instant::now();

    let mut copy_in = conn
        .copy_in_raw("COPY authors (id, name, bio) FROM STDIN (FORMAT TEXT)")
        .await
        .unwrap();

    // ストリーミングでデータを処理
    let generator = AuthorGenerator::new(count).take(count);
    buffered_copy_in(
        generator,
        |author| author.to_text(),
        async |buf| {
            copy_in.send(buf).await.unwrap();
        },
    )
    .await;

    copy_in.finish().await.unwrap();

    start.elapsed()
}

async fn sqlx_csv_copy_in(pool: &PgPool, count: usize) -> std::time::Duration {
    let mut conn = pool.acquire().await.unwrap();

    // テーブルをクリア
    sqlx::query("TRUNCATE authors")
        .execute(&mut *conn)
        .await
        .unwrap();

    let start = std::time::Instant::now();

    let mut copy_in = conn
        .copy_in_raw("COPY authors (id, name, bio) FROM STDIN (FORMAT CSV)")
        .await
        .unwrap();

    // ストリーミングでデータを処理
    let generator = AuthorGenerator::new(count).take(count);
    buffered_copy_in(
        generator,
        |author| author.to_csv(),
        async |buf| {
            copy_in.send(buf).await.unwrap();
        },
    )
    .await;

    copy_in.finish().await.unwrap();

    start.elapsed()
}

async fn sqlx_unnest(pool: &PgPool, count: usize) -> std::time::Duration {
    let mut conn = pool.acquire().await.unwrap();

    // テーブルをクリア
    sqlx::query("TRUNCATE authors")
        .execute(&mut *conn)
        .await
        .unwrap();

    let generator = AuthorGenerator::new(count).take(count);
    let mut ids: Vec<i64> = Vec::with_capacity(count);
    let mut names: Vec<String> = Vec::with_capacity(count);
    let mut bios: Vec<Option<String>> = Vec::with_capacity(count);

    for author in generator {
        ids.push(author.id);
        names.push(author.name);
        bios.push(author.bio);
    }

    let start = std::time::Instant::now();
    sqlx::query("INSERT INTO authors (id, name, bio) SELECT unnest($1::BIGINT[]), unnest($2::TEXT[]), unnest($3::TEXT[])")
        .bind(ids)
        .bind(names)
        .bind(bios)
        .execute(&mut *conn)
        .await
        .unwrap();

    start.elapsed()
}

async fn sqlx_insert(pool: &PgPool, count: usize) -> std::time::Duration {
    let mut conn = pool.acquire().await.unwrap();

    // テーブルをクリア
    sqlx::query("TRUNCATE authors")
        .execute(&mut *conn)
        .await
        .unwrap();

    let generator = AuthorGenerator::new(count).take(count);

    let chunk_size = 10000;
    assert!(count % chunk_size == 0);

    let mut query = String::from("INSERT INTO authors (id, name, bio) VALUES ");
    let parameters = (0..chunk_size)
        .map(|i| format!("(${}, ${}, ${})", i * 3 + 1, i * 3 + 2, i * 3 + 3))
        .collect::<Vec<_>>()
        .join(", ");
    query.push_str(&parameters);

    let start = std::time::Instant::now();
    for authors in &generator.chunks(chunk_size) {
        let mut q = sqlx::query(&query);
        for author in authors {
            q = q.bind(author.id).bind(author.name).bind(author.bio);
        }

        q.execute(&mut *conn).await.unwrap();
    }

    start.elapsed()
}

async fn tokio_postgres_binary_copy_in(
    client: &tokio_postgres::Client,
    count: usize,
) -> std::time::Duration {
    use tokio_postgres::{binary_copy::BinaryCopyInWriter, types::Type};

    client.batch_execute("TRUNCATE authors").await.unwrap();

    let start = std::time::Instant::now();

    let copy_in = client
        .copy_in("COPY authors (id, name, bio) FROM STDIN (FORMAT BINARY)")
        .await
        .unwrap();
    let writer = BinaryCopyInWriter::new(copy_in, &[Type::INT8, Type::TEXT, Type::TEXT]);
    tokio::pin!(writer);

    let generator = AuthorGenerator::new(count).take(count);
    for author in generator {
        writer
            .as_mut()
            .write(&[&author.id, &author.name, &author.bio])
            .await
            .unwrap();
    }

    writer.finish().await.unwrap();

    start.elapsed()
}

async fn tokio_postgres_text_copy_in(
    client: &tokio_postgres::Client,
    count: usize,
) -> std::time::Duration {
    client.batch_execute("TRUNCATE authors").await.unwrap();

    let start = std::time::Instant::now();

    let copy_in: CopyInSink<bytes::Bytes> = client
        .copy_in("COPY authors (id, name, bio) FROM STDIN (FORMAT TEXT)")
        .await
        .unwrap();
    tokio::pin!(copy_in);

    let generator = AuthorGenerator::new(count).take(count);
    buffered_copy_in(
        generator,
        |author| author.to_text(),
        async |buf| {
            copy_in.send(buf).await.unwrap();
        },
    )
    .await;

    copy_in.close().await.unwrap();

    start.elapsed()
}

async fn tokio_postgres_csv_copy_in(
    client: &tokio_postgres::Client,
    count: usize,
) -> std::time::Duration {
    client.batch_execute("TRUNCATE authors").await.unwrap();

    let start = std::time::Instant::now();

    let copy_in: CopyInSink<bytes::Bytes> = client
        .copy_in("COPY authors (id, name, bio) FROM STDIN (FORMAT CSV)")
        .await
        .unwrap();
    tokio::pin!(copy_in);

    let generator = AuthorGenerator::new(count).take(count);
    buffered_copy_in(
        generator,
        |author| author.to_csv(),
        async |buf| {
            copy_in.send(buf).await.unwrap();
        },
    )
    .await;

    copy_in.close().await.unwrap();

    start.elapsed()
}

async fn tokio_postgres_unnest(
    client: &tokio_postgres::Client,
    count: usize,
) -> std::time::Duration {
    client.batch_execute("TRUNCATE authors").await.unwrap();

    let generator = AuthorGenerator::new(count).take(count);
    let mut ids: Vec<i64> = Vec::with_capacity(count);
    let mut names: Vec<String> = Vec::with_capacity(count);
    let mut bios: Vec<Option<String>> = Vec::with_capacity(count);

    for author in generator {
        ids.push(author.id);
        names.push(author.name);
        bios.push(author.bio);
    }

    let start = std::time::Instant::now();
    client
        .execute(
            "INSERT INTO authors (id, name, bio) SELECT unnest($1::BIGINT[]), unnest($2::TEXT[]), unnest($3::TEXT[])",
            &[&ids, &names, &bios],
        )
        .await
        .unwrap();

    start.elapsed()
}

async fn tokio_postgres_insert(
    client: &tokio_postgres::Client,
    count: usize,
) -> std::time::Duration {
    client.batch_execute("TRUNCATE authors").await.unwrap();

    let generator = AuthorGenerator::new(count).take(count);

    let chunk_size = 10000;
    assert!(count % chunk_size == 0);

    let mut query = String::from("INSERT INTO authors (id, name, bio) VALUES ");
    let parameters = (0..chunk_size)
        .map(|i| format!("(${}, ${}, ${})", i * 3 + 1, i * 3 + 2, i * 3 + 3))
        .collect::<Vec<_>>()
        .join(", ");
    query.push_str(&parameters);

    let statement = client
        .prepare(&query)
        .await
        .expect("Failed to prepare statement");

    let start = std::time::Instant::now();
    for authors in &generator.chunks(chunk_size) {
        let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync>> =
            Vec::with_capacity(chunk_size * 3);
        for author in authors {
            params.push(Box::new(author.id));
            params.push(Box::new(author.name));
            params.push(Box::new(author.bio));
        }
        let param_ref = params.iter().map(|p| p.as_ref()).collect::<Vec<_>>();

        client.execute(&statement, &param_ref).await.unwrap();
    }

    start.elapsed()
}

const DATABASE_URL: &str = "postgres://postgres:password@postgres:5432/test";

fn tokio_postgres_config() -> tokio_postgres::Config {
    let postgres_url = url::Url::parse(DATABASE_URL).unwrap();
    let db_name = {
        let path = postgres_url.path().trim_start_matches('/');
        if path.is_empty() { "postgres" } else { path }
    };
    let host = postgres_url
        .host()
        .map(|h| h.to_string())
        .unwrap_or("localhost".into());
    let port = postgres_url.port().unwrap_or(5432);

    let user = postgres_url.username();
    let password = postgres_url.password().unwrap_or("");

    let mut config = tokio_postgres::Config::default();
    config.dbname(db_name);
    config.host(&host);
    config.port(port);
    config.user(user);
    config.password(password);

    config
}

async fn migrate_db(client: &tokio_postgres::Client) -> Result<(), BoxError> {
    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS authors (
                id BIGINT PRIMARY KEY,
                name TEXT NOT NULL,
                bio TEXT
            )",
        )
        .await?;
    Ok(())
}

async fn benchmark_with_stats<F, Fut>(runs: usize, f: F) -> std::time::Duration
where
    F: Fn() -> Fut,
    Fut: Future<Output = std::time::Duration>,
{
    let mut durations = Vec::new();
    for _ in 0..runs {
        durations.push(f().await);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await; // クールダウン
    }

    let total_duration: std::time::Duration = durations.iter().sum();

    total_duration / runs as u32
}

#[tokio::main]
async fn main() {
    let pool = PgPool::connect(DATABASE_URL).await.unwrap();

    let (client, conn) = tokio_postgres_config()
        .connect(tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            panic!("connection error: {e}");
        }
    });
    migrate_db(&client).await.unwrap();

    let count = 1_000_000;
    let runs = 5;

    let duration_sqlx_binary =
        benchmark_with_stats(runs, || sqlx_binary_copy_in(&pool, count)).await;
    println!(
        "sqlx binary copy in took: {}ms",
        duration_sqlx_binary.as_millis()
    );

    let duration_sqlx_text = benchmark_with_stats(runs, || sqlx_text_copy_in(&pool, count)).await;
    println!(
        "sqlx text copy in took: {}ms",
        duration_sqlx_text.as_millis()
    );

    let duration_sqlx_csv = benchmark_with_stats(runs, || sqlx_csv_copy_in(&pool, count)).await;
    println!("sqlx csv copy in took: {}ms", duration_sqlx_csv.as_millis());

    let duration_sqlx_unnest = benchmark_with_stats(runs, || sqlx_unnest(&pool, count)).await;
    println!("sqlx unnest took: {}ms", duration_sqlx_unnest.as_millis());

    let duration_sqlx_insert = benchmark_with_stats(runs, || sqlx_insert(&pool, count)).await;
    println!("sqlx insert took: {}ms", duration_sqlx_insert.as_millis());

    let duration_tokio_postgres_binary =
        benchmark_with_stats(runs, || tokio_postgres_binary_copy_in(&client, count)).await;
    println!(
        "tokio-postgres binary copy in took: {}ms",
        duration_tokio_postgres_binary.as_millis()
    );

    let duration_tokio_postgres_text =
        benchmark_with_stats(runs, || tokio_postgres_text_copy_in(&client, count)).await;
    println!(
        "tokio-postgres text copy in took: {}ms",
        duration_tokio_postgres_text.as_millis()
    );

    let duration_tokio_postgres_csv =
        benchmark_with_stats(runs, || tokio_postgres_csv_copy_in(&client, count)).await;
    println!(
        "tokio-postgres csv copy in took: {}ms",
        duration_tokio_postgres_csv.as_millis()
    );

    let duration_tokio_postgres_unnest =
        benchmark_with_stats(runs, || tokio_postgres_unnest(&client, count)).await;
    println!(
        "tokio-postgres unnest took: {}ms",
        duration_tokio_postgres_unnest.as_millis()
    );

    let duration_tokio_postgres_insert =
        benchmark_with_stats(runs, || tokio_postgres_insert(&client, count)).await;
    println!(
        "tokio-postgres insert took: {}ms",
        duration_tokio_postgres_insert.as_millis()
    );
}
