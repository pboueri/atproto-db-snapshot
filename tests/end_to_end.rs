//! Synthetic-rocks end-to-end test: build a small constellation-shaped
//! rocksdb, run stage + hydrate for real (only R2 publish is skipped),
//! and assert the four validations the spec calls out.

mod common;

use anyhow::{Context, Result};
use at_snapshot::config::Config;
use chrono::Utc;
use common::{tid_for, tid_with_jitter, today, two_days_ago, yesterday, Mocker};
use duckdb::Connection;
use tempfile::TempDir;

const DID_ALICE: &str = "did:plc:alice";
const DID_BOB: &str = "did:plc:bob";
const DID_CAROL: &str = "did:plc:carol";
const DID_DAVE: &str = "did:plc:dave";

const ID_ALICE: u64 = 1;
const ID_BOB: u64 = 2;
const ID_CAROL: u64 = 3;
const ID_DAVE: u64 = 4;

const COL_POST: &str = "app.bsky.feed.post";
const COL_LIKE: &str = "app.bsky.feed.like";
const COL_REPOST: &str = "app.bsky.feed.repost";
const COL_FOLLOW: &str = "app.bsky.graph.follow";
const COL_BLOCK: &str = "app.bsky.graph.block";

#[tokio::test]
async fn end_to_end_synthetic_rocks() -> Result<()> {
    let tmp = TempDir::new()?;
    let work_dir = tmp.path().to_path_buf();
    std::fs::create_dir_all(work_dir.join("rocks"))?;
    std::fs::write(work_dir.join("rocks/.cursor"), b"{}")?;

    populate_fixture(&work_dir.join("rocks"))?;

    let cfg = Config {
        source_url: "test://".into(),
        work_dir: work_dir.clone(),
        snapshot_date: Some("2026-04-27".into()),
        memory_limit: "1GiB".into(),
        batch_size: 1024,
        mirror_concurrency: 1,
        backup_id: None,
        upload: None,
        rocks_block_cache: "64MiB".into(),
        stage_threads: 1,
        hydrate_window_days: None,
    };

    let stage = at_snapshot::stage::run(&cfg, "2026-04-27").await?;
    let hydrate = at_snapshot::hydrate::run(&cfg, "2026-04-27").await?;

    assert_counts(&hydrate.row_counts);

    let conn = Connection::open(&hydrate.duckdb_path)?;
    assert_validation_queries(&conn)?;
    assert_metadata(&conn, "2026-04-27", &cfg)?;

    println!("stage counts: {:?}", stage.counts);
    println!("hydrate counts: {:?}", hydrate.row_counts);
    println!(
        "orphan rates: like={:.4} repost={:.4}",
        hydrate.orphan_like_rate, hydrate.orphan_repost_rate
    );
    Ok(())
}

fn populate_fixture(rocks_dir: &std::path::Path) -> Result<()> {
    let m = Mocker::open(rocks_dir).context("open mock rocks")?;

    // Actors
    m.put_actor(DID_ALICE, ID_ALICE, true)?;
    m.put_actor(DID_BOB, ID_BOB, true)?;
    m.put_actor(DID_CAROL, ID_CAROL, true)?;
    m.put_actor(DID_DAVE, ID_DAVE, false)?;

    // Posts
    let tid_a = tid_for(two_days_ago());
    let tid_b = tid_for(yesterday());
    let tid_c = tid_for(today());
    let tid_d = tid_for(yesterday());
    let tid_e = tid_for(today());
    let post_a_uri = format!("at://{}/{}/{}", DID_ALICE, COL_POST, tid_a);
    let post_b_uri = format!("at://{}/{}/{}", DID_BOB, COL_POST, tid_b);
    let post_c_uri = format!("at://{}/{}/{}", DID_CAROL, COL_POST, tid_c);
    let post_d_uri = format!("at://{}/{}/{}", DID_ALICE, COL_POST, tid_d);
    let post_e_uri = format!("at://{}/{}/{}", DID_BOB, COL_POST, tid_e);

    // TargetIds for everything anyone references.
    // 100..199 reserved for follows
    m.put_target(DID_BOB, COL_FOLLOW, ".subject", 100)?;
    m.put_target(DID_CAROL, COL_FOLLOW, ".subject", 101)?;
    m.put_target(DID_ALICE, COL_FOLLOW, ".subject", 102)?;
    m.put_target(DID_ALICE, COL_FOLLOW, ".subject", 103)?; // alias for carol→alice
    m.put_target(DID_DAVE, COL_FOLLOW, ".subject", 104)?;

    // 200..299 for blocks
    m.put_target(DID_DAVE, COL_BLOCK, ".subject", 200)?;

    // 300..399 for like/repost subjects (post URIs)
    m.put_target(&post_a_uri, COL_LIKE, ".subject.uri", 300)?;
    m.put_target(&post_b_uri, COL_LIKE, ".subject.uri", 301)?;
    m.put_target(&post_d_uri, COL_LIKE, ".subject.uri", 302)?;
    m.put_target(&post_a_uri, COL_REPOST, ".subject.uri", 303)?;

    // 400..499 for post embed/reply targets
    m.put_target(&post_a_uri, COL_POST, ".reply.parent.uri", 400)?;
    m.put_target(&post_a_uri, COL_POST, ".reply.root.uri", 401)?;
    m.put_target(&post_b_uri, COL_POST, ".embed.record.uri", 402)?;
    m.put_target("blob://image-cid-bobs-photo", COL_POST, ".embed.images[0].image", 410)?;
    m.put_target("https://example.com/article", COL_POST, ".embed.external.uri", 411)?;

    // Follows: did_id → record_link. Use jittered TIDs so each row has a
    // unique rkey and rocksdb doesn't collapse them.
    m.put_record_links(ID_ALICE, COL_FOLLOW, &tid_with_jitter(yesterday(), 1), &[(".subject", 100)])?;
    m.put_record_links(ID_ALICE, COL_FOLLOW, &tid_with_jitter(yesterday(), 2), &[(".subject", 101)])?;
    m.put_record_links(ID_BOB,   COL_FOLLOW, &tid_with_jitter(yesterday(), 3), &[(".subject", 102)])?;
    m.put_record_links(ID_CAROL, COL_FOLLOW, &tid_with_jitter(today(), 1),     &[(".subject", 103)])?;
    m.put_record_links(ID_BOB,   COL_FOLLOW, &tid_with_jitter(two_days_ago(), 1), &[(".subject", 104)])?;

    // Blocks
    m.put_record_links(ID_ALICE, COL_BLOCK, &tid_for(yesterday()), &[(".subject", 200)])?;

    // Likes
    m.put_record_links(ID_BOB,   COL_LIKE, &tid_with_jitter(today(), 10),     &[(".subject.uri", 300)])?;
    m.put_record_links(ID_CAROL, COL_LIKE, &tid_with_jitter(today(), 11),     &[(".subject.uri", 300)])?;
    m.put_record_links(ID_ALICE, COL_LIKE, &tid_with_jitter(yesterday(), 10), &[(".subject.uri", 301)])?;
    m.put_record_links(ID_DAVE,  COL_LIKE, &tid_with_jitter(today(), 12),     &[(".subject.uri", 302)])?;

    // Reposts
    m.put_record_links(ID_BOB, COL_REPOST, &tid_for(today()), &[(".subject.uri", 303)])?;

    // Posts (records)
    // post_a: plain
    m.put_record_links(ID_ALICE, COL_POST, &tid_a, &[])?;
    // post_b: image embed
    m.put_record_links(ID_BOB, COL_POST, &tid_b, &[(".embed.images[0].image", 410)])?;
    // post_c: reply to post_a, quote post_b
    m.put_record_links(
        ID_CAROL,
        COL_POST,
        &tid_c,
        &[
            (".reply.parent.uri", 400),
            (".reply.root.uri", 401),
            (".embed.record.uri", 402),
        ],
    )?;
    // post_e: external embed
    m.put_record_links(ID_BOB, COL_POST, &tid_e, &[(".embed.external.uri", 411)])?;
    // post_d intentionally absent from link_targets — exercises Pass C.

    m.close()?;
    let _ = (post_a_uri, post_b_uri, post_c_uri, post_d_uri, post_e_uri);
    let _ = Utc::now();
    Ok(())
}

fn assert_counts(counts: &[(String, u64)]) {
    let get = |t: &str| {
        counts
            .iter()
            .find(|(n, _)| n == t)
            .map(|(_, v)| *v)
            .unwrap_or_else(|| panic!("missing count for {t}"))
    };
    assert_eq!(get("actors"), 4, "expected 4 actors");
    assert_eq!(get("follows"), 5, "expected 5 follows");
    assert_eq!(get("blocks"), 1, "expected 1 block");
    assert_eq!(get("likes"), 4, "expected 4 likes");
    assert_eq!(get("reposts"), 1, "expected 1 repost");
    // post_a, post_b, post_c, post_e from records + post_d from target-only = 5
    assert_eq!(get("posts"), 5, "expected 5 distinct posts");
    assert_eq!(get("post_media"), 2, "expected 2 media (image + external)");
}

fn assert_metadata(conn: &Connection, snapshot_date: &str, cfg: &Config) -> Result<()> {
    let n: i64 = conn.query_row("SELECT COUNT(*) FROM snapshot_metadata", [], |r| r.get(0))?;
    assert_eq!(n, 1, "expected exactly one snapshot_metadata row");
    let (date_str, source_url, window_days): (String, String, Option<i32>) = conn.query_row(
        "SELECT CAST(snapshot_date AS VARCHAR), source_url, hydrate_window_days
         FROM snapshot_metadata",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    assert_eq!(date_str, snapshot_date);
    assert_eq!(source_url, cfg.source_url);
    assert_eq!(window_days, None, "expected no window for this fixture");
    Ok(())
}

fn assert_validation_queries(conn: &Connection) -> Result<()> {
    let day_start_naive = (Utc::now() - chrono::Duration::days(1))
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let day_end_naive = day_start_naive + chrono::Duration::days(1);
    let day_start = day_start_naive.format("%Y-%m-%d %H:%M:%S").to_string();
    let day_end = day_end_naive.format("%Y-%m-%d %H:%M:%S").to_string();

    // 1: follows generated yesterday
    let yesterday_follows: i64 = conn.query_row(
        &format!(
            "SELECT COUNT(*) FROM follows WHERE created_at >= TIMESTAMP '{day_start}' AND created_at < TIMESTAMP '{day_end}'"
        ),
        [],
        |r| r.get(0),
    )?;
    assert_eq!(yesterday_follows, 3, "expected 3 follows yesterday");

    // 2: pct follows yesterday
    let pct: f64 = conn.query_row(
        &format!(
            "SELECT 100.0 * SUM(CASE WHEN created_at >= TIMESTAMP '{day_start}' AND created_at < TIMESTAMP '{day_end}' THEN 1 ELSE 0 END)
                    / COUNT(*) FROM follows"
        ),
        [],
        |r| r.get(0),
    )?;
    assert!((pct - 60.0).abs() < 0.001, "expected 60% follows yesterday, got {pct}");

    // 3: distinct authors who got >=1 like
    let posters_with_likes: i64 = conn.query_row(
        "SELECT COUNT(DISTINCT p.author_did_id)
         FROM posts p WHERE EXISTS (SELECT 1 FROM likes l WHERE l.subject_uri_id = p.uri_id)",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(posters_with_likes, 2, "expected 2 posters with likes (alice, bob)");

    // 4: posts with >=1 like
    let posts_with_likes: i64 = conn.query_row(
        "SELECT COUNT(*) FROM posts p WHERE EXISTS (SELECT 1 FROM likes l WHERE l.subject_uri_id = p.uri_id)",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(posts_with_likes, 3, "expected 3 posts with likes");

    Ok(())
}
