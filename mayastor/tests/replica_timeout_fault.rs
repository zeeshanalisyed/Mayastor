#![allow(unused_assignments)]

use std::{thread, time};

use common::{bdev_io, ms_exec::MayastorProcess, nexus_util};
use mayastor::{
    bdev::{nexus_create, ActionType, NexusStatus},
    core::{MayastorCliArgs, MayastorEnvironment, Reactor},
    subsys,
    subsys::Config,
};

pub mod common;

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

static DISKNAME2: &str = "/tmp/disk2.img";
static BDEVNAME2: &str = "aio:///tmp/disk2.img?blk_size=512";

static DISKSIZE_KB: u64 = 64 * 1024;

static CONFIG_FILE_CHILD1: &str = "/tmp/replica_timeout_fault_child1.yaml";
static UUID1: &str = "00000000-76b6-4fcf-864d-1027d4038756";
static CONFIG_FILE_CHILD2: &str = "/tmp/replica_timeout_fault_child2.yaml";
static UUID2: &str = "11111111-76b6-4fcf-864d-1027d4038756";

static NXNAME: &str = "replica_timeout_test";

static CONFIG_FILE_NEXUS: &str = "/tmp/replica_timeout_fault_nexus.yaml";

/// Generate and write config files for the replicas.
fn generate_config() {
    let mut config = Config::default();

    let child1_bdev = subsys::BaseBdev {
        uri: format!("{}&uuid={}", BDEVNAME1, UUID1),
    };

    let child2_bdev = subsys::BaseBdev {
        uri: format!("{}&uuid={}", BDEVNAME2, UUID2),
    };

    config.base_bdevs = Some(vec![child1_bdev]);
    config.implicit_share_base = true;
    config.nexus_opts.iscsi_enable = false;
    config.nexus_opts.nvmf_replica_port = 8430;
    config.nexus_opts.nvmf_nexus_port = 8440;
    config.write(CONFIG_FILE_CHILD1).unwrap();

    config.base_bdevs = Some(vec![child2_bdev]);
    config.nexus_opts.nvmf_replica_port = 8431;
    config.nexus_opts.nvmf_nexus_port = 8441;
    config.write(CONFIG_FILE_CHILD2).unwrap();
}

fn start_mayastor(cfg: &str) -> MayastorProcess {
    let args = vec![
        "-s".to_string(),
        "128".to_string(),
        "-y".to_string(),
        cfg.to_string(),
    ];

    MayastorProcess::new(Box::from(args)).unwrap()
}

/// Run mayastor with 2 replicas and configured to fault on timeout
/// Send IO and pause one replica. Verify that the child becomes
/// marked as faulted and that the IO path still works afterward.
#[test]
fn replica_timeout_fault() {
    generate_config();

    common::truncate_file(DISKNAME1, DISKSIZE_KB);
    common::truncate_file(DISKNAME2, DISKSIZE_KB);

    let mut ms1 = start_mayastor(CONFIG_FILE_CHILD1);
    let _ms2 = start_mayastor(CONFIG_FILE_CHILD2);

    let mut config = Config::default();
    config.err_store_opts.enable_err_store = true;
    config.err_store_opts.err_store_size = 256;
    config.err_store_opts.action = ActionType::Ignore;
    config.err_store_opts.retention_ns = 1_000_000_000;
    config.err_store_opts.max_errors = 64;

    config.err_store_opts.timeout_action = ActionType::Fault;
    config.err_store_opts.timeout_sec = 1;

    config.write(CONFIG_FILE_NEXUS).unwrap();

    test_init!(CONFIG_FILE_NEXUS);

    Reactor::block_on(async {
        create_nexus().await;
        bdev_io::write_some(NXNAME).await.unwrap();
        bdev_io::read_some(NXNAME).await.unwrap();
        ms1.sig_stop();
        let handle = thread::spawn(move || {
            // Sufficiently long to trigger the timeout
            thread::sleep(time::Duration::from_secs(3));
            ms1.sig_cont();
            ms1
        });
        nexus_util::check_nexus_state_is(NXNAME, NexusStatus::Online);

        bdev_io::write_some(NXNAME) // this will block until the child bdev is removed
            .await
            .expect_err("should fail write when faulted child is flushed");

        nexus_util::check_nexus_state_is(NXNAME, NexusStatus::Degraded);

        bdev_io::write_some(NXNAME) // will succeed because the failed bdev is no longer in the path
            .await
            .expect("should write again after the child has been faulted");
        ms1 = handle.join().unwrap();
        nexus_util::delete_nexus(NXNAME).await;
    });
    common::delete_file(&[DISKNAME1.to_string()]);
    common::delete_file(&[DISKNAME2.to_string()]);
    common::delete_file(&[CONFIG_FILE_NEXUS.to_string()]);
    common::delete_file(&[CONFIG_FILE_CHILD1.to_string()]);
    common::delete_file(&[CONFIG_FILE_CHILD2.to_string()]);
}

/// Create a 2-replica nexus for this test
async fn create_nexus() {
    let ch = vec![
        "nvmf://127.0.0.1:8430/nqn.2019-05.io.openebs:".to_string()
            + &UUID1.to_string(),
        "nvmf://127.0.0.1:8431/nqn.2019-05.io.openebs:".to_string()
            + &UUID2.to_string(),
    ];
    nexus_create(NXNAME, DISKSIZE_KB * 1024, None, &ch)
        .await
        .unwrap();
}
