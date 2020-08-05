extern crate log;

use crossbeam::channel::unbounded;

use std::time::Duration;
pub mod common;

use common::{
    error_bdev::{
        create_error_bdev,
        inject_error,
        SPDK_BDEV_IO_TYPE_READ,
        SPDK_BDEV_IO_TYPE_WRITE,
        VBDEV_IO_FAILURE,
    },
    nexus_util,
};

use mayastor::{
    bdev::{nexus_create, ActionType, NexusStatus},
    core::{
        mayastor_env_stop,
        Bdev,
        MayastorCliArgs,
        MayastorEnvironment,
        Reactor,
    },
    subsys::Config,
};

static NXNAME: &str = "error_fault_child_test_nexus";

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

static DISKNAME2: &str = "/tmp/disk2.img";

static ERROR_DEVICE: &str = "error_device";
static EE_ERROR_DEVICE: &str = "EE_error_device"; // The prefix is added by the vbdev_error module
static BDEV_EE_ERROR_DEVICE: &str = "bdev:///EE_error_device";

static CONFIG_FILE_NEXUS: &str = "/tmp/error_fault.yaml";

#[test]
fn nexus_fault_child_test() {
    common::truncate_file(DISKNAME1, 64 * 1024);
    common::truncate_file(DISKNAME2, 64 * 1024);

    let mut config = Config::default();
    config.err_store_opts.enable_err_store = true;
    config.err_store_opts.err_store_size = 256;
    config.err_store_opts.action = ActionType::Fault;
    config.err_store_opts.retention_ns = 1_000_000_000;
    config.err_store_opts.max_errors = 4;

    config.err_store_opts.timeout_action = ActionType::Ignore;
    config.err_store_opts.timeout_sec = 0;

    config.write(CONFIG_FILE_NEXUS).unwrap();

    test_init!(CONFIG_FILE_NEXUS);

    Reactor::block_on(async {
        create_error_bdev(ERROR_DEVICE, DISKNAME2);
        create_nexus().await;

        nexus_util::check_nexus_state_is(NXNAME, NexusStatus::Online);

        inject_error(
            EE_ERROR_DEVICE,
            SPDK_BDEV_IO_TYPE_READ,
            VBDEV_IO_FAILURE,
            10,
        );
        inject_error(
            EE_ERROR_DEVICE,
            SPDK_BDEV_IO_TYPE_WRITE,
            VBDEV_IO_FAILURE,
            10,
        );

        for _ in 0 .. 3 {
            err_read_nexus_both(false).await;
            reactor_run_millis(1);
        }
        for _ in 0 .. 2 {
            // the second iteration causes the error count to exceed the max no
            // of retry errors (4) for the read and causes the child to be
            // removed
            err_read_nexus_both(false).await;
            reactor_run_millis(1);
        }
    });

    // error child should be removed from the IO path here

    nexus_util::check_nexus_state_is(NXNAME, NexusStatus::Degraded);

    Reactor::block_on(async {
        err_read_nexus_both(true).await; // should succeed because both IOs go to the remaining child
        err_write_nexus(true).await; // should succeed because the IO goes to
                                     // the remaining child
    });

    Reactor::block_on(async {
        nexus_util::delete_nexus(NXNAME).await;
    });

    mayastor_env_stop(0);

    common::delete_file(&[DISKNAME1.to_string()]);
    common::delete_file(&[DISKNAME2.to_string()]);
    common::delete_file(&[CONFIG_FILE_NEXUS.to_string()]);
}

async fn create_nexus() {
    let ch = vec![BDEV_EE_ERROR_DEVICE.to_string(), BDEVNAME1.to_string()];

    nexus_create(NXNAME, 64 * 1024 * 1024, None, &ch)
        .await
        .unwrap();
}

async fn err_read_nexus() -> bool {
    let bdev = Bdev::lookup_by_name(NXNAME).expect("failed to lookup nexus");
    let d = bdev
        .open(true)
        .expect("failed open bdev")
        .into_handle()
        .unwrap();
    let mut buf = d.dma_malloc(512).expect("failed to allocate buffer");

    d.read_at(0, &mut buf).await.is_ok()
}

async fn err_read_nexus_both(succeed: bool) {
    let res1 = err_read_nexus().await;
    let res2 = err_read_nexus().await;

    if succeed {
        assert!(res1 && res2); // both succeeded
    } else {
        assert_ne!(res1, res2); // one succeeded, one failed
    }
}

async fn err_write_nexus(succeed: bool) {
    let bdev = Bdev::lookup_by_name(NXNAME).expect("failed to lookup nexus");
    let d = bdev
        .open(true)
        .expect("failed open bdev")
        .into_handle()
        .unwrap();
    let buf = d.dma_malloc(512).expect("failed to allocate buffer");

    match d.write_at(0, &buf).await {
        Ok(_) => {
            assert_eq!(succeed, true);
        }
        Err(_) => {
            assert_eq!(succeed, false);
        }
    };
}

fn reactor_run_millis(milliseconds: u64) {
    let (s, r) = unbounded::<()>();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(milliseconds));
        s.send(())
    });
    reactor_poll!(r);
}
