use libc::c_void;
use once_cell::sync::{Lazy, OnceCell};

use mayastor::{
    bdev::{device_create, device_destroy, device_lookup, device_open},
    core::{BlockDeviceHandle, DmaBuf, MayastorCliArgs},
};

use std::{
    alloc::Layout,
    slice,
    str,
    sync::{
        atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering},
        Arc,
    },
};

use spdk_sys::{self, iovec};

pub mod common;
use common::compose::MayastorTest;
use uuid::Uuid;

static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();
static INVOCATION_FLAG: AtomicBool = AtomicBool::new(false);

struct IoStats {
    reads: AtomicU64,
    writes: AtomicU64,
}

impl Default for IoStats {
    fn default() -> Self {
        Self {
            reads: AtomicU64::new(0),
            writes: AtomicU64::new(0),
        }
    }
}

static IO_STATS: Lazy<IoStats> = Lazy::new(IoStats::default);

const MAYASTOR_CTRLR_TITLE: &str = "Mayastor NVMe controler";
//const MAYASTOR_NQN_PREFIX: &str = "nqn.2019-05.io.openebs:";

fn get_ms() -> &'static MayastorTest<'static> {
    let instance =
        MAYASTOR.get_or_init(|| MayastorTest::new(MayastorCliArgs::default()));
    &instance
}

async fn launch_instance() -> String {
    return "nvmf://127.0.0.1:4420/replica0".to_string();
}

#[tokio::test]
async fn nvmf_device_create_destroy() {
    let ms = get_ms();
    let url = launch_instance().await;

    ms.spawn(async move {
        let name1 = device_create(&url).await.unwrap();

        // Check device properties for sanity.
        let bdev = device_lookup(&name1).unwrap();
        assert_eq!(bdev.product_name(), "NVMe disk");
        assert_eq!(bdev.driver_name(), "nvme");
        assert_eq!(bdev.device_name(), name1);

        assert_ne!(bdev.block_len(), 0);
        assert_ne!(bdev.num_blocks(), 0);
        assert_ne!(bdev.size_in_bytes(), 0);
        assert_eq!(bdev.block_len() * bdev.num_blocks(), bdev.size_in_bytes());

        Uuid::parse_str(&bdev.uuid()).unwrap();

        // Destroy the device the first time - should succeed.
        device_destroy(&url).await.unwrap();

        // Destroy the device which is supposed to be already destroyed -
        // should fail.
        assert!(device_destroy(&url).await.is_err());

        // Create the same device one more time - should succeed.
        let name2 = device_create(&url).await.unwrap();

        // Destroy the device the second time - should succeed.
        device_destroy(&url).await.unwrap();

        // Device paths should match.
        assert_eq!(name1, name2);
    })
    .await;
}

#[tokio::test]
async fn nvmf_device_identify_controller() {
    let ms = get_ms();
    let url = launch_instance().await;
    let u = url.clone();

    ms.spawn(async move {
        let name = device_create(&url).await.unwrap();
        let descr = device_open(&name, false).unwrap();
        let handle = descr.into_handle().unwrap();

        let _buf = handle.nvme_identify_ctrlr().await.unwrap();
    })
    .await;

    println!(
        "Sleeping for 1 sec to let all async resource cleanup operations complete"
    );
    tokio::time::delay_for(std::time::Duration::from_secs(1)).await;
    println!("Awakened.");

    ms.spawn(async move {
        device_destroy(&u).await.unwrap();
    })
    .await;
}

const GUARD_PATTERN: u8 = 0xFF;
const IO_PATTERN: u8 = 0x77;

fn check_buf_pattern(buf: &DmaBuf, pattern: u8) {
    for i in buf.as_slice() {
        assert_eq!(*i, pattern, "Buffer doesn't match the pattern");
    }
}

fn create_io_buffer(alignment: u64, size: u64, pattern: u8) -> DmaBuf {
    let mut buf = DmaBuf::new(size, alignment).unwrap();

    for i in buf.as_mut_slice() {
        *i = pattern;
    }

    buf
}

fn clear_callback_invocation_flag() {
    INVOCATION_FLAG.store(false, Ordering::Relaxed);
}

fn flag_callback_invocation() {
    assert_eq!(
        INVOCATION_FLAG.compare_exchange(
            false,
            true,
            Ordering::Acquire,
            Ordering::Relaxed
        ),
        Ok(false),
        "Callback is called more than once"
    );
}

fn check_callback_invocation() {
    assert_eq!(
        INVOCATION_FLAG.compare_exchange(
            true,
            false,
            Ordering::Acquire,
            Ordering::Relaxed
        ),
        Ok(true),
        "Callback has not been called"
    );
}

fn reset_io_stats() {
    IO_STATS.reads.store(0, Ordering::Relaxed);
    IO_STATS.writes.store(0, Ordering::Relaxed);
}

fn io_stat_account_read() {
    IO_STATS.reads.fetch_add(1, Ordering::SeqCst);
}

fn io_stat_account_write() {
    IO_STATS.writes.fetch_add(1, Ordering::SeqCst);
}

fn check_io_stats(reads: u64, writes: u64) {
    assert_eq!(
        IO_STATS.reads.load(Ordering::Relaxed),
        reads,
        "Number of expected read I/O operations mismatches"
    );
    assert_eq!(
        IO_STATS.writes.load(Ordering::Relaxed),
        writes,
        "Number of expected write I/O operations mismatches"
    );
}

#[tokio::test]
async fn nvmf_device_read_write_at() {
    let ms = get_ms();
    let url = launch_instance().await;
    let u = url.clone();

    // Perform a sequence of write-read operations to write test pattern to the
    // device via write_at() and verify data integrity via read_at().
    ms.spawn(async move {
        const BUF_SIZE: u64 = 32768;
        const OP_OFFSET: u64 = 1024 * 1024;

        let name = device_create(&url).await.unwrap();
        let descr = device_open(&name, false).unwrap();
        let handle = descr.into_handle().unwrap();
        let device = handle.get_device();

        let guard_buf =
            create_io_buffer(device.alignment(), BUF_SIZE, GUARD_PATTERN);

        // First, write 2 guard buffers before and after target I/O location.
        let mut r = handle.write_at(OP_OFFSET, &guard_buf).await.unwrap();
        assert_eq!(r, BUF_SIZE, "The amount of data written mismatches");
        r = handle
            .write_at(OP_OFFSET + 2 * BUF_SIZE, &guard_buf)
            .await
            .unwrap();
        assert_eq!(r, BUF_SIZE, "The amount of data written mismatches");

        // Write data buffer between guard buffers.
        let data_buf =
            create_io_buffer(device.alignment(), BUF_SIZE, IO_PATTERN);
        r = handle
            .write_at(OP_OFFSET + BUF_SIZE, &data_buf)
            .await
            .unwrap();
        assert_eq!(r, BUF_SIZE, "The amount of data written mismatches");

        // Check the first guard buffer.
        let g1 = DmaBuf::new(BUF_SIZE, device.alignment()).unwrap();
        r = handle.read_at(OP_OFFSET, &g1).await.unwrap();
        assert_eq!(r, BUF_SIZE, "The amount of data read mismatches");
        check_buf_pattern(&g1, GUARD_PATTERN);

        // Check the second guard buffer.
        let g2 = DmaBuf::new(BUF_SIZE, device.alignment()).unwrap();
        r = handle.read_at(OP_OFFSET + 2 * BUF_SIZE, &g2).await.unwrap();
        assert_eq!(r, BUF_SIZE, "The amount of data read mismatches");
        check_buf_pattern(&g2, GUARD_PATTERN);

        // Check the data region.
        let dbuf = DmaBuf::new(BUF_SIZE, device.alignment()).unwrap();
        r = handle.read_at(OP_OFFSET + BUF_SIZE, &dbuf).await.unwrap();
        assert_eq!(r, BUF_SIZE, "The amount of data read mismatches");
        check_buf_pattern(&dbuf, IO_PATTERN);
    })
    .await;

    println!(
        "Sleeping for 1 sec to let all async resource cleanup operations complete"
    );
    tokio::time::delay_for(std::time::Duration::from_secs(1)).await;
    println!("Awakened.");

    // Safely destroy the device once all handles are freed.
    ms.spawn(async move {
        device_destroy(&u).await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn nvmf_device_readv_test() {
    const BUF_SIZE: u64 = 32768;

    let ms = get_ms();
    let u = Arc::new(launch_instance().await);
    let mut url = Arc::clone(&u);

    // Placeholder structure to let all the fields outlive API invocations.
    struct IoCtx {
        iov: iovec,
        iovcnt: i32,
        dma_buf: DmaBuf,
        handle: Box<dyn BlockDeviceHandle>,
    }

    // Read completion callback.
    fn read_completion_callback(success: bool, ctx: *mut c_void) {
        // Make sure callback is invoked only once.
        flag_callback_invocation();

        assert!(success, "readv_blocks() failed");
        // Make sure we were passed the same pattern string as requested.
        let s = unsafe {
            let slice = slice::from_raw_parts(
                ctx as *const u8,
                MAYASTOR_CTRLR_TITLE.len(),
            );
            str::from_utf8(slice).unwrap()
        };

        assert_eq!(s, MAYASTOR_CTRLR_TITLE);
    }

    // Clear callback invocation flag.
    clear_callback_invocation_flag();

    let buf_ptr = ms
        .spawn(async move {
            let name = device_create(&(*url)).await.unwrap();
            let descr = device_open(&name, false).unwrap();
            let handle = descr.into_handle().unwrap();
            let device = handle.get_device();

            // Create a buffer with the guard pattern.
            let mut io_ctx = IoCtx {
                iov: iovec::default(),
                iovcnt: 1,
                dma_buf: create_io_buffer(
                    device.alignment(),
                    BUF_SIZE,
                    GUARD_PATTERN,
                ),
                handle,
            };

            io_ctx.iov.iov_base = *io_ctx.dma_buf;
            io_ctx.iov.iov_len = BUF_SIZE;

            // Initiate a read operation into the buffer.
            io_ctx
                .handle
                .readv_blocks(
                    &mut io_ctx.iov,
                    io_ctx.iovcnt,
                    (3 * 1024 * 1024) / device.block_len(),
                    BUF_SIZE / device.block_len(),
                    read_completion_callback,
                    // Use a predefined string to check that we receive the
                    // same context pointer as we pass upon
                    // invocation. For this call we don't need any
                    // specific, operation-related context.
                    MAYASTOR_CTRLR_TITLE.as_ptr() as *mut c_void,
                )
                .unwrap();

            AtomicPtr::new(Box::into_raw(Box::new(io_ctx)))
        })
        .await;

    // Sleep for a few seconds to let I/O operation complete.
    println!("Sleeping for 2 secs to let I/O operation complete");
    tokio::time::delay_for(std::time::Duration::from_secs(3)).await;
    println!("Awakened.");

    // Check that the callback has been called.
    check_callback_invocation();

    // Check the contents of the buffer to make sure it has been overwritten
    // with data pattern. We should see all zeroes in the buffer instead of
    // the guard pattern.
    let b = buf_ptr.into_inner();
    check_buf_pattern(unsafe { &((*b).dma_buf) }, 0);

    // Turn placeholder structure into a box to trigger drop() action
    // on handle's resources once the box is dropped.
    ms.spawn(async move {
        let _ph = unsafe { Box::from_raw(b) };
    })
    .await;

    // Sleep for 1 sec to let async resource cleanup actions be processed.
    println!(
        "Sleeping for 1 sec to let all async resource cleanup operations complete"
    );
    tokio::time::delay_for(std::time::Duration::from_secs(1)).await;
    println!("Awakened.");

    // Once all handles are closed, destroy the device.
    url = Arc::clone(&u);
    ms.spawn(async move {
        device_destroy(&(*url)).await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn nvmf_device_writev_test() {
    const BUF_SIZE: u64 = 128 * 1024;
    const OP_OFFSET: u64 = 4 * 1024 * 1024;

    let ms = get_ms();
    let u = Arc::new(launch_instance().await);
    let url = Arc::clone(&u);

    // Read completion callback.
    fn write_completion_callback(success: bool, ctx: *mut c_void) {
        // Make sure callback is invoked only once.
        flag_callback_invocation();

        assert!(success, "writev_blocks() failed");
        // Make sure we were passed the same pattern string as requested.
        let s = unsafe {
            let slice = slice::from_raw_parts(
                ctx as *const u8,
                MAYASTOR_CTRLR_TITLE.len(),
            );
            str::from_utf8(slice).unwrap()
        };

        assert_eq!(s, MAYASTOR_CTRLR_TITLE);
    }

    // Placeholder structure to let all the fields outlive API invocations.
    struct IoCtx {
        iov: iovec,
        dma_buf: DmaBuf,
        handle: Box<dyn BlockDeviceHandle>,
    }

    // Clear callback invocation flag.
    clear_callback_invocation_flag();

    let ctx = ms
        .spawn(async move {
            let name = device_create(&(*url)).await.unwrap();
            let descr = device_open(&name, false).unwrap();
            let handle = descr.into_handle().unwrap();
            let device = handle.get_device();

            let guard_buf =
                create_io_buffer(device.alignment(), BUF_SIZE, GUARD_PATTERN);

            // First, write 2 guard buffers before and after target I/O
            // location.
            let mut r = handle.write_at(OP_OFFSET, &guard_buf).await.unwrap();
            assert_eq!(r, BUF_SIZE, "The amount of data written mismatches");
            r = handle
                .write_at(OP_OFFSET + 2 * BUF_SIZE, &guard_buf)
                .await
                .unwrap();
            assert_eq!(r, BUF_SIZE, "The amount of data written mismatches");

            let mut ctx = IoCtx {
                iov: iovec::default(),
                dma_buf: create_io_buffer(
                    device.alignment(),
                    BUF_SIZE,
                    IO_PATTERN,
                ),
                handle,
            };

            ctx.iov.iov_base = *ctx.dma_buf;
            ctx.iov.iov_len = BUF_SIZE;

            // Write data buffer between guard buffers to catch writes outside
            // the range.
            ctx.handle
                .writev_blocks(
                    &mut ctx.iov,
                    1,
                    (OP_OFFSET + BUF_SIZE) / device.block_len(),
                    BUF_SIZE / device.block_len(),
                    write_completion_callback,
                    // Use a predefined string to check that we receive the
                    // same context pointer as we pass upon
                    // invocation. For this call we don't need any
                    // specific, operation-related context.
                    MAYASTOR_CTRLR_TITLE.as_ptr() as *mut c_void,
                )
                .unwrap();

            AtomicPtr::new(Box::into_raw(Box::new(ctx)))
        })
        .await;

    // Sleep for a few seconds to let I/O operation complete.
    println!("Sleeping for 2 secs to let I/O operation complete");
    tokio::time::delay_for(std::time::Duration::from_secs(3)).await;
    println!("Awakened.");

    // Check that the callback has been called.
    check_callback_invocation();

    // Read data just written and check that no boundaries were crossed.
    ms.spawn(async move {
        let ctx = unsafe { Box::<IoCtx>::from_raw(ctx.into_inner()) };
        let device = ctx.handle.get_device();

        // Check the first guard buffer.
        let g1 = DmaBuf::new(BUF_SIZE, device.alignment()).unwrap();
        let mut r = ctx.handle.read_at(OP_OFFSET, &g1).await.unwrap();
        assert_eq!(r, BUF_SIZE, "The amount of data read mismatches");
        check_buf_pattern(&g1, GUARD_PATTERN);

        // Check the second guard buffer.
        let g2 = DmaBuf::new(BUF_SIZE, device.alignment()).unwrap();
        r = ctx
            .handle
            .read_at(OP_OFFSET + 2 * BUF_SIZE, &g2)
            .await
            .unwrap();
        assert_eq!(r, BUF_SIZE, "The amount of data read mismatches");
        check_buf_pattern(&g2, GUARD_PATTERN);

        // Check the data region between guard buffers.
        let dbuf = DmaBuf::new(BUF_SIZE, device.alignment()).unwrap();
        r = ctx
            .handle
            .read_at(OP_OFFSET + BUF_SIZE, &dbuf)
            .await
            .unwrap();
        assert_eq!(r, BUF_SIZE, "The amount of data read mismatches");
        check_buf_pattern(&dbuf, IO_PATTERN);
        // Device handle will be dropped once the box is dropped, which triggers
        // async reclamation for handle's resources.
    })
    .await;

    println!(
        "Sleeping for 1 sec to let all async resource cleanup operations complete"
    );
    tokio::time::delay_for(std::time::Duration::from_secs(1)).await;
    println!("Awakened.");

    // Safely destroy the device once all handles are freed.
    ms.spawn(async move {
        device_destroy(&u).await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn nvmf_device_readv_iovs_test() {
    const OP_OFFSET: u64 = 6 * 1024 * 1024;
    const IOVCNT: usize = 5;
    const IOVSIZES: [u64; IOVCNT] = [
        // Sizes of I/O vectors, in kilobytes.
        512 * 1024,
        128 * 1024,
        16 * 1024,
        256 * 1024,
        128 * 1024,
    ];
    let iosize = IOVSIZES.iter().sum();

    let ms = get_ms();
    let u = Arc::new(launch_instance().await);
    let mut url = Arc::clone(&u);

    // Read completion callback.
    fn read_completion_callback(success: bool, ctx: *mut c_void) {
        // Make sure callback is invoked only once.
        flag_callback_invocation();

        assert!(success, "readv_blocks() failed");
        // Make sure we were passed the same pattern string as requested.
        let s = unsafe {
            let slice = slice::from_raw_parts(
                ctx as *const u8,
                MAYASTOR_CTRLR_TITLE.len(),
            );
            str::from_utf8(slice).unwrap()
        };

        assert_eq!(s, MAYASTOR_CTRLR_TITLE);
    }

    // Placeholder structure to let all the fields outlive API invocations.
    struct IoCtx {
        iovs: *mut iovec,
        buffers: Vec<DmaBuf>,
        handle: Box<dyn BlockDeviceHandle>,
    }

    // Clear callback invocation flag.
    clear_callback_invocation_flag();

    let io_ctx = ms
        .spawn(async move {
            let device_name = device_create(&(*url)).await.unwrap();
            let descr = device_open(&device_name, false).unwrap();
            let handle = descr.into_handle().unwrap();
            let device = handle.get_device();

            let mut buffers = Vec::<DmaBuf>::with_capacity(IOVCNT);

            // Allocate phsycally continous memory for storing raw I/O vectors.
            let l = Layout::array::<iovec>(IOVCNT).unwrap();
            let iovs = unsafe { std::alloc::alloc(l) } as *mut iovec;

            for (i, s) in IOVSIZES.iter().enumerate().take(IOVCNT) {
                let mut iov = iovec::default();
                let buf =
                    create_io_buffer(device.alignment(), *s, GUARD_PATTERN);

                iov.iov_base = *buf;
                iov.iov_len = buf.len();

                buffers.push(buf);
                unsafe { *iovs.add(i) = iov };
            }

            let io_ctx = IoCtx {
                iovs,
                buffers,
                handle,
            };

            // First, write data pattern of required size.
            let data_buf =
                create_io_buffer(device.alignment(), iosize, IO_PATTERN);
            let r = io_ctx.handle.write_at(OP_OFFSET, &data_buf).await.unwrap();
            assert_eq!(r, iosize, "The amount of data written mismatches");

            // Initiate a read operation into the I/O vectors.
            io_ctx
                .handle
                .readv_blocks(
                    io_ctx.iovs,
                    IOVCNT as i32,
                    OP_OFFSET / device.block_len(),
                    iosize / device.block_len(),
                    read_completion_callback,
                    // Use a predefined string to check that we receive the
                    // same context pointer as we pass upon
                    // invocation. For this call we don't need any
                    // specific, operation-related context.
                    MAYASTOR_CTRLR_TITLE.as_ptr() as *mut c_void,
                )
                .unwrap();

            AtomicPtr::new(Box::into_raw(Box::new(io_ctx)))
        })
        .await;

    // Sleep for a few seconds to let I/O operation complete.
    println!("Sleeping for 2 secs to let I/O operation complete");
    tokio::time::delay_for(std::time::Duration::from_secs(3)).await;
    println!("Awakened.");

    // Check that the callback has been called.
    check_callback_invocation();

    url = Arc::clone(&u);
    ms.spawn(async move {
        let ctx = unsafe { Box::<IoCtx>::from_raw(io_ctx.into_inner()) };

        for b in &ctx.buffers {
            check_buf_pattern(b, IO_PATTERN);
        }
        // Device handle will be dropped once the box is dropped, which triggers
        // async reclamation for handle's resources.
    })
    .await;

    println!(
        "Sleeping for 1 sec to let all async resource cleanup operations complete"
    );
    tokio::time::delay_for(std::time::Duration::from_secs(1)).await;
    println!("Awakened.");

    // Safely destroy the device once all handles are freed.
    ms.spawn(async move {
        device_destroy(&(*url)).await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn nvmf_device_writev_iovs_test() {
    const GUARD_SIZE: u64 = 32 * 1024;
    const OP_OFFSET: u64 = 10 * 1024 * 1024;
    const IOVCNT: usize = 6;
    const IOVSIZES: [u64; IOVCNT] = [
        // Sizes of I/O vectors, in kilobytes.
        512 * 1024,
        256 * 1024,
        512 * 1024,
        8 * 1024,
        128 * 1024,
        64 * 1024,
    ];
    let iosize = IOVSIZES.iter().sum();

    let ms = get_ms();
    let u = Arc::new(launch_instance().await);
    let url = Arc::clone(&u);

    // Clear callback invocation flag.
    clear_callback_invocation_flag();

    // Write completion callback.
    fn write_completion_callback(success: bool, ctx: *mut c_void) {
        // Make sure callback is invoked only once.
        flag_callback_invocation();

        assert!(success, "writev_blocks() failed");
        // Make sure we were passed the same pattern string as requested.
        let s = unsafe {
            let slice = slice::from_raw_parts(
                ctx as *const u8,
                MAYASTOR_CTRLR_TITLE.len(),
            );
            str::from_utf8(slice).unwrap()
        };

        assert_eq!(s, MAYASTOR_CTRLR_TITLE);
    }

    // Placeholder structure to let all the fields outlive API invocations.
    struct IoCtx {
        iovs: *mut iovec,
        buffers: Vec<DmaBuf>,
        handle: Box<dyn BlockDeviceHandle>,
    }

    let io_ctx = ms
        .spawn(async move {
            let device_name = device_create(&(*url)).await.unwrap();
            let descr = device_open(&device_name, false).unwrap();
            let handle = descr.into_handle().unwrap();
            let device = handle.get_device();

            let mut buffers = Vec::<DmaBuf>::with_capacity(IOVCNT);

            // Allocate phsycally continous memory for storing raw I/O vectors.
            let l = Layout::array::<iovec>(IOVCNT).unwrap();
            let iovs = unsafe { std::alloc::alloc(l) } as *mut iovec;

            for (i, s) in IOVSIZES.iter().enumerate().take(IOVCNT) {
                let mut iov = iovec::default();
                let buf = create_io_buffer(device.alignment(), *s, IO_PATTERN);

                iov.iov_base = *buf;
                iov.iov_len = buf.len();

                buffers.push(buf);
                unsafe { *iovs.add(i) = iov };
            }

            let io_ctx = IoCtx {
                iovs,
                buffers,
                handle,
            };

            // First, write 2 guard buffers before and after target I/O
            // location.
            let guard_buf =
                create_io_buffer(device.alignment(), GUARD_SIZE, GUARD_PATTERN);
            let mut r = io_ctx
                .handle
                .write_at(OP_OFFSET - GUARD_SIZE, &guard_buf)
                .await
                .unwrap();
            assert_eq!(r, GUARD_SIZE, "The amount of data written mismatches");
            r = io_ctx
                .handle
                .write_at(OP_OFFSET + iosize, &guard_buf)
                .await
                .unwrap();
            assert_eq!(r, GUARD_SIZE, "The amount of data written mismatches");

            // Initiate a write operation into the I/O vectors.
            io_ctx
                .handle
                .writev_blocks(
                    io_ctx.iovs,
                    IOVCNT as i32,
                    OP_OFFSET / device.block_len(),
                    iosize / device.block_len(),
                    write_completion_callback,
                    // Use a predefined string to check that we receive the
                    // same context pointer as we pass upon
                    // invocation. For this call we don't need any
                    // specific, operation-related context.
                    MAYASTOR_CTRLR_TITLE.as_ptr() as *mut c_void,
                )
                .unwrap();

            AtomicPtr::new(Box::into_raw(Box::new(io_ctx)))
        })
        .await;

    // Sleep for a few seconds to let I/O operation complete.
    println!("Sleeping for 2 secs to let I/O operation complete");
    tokio::time::delay_for(std::time::Duration::from_secs(3)).await;
    println!("Awakened.");

    // Check that the callback has been called.
    check_callback_invocation();

    ms.spawn(async move {
        let ctx = unsafe { Box::<IoCtx>::from_raw(io_ctx.into_inner()) };
        let device = ctx.handle.get_device();

        // Make sure buffers content didn't change.
        for b in &ctx.buffers {
            check_buf_pattern(b, IO_PATTERN);
        }

        // Check the first guard buffer.
        let g1 = DmaBuf::new(GUARD_SIZE, device.alignment()).unwrap();
        let mut r = ctx
            .handle
            .read_at(OP_OFFSET - GUARD_SIZE, &g1)
            .await
            .unwrap();
        assert_eq!(r, GUARD_SIZE, "The amount of data read mismatches");
        check_buf_pattern(&g1, GUARD_PATTERN);

        // Check the second guard buffer.
        let g2 = DmaBuf::new(GUARD_SIZE, device.alignment()).unwrap();
        r = ctx.handle.read_at(OP_OFFSET + iosize, &g2).await.unwrap();
        assert_eq!(r, GUARD_SIZE, "The amount of data read mismatches");
        check_buf_pattern(&g2, GUARD_PATTERN);

        // Check the data region between guard buffers.
        let dbuf = DmaBuf::new(iosize, device.alignment()).unwrap();
        r = ctx.handle.read_at(OP_OFFSET, &dbuf).await.unwrap();
        assert_eq!(r, iosize, "The amount of data read mismatches");
        check_buf_pattern(&dbuf, IO_PATTERN);
        // Device handle will be dropped once the box is dropped, which triggers
        // async reclamation for handle's resources.
    })
    .await;

    println!(
        "Sleeping for 1 sec to let all async resource cleanup operations complete"
    );
    tokio::time::delay_for(std::time::Duration::from_secs(1)).await;
    println!("Awakened.");

    // Safely destroy the device once all handles are freed.
    ms.spawn(async move {
        device_destroy(&(*u)).await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn nvmf_device_admin_ctrl() {
    let ms = get_ms();
    let url = launch_instance().await;
    let url2 = url.clone();

    ms.spawn(async move {
        let name = device_create(&url).await.unwrap();
        let descr = device_open(&name, false).unwrap();
        let handle = descr.into_handle().unwrap();

        handle.nvme_admin_custom(0xCF).await.expect_err(
            "successfully executed invalid NVMe admin command (0xCF)",
        );
    })
    .await;

    println!(
        "Sleeping for 1 sec to let all async resource cleanup operations complete"
    );
    tokio::time::delay_for(std::time::Duration::from_secs(1)).await;
    println!("Awakened.");

    // Destroy controller after all resources are freed.
    ms.spawn(async move {
        device_destroy(&url2).await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn nvmf_device_reset() {
    let ms = get_ms();
    let url = launch_instance().await;
    let url2 = url.clone();

    // Clear callback invocation flag.
    clear_callback_invocation_flag();

    struct DeviceIoCtx {
        handle: Box<dyn BlockDeviceHandle>,
    }

    // Read completion callback.
    fn reset_completion_callback(success: bool, ctx: *mut c_void) {
        // Make sure callback is invoked only once.
        flag_callback_invocation();

        assert!(success, "reset() failed");
        // Make sure we were passed the same pattern string as requested.
        let s = unsafe {
            let slice = slice::from_raw_parts(
                ctx as *const u8,
                MAYASTOR_CTRLR_TITLE.len(),
            );
            str::from_utf8(slice).unwrap()
        };

        assert_eq!(s, MAYASTOR_CTRLR_TITLE);
    }

    let op_ctx = ms
        .spawn(async move {
            let name = device_create(&url).await.unwrap();
            let descr = device_open(&name, false).unwrap();
            let handle = descr.into_handle().unwrap();

            handle
                .reset(
                    reset_completion_callback,
                    // Use a predefined string to check that we receive the
                    // same context pointer as we pass upon
                    // invocation. For this call we don't need any
                    // specific, operation-related context.
                    MAYASTOR_CTRLR_TITLE.as_ptr() as *mut c_void,
                )
                .unwrap();

            AtomicPtr::new(Box::into_raw(Box::new(DeviceIoCtx {
                handle,
            })))
        })
        .await;

    // Sleep for a few seconds to let reset operation complete.
    println!("Sleeping for 2 secs to let reset operation complete");
    tokio::time::delay_for(std::time::Duration::from_secs(2)).await;
    println!("Awakened.");

    // Check that the callback has been called.
    check_callback_invocation();

    ms.spawn(async move {
        let io_ctx = unsafe { Box::from_raw(op_ctx.into_inner()) };
        println!(
            "Identifying controller using a newly recreated I/O channels."
        );
        io_ctx.handle.nvme_identify_ctrlr().await.unwrap();
        println!("Controller successfully identified");
    })
    .await;

    println!(
        "Sleeping for 1 sec to let all async resource cleanup operations complete"
    );
    tokio::time::delay_for(std::time::Duration::from_secs(1)).await;
    println!("Awakened.");

    // Destroy controller after all resources are freed.
    ms.spawn(async move {
        device_destroy(&url2).await.unwrap();
    })
    .await;
}

async fn wipe_device_blocks(is_unmap: bool) {
    let ms = get_ms();
    let url = launch_instance().await;
    let url2 = url.clone();

    struct DeviceIoCtx {
        handle: Box<dyn BlockDeviceHandle>,
    }

    const BUF_SIZE: u64 = 32768;
    const OP_OFFSET: u64 = 12 * 1024 * 1024;

    // Read completion callback.
    fn wipe_completion_callback(success: bool, ctx: *mut c_void) {
        // Make sure callback is invoked only once.
        flag_callback_invocation();

        assert!(success, "block deallocation failed");
        // Make sure we were passed the same pattern string as requested.
        let s = unsafe {
            let slice = slice::from_raw_parts(
                ctx as *const u8,
                MAYASTOR_CTRLR_TITLE.len(),
            );
            str::from_utf8(slice).unwrap()
        };

        assert_eq!(s, MAYASTOR_CTRLR_TITLE);
    }

    // Clear callback invocation flag.
    clear_callback_invocation_flag();

    // Write guard buffers and data buffer, then unmap data buffer and check
    // if it was unmapped.
    let op_ctx = ms
        .spawn(async move {
            let name = device_create(&url).await.unwrap();
            let descr = device_open(&name, false).unwrap();
            let handle = descr.into_handle().unwrap();
            let device = handle.get_device();

            let guard_buf =
                create_io_buffer(device.alignment(), BUF_SIZE, GUARD_PATTERN);

            // First, write 2 guard buffers before and after target I/O
            // location.
            let mut r = handle.write_at(OP_OFFSET, &guard_buf).await.unwrap();
            assert_eq!(r, BUF_SIZE, "The amount of data written mismatches");
            r = handle
                .write_at(OP_OFFSET + 2 * BUF_SIZE, &guard_buf)
                .await
                .unwrap();
            assert_eq!(r, BUF_SIZE, "The amount of data written mismatches");

            // Write data buffer between guard buffers.
            let data_buf =
                create_io_buffer(device.alignment(), BUF_SIZE, IO_PATTERN);
            r = handle
                .write_at(OP_OFFSET + BUF_SIZE, &data_buf)
                .await
                .unwrap();
            assert_eq!(r, BUF_SIZE, "The amount of data written mismatches");

            if is_unmap {
                handle
                    .unmap_blocks(
                        (OP_OFFSET + BUF_SIZE) / device.block_len(),
                        BUF_SIZE / device.block_len(),
                        wipe_completion_callback,
                        // Use a predefined string to check that we receive the
                        // same context pointer as we pass upon
                        // invocation. For this call we don't need any
                        // specific, operation-related context.
                        MAYASTOR_CTRLR_TITLE.as_ptr() as *mut c_void,
                    )
                    .unwrap();
            } else {
                handle
                    .write_zeroes(
                        (OP_OFFSET + BUF_SIZE) / device.block_len(),
                        BUF_SIZE / device.block_len(),
                        wipe_completion_callback,
                        // Use a predefined string to check that we receive the
                        // same context pointer as we pass upon
                        // invocation. For this call we don't need any
                        // specific, operation-related context.
                        MAYASTOR_CTRLR_TITLE.as_ptr() as *mut c_void,
                    )
                    .unwrap();
            }

            AtomicPtr::new(Box::into_raw(Box::new(DeviceIoCtx {
                handle,
            })))
        })
        .await;

    // Sleep for a few seconds to let unmap operation complete.
    println!("Sleeping for 2 secs to let operation complete");
    tokio::time::delay_for(std::time::Duration::from_secs(3)).await;
    println!("Awakened.");

    ms.spawn(async move {
        let io_ctx = unsafe { Box::from_raw(op_ctx.into_inner()) };
        let device = io_ctx.handle.get_device();

        // Check the first guard buffer.
        let g1 = DmaBuf::new(BUF_SIZE, device.alignment()).unwrap();
        let mut r = io_ctx.handle.read_at(OP_OFFSET, &g1).await.unwrap();
        assert_eq!(r, BUF_SIZE, "The amount of data read mismatches");
        check_buf_pattern(&g1, GUARD_PATTERN);

        // Check the second guard buffer.
        let g2 = DmaBuf::new(BUF_SIZE, device.alignment()).unwrap();
        r = io_ctx
            .handle
            .read_at(OP_OFFSET + 2 * BUF_SIZE, &g2)
            .await
            .unwrap();
        assert_eq!(r, BUF_SIZE, "The amount of data read mismatches");
        check_buf_pattern(&g2, GUARD_PATTERN);

        // Check that data buffer has been unmapped.
        // Note that we allocate a buffer with non-zero content to make sure we
        // read zeroes afterwards.
        let dbuf = create_io_buffer(device.alignment(), BUF_SIZE, 0x1);
        r = io_ctx
            .handle
            .read_at(OP_OFFSET + BUF_SIZE, &dbuf)
            .await
            .unwrap();
        assert_eq!(r, BUF_SIZE, "The amount of data read mismatches");
        check_buf_pattern(&dbuf, 0x0); // Unmapped blocks must be read as
                                       // zeroes.
    })
    .await;

    println!(
        "Sleeping for 1 sec to let all async resource cleanup operations complete"
    );
    tokio::time::delay_for(std::time::Duration::from_secs(1)).await;
    println!("Awakened.");

    // Destroy controller after all resources are freed.
    ms.spawn(async move {
        device_destroy(&url2).await.unwrap();
    })
    .await;

    // Check that the callback has been called.
    check_callback_invocation();
}

#[tokio::test]
async fn nvmf_device_unmap_blocks() {
    wipe_device_blocks(true).await;
}

#[tokio::test]
async fn nvmf_device_write_zeroes() {
    wipe_device_blocks(false).await;
}

#[tokio::test]
async fn nvmf_reset_abort_io() {
    const BUF_SIZE: u64 = 32768;
    const NUM_IOS: u64 = 4;

    let ms = get_ms();
    let u = Arc::new(launch_instance().await);
    let mut url = Arc::clone(&u);

    // Placeholder structure to let all the fields outlive API invocations.
    struct IoCtx {
        iov: iovec,
        iovcnt: i32,
        dma_buf: DmaBuf,
        handle: Box<dyn BlockDeviceHandle>,
    }

    // Read I/O completion callback.
    fn read_completion_callback(success: bool, ctx: *mut c_void) {
        assert_eq!(success, false, "read I/O operation completed successfully");

        // Make sure we were passed the same pattern string as requested.
        let s = unsafe {
            let slice = slice::from_raw_parts(
                ctx as *const u8,
                MAYASTOR_CTRLR_TITLE.len(),
            );
            str::from_utf8(slice).unwrap()
        };

        assert_eq!(s, MAYASTOR_CTRLR_TITLE);
        io_stat_account_read();
    }

    // Write I/O completion callback.
    fn write_completion_callback(success: bool, ctx: *mut c_void) {
        assert_eq!(
            success, false,
            "write I/O operation completed successfully"
        );

        // Make sure we were passed the same pattern string as requested.
        let s = unsafe {
            let slice = slice::from_raw_parts(
                ctx as *const u8,
                MAYASTOR_CTRLR_TITLE.len(),
            );
            str::from_utf8(slice).unwrap()
        };

        assert_eq!(s, MAYASTOR_CTRLR_TITLE);
        io_stat_account_write();
    }

    // Reset completion calback.
    fn reset_completion_callback(success: bool, ctx: *mut c_void) {
        flag_callback_invocation();
        assert!(success, "Reset failed");

        // Make sure we were passed the same pattern string as requested.
        let s = unsafe {
            let slice = slice::from_raw_parts(
                ctx as *const u8,
                MAYASTOR_CTRLR_TITLE.len(),
            );
            str::from_utf8(slice).unwrap()
        };
        assert_eq!(s, MAYASTOR_CTRLR_TITLE);
    }

    // Clear callback invocation flag and I/O stats.
    clear_callback_invocation_flag();
    reset_io_stats();

    let buf_ptr = ms
        .spawn(async move {
            let name = device_create(&(*url)).await.unwrap();
            let descr = device_open(&name, false).unwrap();
            let handle = descr.into_handle().unwrap();
            let device = handle.get_device();

            let mut io_ctx = IoCtx {
                iov: iovec::default(),
                iovcnt: 1,
                dma_buf: create_io_buffer(
                    device.alignment(),
                    BUF_SIZE,
                    GUARD_PATTERN,
                ),
                handle,
            };

            io_ctx.iov.iov_base = *io_ctx.dma_buf;
            io_ctx.iov.iov_len = BUF_SIZE;

            // Initiate a 3 read and 3 write operations into the buffer.
            // We use the same IOVs as we don't care about the I/O result and
            // care only about failures which we're gonna trigger.
            for _ in 0 .. NUM_IOS {
                io_ctx
                    .handle
                    .readv_blocks(
                        &mut io_ctx.iov,
                        io_ctx.iovcnt,
                        (3 * 1024 * 1024) / device.block_len(),
                        BUF_SIZE / device.block_len(),
                        read_completion_callback,
                        // Use a predefined string to check that we receive the
                        // same context pointer as we pass upon
                        // invocation. For this call we don't need any
                        // specific, operation-related context.
                        MAYASTOR_CTRLR_TITLE.as_ptr() as *mut c_void,
                    )
                    .unwrap();

                io_ctx
                    .handle
                    .writev_blocks(
                        &mut io_ctx.iov,
                        io_ctx.iovcnt,
                        (3 * 1024 * 1024) / device.block_len(),
                        BUF_SIZE / device.block_len(),
                        write_completion_callback,
                        // Use a predefined string to check that we receive the
                        // same context pointer as we pass upon
                        // invocation. For this call we don't need any
                        // specific, operation-related context.
                        MAYASTOR_CTRLR_TITLE.as_ptr() as *mut c_void,
                    )
                    .unwrap();
            }

            // Reset the controller with active I/O requests.
            io_ctx
                .handle
                .reset(
                    reset_completion_callback,
                    // Use a predefined string to check that we receive the
                    // same context pointer as we pass upon
                    // invocation. For this call we don't need any
                    // specific, operation-related context.
                    MAYASTOR_CTRLR_TITLE.as_ptr() as *mut c_void,
                )
                .unwrap();

            AtomicPtr::new(Box::into_raw(Box::new(io_ctx)))
        })
        .await;

    // Sleep for a few seconds to let all I/O operations be aborted.
    println!("Sleeping for 1 sec to let reset hit I/O operations");
    tokio::time::delay_for(std::time::Duration::from_secs(1)).await;
    println!("Awakened.");

    // Check that the reset callback has been called and
    // all I/O related callbacks have also been called.
    check_callback_invocation();
    check_io_stats(NUM_IOS, NUM_IOS);

    // Check the contents of the buffer to make sure it has been overwritten
    // with data pattern. We should see all zeroes in the buffer instead of
    // the guard pattern.
    let b = buf_ptr.into_inner();
    // check_buf_pattern(unsafe { &((*b).dma_buf) }, 0);

    // Turn placeholder structure into a box to trigger drop() action
    // on handle's resources once the box is dropped.
    ms.spawn(async move {
        let _ph = unsafe { Box::from_raw(b) };
    })
    .await;

    // Sleep for 1 sec to let async resource cleanup actions be processed.
    println!(
        "Sleeping for 1 sec to let all async resource cleanup operations complete"
    );
    tokio::time::delay_for(std::time::Duration::from_secs(1)).await;
    println!("Awakened.");

    // Once all handles are closed, destroy the device.
    url = Arc::clone(&u);
    ms.spawn(async move {
        device_destroy(&(*url)).await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn nvmf_device_io_handle_cleanup() {
    let ms = get_ms();
    let url = launch_instance().await;

    const BUF_SIZE: u64 = 32768;
    const OP_OFFSET: u64 = 1024 * 1024;

    struct DeviceIoCtx {
        handle: Box<dyn BlockDeviceHandle>,
        alignment: u64,
    }

    // 1. Obtain a valid I/O handle for the NVMe device and
    // remove the device whilst keeping the I/O handle open.
    let op_ctx = ms
        .spawn(async move {
            let name = device_create(&url).await.unwrap();
            let descr = device_open(&name, false).unwrap();
            let handle = descr.into_handle().unwrap();
            let alignment = handle.get_device().alignment();

            // Controller identification command must succeed the first time.
            handle.nvme_identify_ctrlr().await.unwrap();

            // I/O command must succeed.
            let buf = DmaBuf::new(BUF_SIZE, alignment).unwrap();
            let r = handle.read_at(OP_OFFSET, &buf).await.unwrap();
            assert_eq!(r, BUF_SIZE, "The amount of data read mismatches");

            // Make sure device can still be looked up by its name before
            // removal.
            device_lookup(&name).unwrap();

            device_destroy(&url).await.unwrap();

            // Make sure device can't be looked up by its name after removal.
            assert!(
                device_lookup(&name).is_none(),
                "Device still resolvable by name after removal"
            );

            AtomicPtr::new(Box::into_raw(Box::new(DeviceIoCtx {
                handle,
                alignment,
            })))
        })
        .await;

    println!("Sleeping for 1 sec to let device cleanup operations complete");
    tokio::time::delay_for(std::time::Duration::from_secs(1)).await;
    println!("Awakened.");

    // 2. Try to repeat the same I/O operations: expecting
    // all operations to fail as all controller's I/O resources
    // are supposed to be invalidated after device removal.
    ms.spawn(async move {
        let io_ctx = unsafe { Box::from_raw(op_ctx.into_inner()) };
        println!(
            "Identifying controller using a newly recreated I/O channels."
        );
        // Make sure the same NVMe admin command now fail.
        io_ctx
            .handle
            .nvme_identify_ctrlr()
            .await
            .expect_err("Controller successfully identified");

        // Make sure the same I/O command now fail.
        let buf = DmaBuf::new(BUF_SIZE, io_ctx.alignment).unwrap();
        io_ctx
            .handle
            .read_at(OP_OFFSET, &buf)
            .await
            .expect_err("Data successfully read");
    })
    .await;
}
