use async_trait::async_trait;
use futures::channel::oneshot;
use nix::errno::Errno;
use once_cell::sync::OnceCell;
use std::{
    alloc::Layout,
    mem::ManuallyDrop,
    os::raw::c_void,
    ptr::NonNull,
    sync::Arc,
};

use crate::core::mempool::MemoryPool;

use crate::{
    bdev::dev::nvmx::{
        channel::NvmeControllerIoChannel,
        utils,
        utils::{
            nvme_command_status,
            nvme_cpl_is_pi_error,
            nvme_cpl_succeeded,
        },
        NvmeBlockDevice,
        NvmeIoChannel,
        NvmeNamespace,
        NVME_CONTROLLERS,
    },
    core::{
        nvme_admin_opc,
        BlockDevice,
        BlockDeviceHandle,
        CoreError,
        DmaBuf,
        DmaError,
        GenericStatusCode,
        IoCompletionCallback,
        IoCompletionCallbackArg,
        IoCompletionStatus,
        IoType,
        NvmeCommandStatus,
    },
    ffihelper::{cb_arg, done_cb},
};

use spdk_sys::{
    self,
    iovec,
    spdk_get_io_channel,
    spdk_io_channel,
    spdk_nvme_cpl,
    spdk_nvme_ctrlr,
    spdk_nvme_ctrlr_cmd_admin_raw,
    spdk_nvme_dsm_range,
    spdk_nvme_ns_cmd_dataset_management,
    spdk_nvme_ns_cmd_read,
    spdk_nvme_ns_cmd_readv,
    spdk_nvme_ns_cmd_write,
    spdk_nvme_ns_cmd_writev,
};

use super::NvmeIoChannelInner;

/*
 * I/O context for NVMe controller I/O operation. Used as a placeholder for
 * storing user context and also private state of I/O operations, specific to
 * the controller.
 */
struct NvmeIoCtx {
    cb: IoCompletionCallback,
    cb_arg: IoCompletionCallbackArg,
    iov: *mut iovec,
    iovcnt: u64,
    iovpos: u64,
    iov_offset: u64,
    op: IoType,
    num_blocks: u64,
    channel: *mut spdk_io_channel,
}

unsafe impl Send for NvmeIoCtx {}
unsafe impl Sync for NvmeIoCtx {}

// Size of the memory pool for NVMe I/O structures.
const IOCTX_POOL_SIZE: u64 = 64 * 1024 - 1;

// Memory pool for NVMe controller - specific I/O context, which is used
// in every user BIO-based I/O operation.
static IOCTX_POOL: OnceCell<MemoryPool<NvmeIoCtx>> = OnceCell::new();

// Maximum number of range sets that may be specified in the dataset management
// command.
const SPDK_NVME_DATASET_MANAGEMENT_MAX_RANGES: u64 = 256;

// Maximum number of blocks that may be specified in a single dataset management
// range.
const SPDK_NVME_DATASET_MANAGEMENT_RANGE_MAX_BLOCKS: u64 = 0xFFFFFFFF;

/*
 * I/O handle for NVMe block device.
 */
pub struct NvmeDeviceHandle {
    io_channel: ManuallyDrop<NvmeControllerIoChannel>,
    ctrlr: NonNull<spdk_nvme_ctrlr>,
    name: String,
    ns: Arc<NvmeNamespace>,
    prchk_flags: u32,

    // Private instance of the block device backed by the NVMe namespace.
    block_device: Box<dyn BlockDevice>,

    // Static values cached for performance.
    num_blocks: u64,
    block_len: u64,
}
/// Context for reset operation.
struct ResetCtx {
    cb: IoCompletionCallback,
    cb_arg: IoCompletionCallbackArg,
    device: Box<dyn BlockDevice>,
}

impl NvmeDeviceHandle {
    pub fn create(
        name: &str,
        id: u64,
        ctrlr: NonNull<spdk_nvme_ctrlr>,
        ns: Arc<NvmeNamespace>,
        prchk_flags: u32,
    ) -> Result<NvmeDeviceHandle, CoreError> {
        // Initialize memory pool for holding I/O context now, during the slow
        // path, to make sure it's available before the first I/O
        // oepration takes place.
        IOCTX_POOL.get_or_init(|| MemoryPool::<NvmeIoCtx>::create(
            "nvme_ctrl_io_ctx",
            IOCTX_POOL_SIZE
        ).expect("Failed to create memory pool for NVMe controller I/O contexts"));

        // Obtain SPDK I/O channel for NVMe controller.
        let io_channel = NvmeControllerIoChannel::from_null_checked(unsafe {
            spdk_get_io_channel(id as *mut c_void)
        })
        .ok_or(CoreError::GetIoChannel {
            name: name.to_string(),
        })?;

        Ok(NvmeDeviceHandle {
            name: name.to_string(),
            io_channel: ManuallyDrop::new(io_channel),
            ctrlr,
            block_device: Self::get_nvme_device(name, &ns),
            num_blocks: ns.num_blocks(),
            block_len: ns.block_len(),
            prchk_flags,
            ns,
        })
    }

    fn get_nvme_device(
        name: &str,
        ns: &Arc<NvmeNamespace>,
    ) -> Box<dyn BlockDevice> {
        Box::new(NvmeBlockDevice::from_ns(name, Arc::clone(ns)))
    }

    #[inline]
    fn bytes_to_blocks(
        &self,
        offset_bytes: u64,
        num_bytes: u64,
    ) -> (bool, u64, u64) {
        let offset_blocks = offset_bytes / self.block_len;
        let num_blocks = num_bytes / self.block_len;
        let alignment =
            (offset_bytes % self.block_len) | (num_bytes % self.block_len);

        // TODO: Optimize for ^2.
        (alignment == 0, offset_blocks, num_blocks)
    }
}

extern "C" fn nvme_admin_passthru_done(
    ctx: *mut c_void,
    cpl: *const spdk_nvme_cpl,
) {
    debug!(
        "Admin passthrough completed, succeeded={}",
        nvme_cpl_succeeded(cpl)
    );
    done_cb(ctx, nvme_cpl_succeeded(cpl));
}

extern "C" fn nvme_queued_reset_sgl(ctx: *mut c_void, sgl_offset: u32) {
    let nvme_io_ctx = unsafe { &mut *(ctx as *mut NvmeIoCtx) };

    nvme_io_ctx.iov_offset = sgl_offset as u64;
    nvme_io_ctx.iovpos = 0;

    while nvme_io_ctx.iovpos < nvme_io_ctx.iovcnt {
        unsafe {
            let iov = nvme_io_ctx.iov.add(nvme_io_ctx.iovpos as usize);
            if nvme_io_ctx.iov_offset < (*iov).iov_len {
                break;
            }

            nvme_io_ctx.iov_offset -= (*iov).iov_len;
        }

        nvme_io_ctx.iovpos += 1;
    }
}

extern "C" fn nvme_queued_next_sge(
    ctx: *mut c_void,
    address: *mut *mut c_void,
    length: *mut u32,
) -> i32 {
    let nvme_io_ctx = unsafe { &mut *(ctx as *mut NvmeIoCtx) };

    assert!(nvme_io_ctx.iovpos < nvme_io_ctx.iovcnt);

    unsafe {
        let iov = nvme_io_ctx.iov.add(nvme_io_ctx.iovpos as usize);

        let mut a = (*iov).iov_base as u64;
        *length = (*iov).iov_len as u32;

        if nvme_io_ctx.iov_offset > 0 {
            assert!(nvme_io_ctx.iov_offset <= (*iov).iov_len);
            a += nvme_io_ctx.iov_offset;
            *length -= nvme_io_ctx.iov_offset as u32;
        }

        nvme_io_ctx.iov_offset += *length as u64;
        if nvme_io_ctx.iov_offset == (*iov).iov_len {
            nvme_io_ctx.iovpos += 1;
            nvme_io_ctx.iov_offset = 0;
        }

        *(address as *mut u64) = a;
    }

    0
}

/// Notify the caller and deallocate Nvme IO context.
#[inline]
fn complete_nvme_command(ctx: *mut NvmeIoCtx, cpl: *const spdk_nvme_cpl) {
    let io_ctx = unsafe { &mut *ctx };
    let op_succeeded = nvme_cpl_succeeded(cpl);
    let inner = NvmeIoChannel::inner_from_channel(io_ctx.channel);

    // Update I/O statistics in case the operation succeeded.
    if op_succeeded {
        let stats_controller = inner.get_io_stats_controller();
        stats_controller.account_block_io(io_ctx.op, 1, io_ctx.num_blocks);
    }

    // Adjust the number of active I/O.
    inner.discard_io();

    // Invoke caller's callback and free I/O context.
    if op_succeeded {
        (io_ctx.cb)(&inner.device, IoCompletionStatus::Success, io_ctx.cb_arg);
    } else {
        (io_ctx.cb)(
            &inner.device,
            IoCompletionStatus::NvmeError(nvme_command_status(cpl)),
            io_ctx.cb_arg,
        );
    }
    IOCTX_POOL.get().unwrap().put(ctx);
}

/// Completion handler for vectored write requests.
extern "C" fn nvme_writev_done(ctx: *mut c_void, cpl: *const spdk_nvme_cpl) {
    let nvme_io_ctx = ctx as *mut NvmeIoCtx;

    debug!("NVMe writev I/O completed !");

    // Check if operation successfully completed.
    if nvme_cpl_is_pi_error(cpl) {
        error!("writev completed with PI error");
    }

    complete_nvme_command(nvme_io_ctx, cpl);
}

/// I/O completion handler for all read requests (vectored/non-vectored)
/// and non-vectored write requests.
extern "C" fn nvme_io_done(ctx: *mut c_void, cpl: *const spdk_nvme_cpl) {
    let nvme_io_ctx = ctx as *mut NvmeIoCtx;

    // Check if operation successfully completed.
    if nvme_cpl_is_pi_error(cpl) {
        error!("readv completed with PI error");
    }

    complete_nvme_command(nvme_io_ctx, cpl);
}

extern "C" fn nvme_async_io_completion(
    ctx: *mut c_void,
    cpl: *const spdk_nvme_cpl,
) {
    debug!("Async NVMe I/O completed !");
    done_cb(ctx, nvme_cpl_succeeded(cpl));
}

extern "C" fn nvme_unmap_completion(
    ctx: *mut c_void,
    cpl: *const spdk_nvme_cpl,
) {
    let nvme_io_ctx = ctx as *mut NvmeIoCtx;
    debug!("Async unmap completed");
    complete_nvme_command(nvme_io_ctx, cpl);
}

fn check_io_args(
    op: IoType,
    iov: *mut iovec,
    iovcnt: i32,
    offset_blocks: u64,
    num_blocks: u64,
) -> Result<(), CoreError> {
    // Make sure I/O structures look sane.
    // As of now, we assume that I/O vector is fully prepared by the caller.
    if iovcnt <= 0 {
        error!("insufficient number of elements in I/O vector: {}", iovcnt);
        return Err(io_type_to_err(
            op,
            libc::EINVAL,
            offset_blocks,
            num_blocks,
        ));
    }
    unsafe {
        if (*iov).iov_base.is_null() {
            error!("I/O vector is not initialized");
            return Err(io_type_to_err(
                op,
                libc::EINVAL,
                offset_blocks,
                num_blocks,
            ));
        }
    }
    Ok(())
}

fn io_type_to_err(
    op: IoType,
    errno: i32,
    offset_blocks: u64,
    num_blocks: u64,
) -> CoreError {
    assert!(errno > 0, "Errno code must be provided");
    let source = Errno::from_i32(errno);

    match op {
        IoType::Read => CoreError::ReadDispatch {
            source,
            offset: offset_blocks,
            len: num_blocks,
        },
        IoType::Write => CoreError::WriteDispatch {
            source,
            offset: offset_blocks,
            len: num_blocks,
        },
        IoType::Unmap => CoreError::UnmapDispatch {
            source,
            offset: offset_blocks,
            len: num_blocks,
        },
        _ => {
            warn!("Unsupported I/O operation: {:?}", op);
            CoreError::NotSupported {
                source,
            }
        }
    }
}

fn alloc_nvme_io_ctx(
    op: IoType,
    ctx: NvmeIoCtx,
    offset_blocks: u64,
    num_blocks: u64,
) -> Result<*mut NvmeIoCtx, CoreError> {
    let pool = IOCTX_POOL.get().unwrap();

    if let Some(c) = pool.get(ctx) {
        Ok(c)
    } else {
        Err(io_type_to_err(op, libc::ENOMEM, offset_blocks, num_blocks))
    }
}

/// Check whether channel is suitable for serving I/O.
fn check_channel_for_io(
    op: IoType,
    inner: &NvmeIoChannelInner,
    offset_blocks: u64,
    num_blocks: u64,
) -> Result<(), CoreError> {
    let mut errno = 0;

    // Check against concurrent controller reset, which results in valid
    // I/O channel but deactivated I/O pair.
    if inner.qpair.is_none() {
        errno = libc::ENODEV;
    }

    if errno == 0 {
        Ok(())
    } else {
        Err(io_type_to_err(op, errno, offset_blocks, num_blocks))
    }
}

/// Handler for controller reset operation.
/// Serves as a proxy layer between NVMe controller and block device layer
/// (represented by device I/O handle): we need to pass block device
/// reference to user callback for handle-based operation.
fn reset_callback(success: bool, arg: *mut c_void) {
    let ctx = unsafe { Box::from_raw(arg as *mut ResetCtx) };

    // Translate success/failure into NVMe errors.
    let status = if success {
        IoCompletionStatus::Success
    } else {
        IoCompletionStatus::NvmeError(NvmeCommandStatus::GenericCommandStatus(
            GenericStatusCode::InternalDeviceError,
        ))
    };

    (ctx.cb)(&ctx.device, status, ctx.cb_arg);
}

#[async_trait(?Send)]
impl BlockDeviceHandle for NvmeDeviceHandle {
    fn get_device(&self) -> &Box<dyn BlockDevice> {
        &self.block_device
    }

    fn dma_malloc(&self, size: u64) -> Result<DmaBuf, DmaError> {
        DmaBuf::new(size, self.ns.alignment())
    }

    async fn nvme_identify_ctrlr(&self) -> Result<DmaBuf, CoreError> {
        let mut buf = DmaBuf::new(4096, 8).map_err(|_e| {
            CoreError::DmaAllocationError {
                size: 4096,
            }
        })?;

        let mut cmd = spdk_sys::spdk_nvme_cmd::default();
        cmd.set_opc(nvme_admin_opc::IDENTIFY.into());
        cmd.nsid = 0xffffffff;
        // Controller Identifier
        unsafe { *spdk_sys::nvme_cmd_cdw10_get(&mut cmd) = 1 };
        self.nvme_admin(&cmd, Some(&mut buf)).await?;
        Ok(buf)
    }

    async fn read_at(
        &self,
        offset: u64,
        buffer: &mut DmaBuf,
    ) -> Result<u64, CoreError> {
        let (valid, offset_blocks, num_blocks) =
            self.bytes_to_blocks(offset, buffer.len());

        debug!(
            "{} read(offset={}, size={})",
            self.name,
            offset,
            buffer.len()
        );
        // Make sure offset/size matches device block size.
        if !valid {
            error!(
                "{} invalid offset/buffer size: (offset={}, size={})",
                self.name,
                offset,
                buffer.len()
            );
            return Err(CoreError::InvalidOffset {
                offset,
            });
        }

        let inner = NvmeIoChannel::inner_from_channel(self.io_channel.as_ptr());

        // Make sure channel allows I/O.
        check_channel_for_io(IoType::Read, inner, offset_blocks, num_blocks)?;

        let (s, r) = oneshot::channel::<bool>();

        let rc = unsafe {
            spdk_nvme_ns_cmd_read(
                self.ns.as_ptr(),
                inner.qpair.as_mut().unwrap().as_ptr(),
                **buffer,
                offset_blocks,
                num_blocks as u32,
                Some(nvme_async_io_completion),
                cb_arg(s),
                self.prchk_flags,
            )
        };

        if rc != 0 && rc != -libc::ENOMEM {
            error!("{} read failed: rc = {}", self.name, rc);
            return Err(CoreError::ReadFailed {
                offset,
                len: buffer.len(),
            });
        }

        inner.account_io();
        let ret = if r.await.expect("Failed awaiting at read_at()") {
            inner.get_io_stats_controller().account_block_io(
                IoType::Read,
                1,
                num_blocks,
            );
            Ok(buffer.len())
        } else {
            Err(CoreError::ReadFailed {
                offset,
                len: buffer.len(),
            })
        };
        inner.discard_io();
        ret
    }

    async fn write_at(
        &self,
        offset: u64,
        buffer: &DmaBuf,
    ) -> Result<u64, CoreError> {
        let (valid, offset_blocks, num_blocks) =
            self.bytes_to_blocks(offset, buffer.len());

        debug!(
            "{} write(offset={}, size={})",
            self.name,
            offset,
            buffer.len()
        );
        // Make sure offset/size matches device block size.
        if !valid {
            error!(
                "{} invalid offset/buffer size: (offset={}, size={})",
                self.name,
                offset,
                buffer.len()
            );
            return Err(CoreError::InvalidOffset {
                offset,
            });
        }

        let inner = NvmeIoChannel::inner_from_channel(self.io_channel.as_ptr());

        // Make sure channel allows I/O.
        check_channel_for_io(IoType::Write, inner, offset_blocks, num_blocks)?;

        let (s, r) = oneshot::channel::<bool>();

        let rc = unsafe {
            spdk_nvme_ns_cmd_write(
                self.ns.as_ptr(),
                inner.qpair.as_mut().unwrap().as_ptr(),
                **buffer,
                offset_blocks,
                num_blocks as u32,
                Some(nvme_async_io_completion),
                cb_arg(s),
                self.prchk_flags,
            )
        };

        if rc != 0 && rc != -libc::ENOMEM {
            error!("{} write failed: rc = {}", self.name, rc);
            return Err(CoreError::WriteFailed {
                offset,
                len: buffer.len(),
            });
        }

        inner.account_io();
        let ret = if r.await.expect("Failed awaiting at write_at()") {
            inner.get_io_stats_controller().account_block_io(
                IoType::Write,
                1,
                num_blocks,
            );
            Ok(buffer.len())
        } else {
            Err(CoreError::WriteFailed {
                offset,
                len: buffer.len(),
            })
        };
        inner.discard_io();
        ret
    }

    // bdev_nvme_get_buf_cb
    fn readv_blocks(
        &self,
        iov: *mut iovec,
        iovcnt: i32,
        offset_blocks: u64,
        num_blocks: u64,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError> {
        check_io_args(IoType::Read, iov, iovcnt, offset_blocks, num_blocks)?;

        let channel = self.io_channel.as_ptr();
        let inner = NvmeIoChannel::inner_from_channel(channel);

        // Make sure channel allows I/O.
        check_channel_for_io(IoType::Read, inner, offset_blocks, num_blocks)?;

        let bio = alloc_nvme_io_ctx(
            IoType::Read,
            NvmeIoCtx {
                cb,
                cb_arg,
                iov,
                iovcnt: iovcnt as u64,
                iovpos: 0,
                iov_offset: 0,
                channel: channel,
                op: IoType::Read,
                num_blocks,
            },
            offset_blocks,
            num_blocks,
        )?;

        let rc;

        if iovcnt == 1 {
            rc = unsafe {
                spdk_nvme_ns_cmd_read(
                    self.ns.as_ptr(),
                    inner.qpair.as_mut().unwrap().as_ptr(),
                    (*iov).iov_base,
                    offset_blocks,
                    num_blocks as u32,
                    Some(nvme_io_done),
                    bio as *mut c_void,
                    self.prchk_flags,
                )
            };
        } else {
            rc = unsafe {
                spdk_nvme_ns_cmd_readv(
                    self.ns.as_ptr(),
                    inner.qpair.as_mut().unwrap().as_ptr(),
                    offset_blocks,
                    num_blocks as u32,
                    Some(nvme_io_done),
                    bio as *mut c_void,
                    self.prchk_flags,
                    Some(nvme_queued_reset_sgl),
                    Some(nvme_queued_next_sge),
                )
            }
        }

        if rc < 0 {
            Err(CoreError::ReadDispatch {
                source: Errno::from_i32(-rc),
                offset: offset_blocks,
                len: num_blocks,
            })
        } else {
            inner.account_io();
            Ok(())
        }
    }

    fn writev_blocks(
        &self,
        iov: *mut iovec,
        iovcnt: i32,
        offset_blocks: u64,
        num_blocks: u64,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError> {
        check_io_args(IoType::Write, iov, iovcnt, offset_blocks, num_blocks)?;

        let channel = self.io_channel.as_ptr();
        let inner = NvmeIoChannel::inner_from_channel(channel);

        // Make sure channel allows I/O.
        check_channel_for_io(IoType::Write, inner, offset_blocks, num_blocks)?;

        let bio = alloc_nvme_io_ctx(
            IoType::Write,
            NvmeIoCtx {
                cb,
                cb_arg,
                iov,
                iovcnt: iovcnt as u64,
                iovpos: 0,
                iov_offset: 0,
                channel: channel,
                op: IoType::Write,
                num_blocks,
            },
            offset_blocks,
            num_blocks,
        )?;

        let rc;

        if iovcnt == 1 {
            rc = unsafe {
                spdk_nvme_ns_cmd_write(
                    self.ns.as_ptr(),
                    inner.qpair.as_mut().unwrap().as_ptr(),
                    (*iov).iov_base,
                    offset_blocks,
                    num_blocks as u32,
                    Some(nvme_io_done),
                    bio as *mut c_void,
                    self.prchk_flags,
                )
            };
        } else {
            rc = unsafe {
                spdk_nvme_ns_cmd_writev(
                    self.ns.as_ptr(),
                    inner.qpair.as_mut().unwrap().as_ptr(),
                    offset_blocks,
                    num_blocks as u32,
                    Some(nvme_writev_done),
                    bio as *mut c_void,
                    self.prchk_flags,
                    Some(nvme_queued_reset_sgl),
                    Some(nvme_queued_next_sge),
                )
            }
        }

        if rc < 0 {
            Err(CoreError::WriteDispatch {
                source: Errno::from_i32(-rc),
                offset: offset_blocks,
                len: num_blocks,
            })
        } else {
            inner.account_io();
            Ok(())
        }
    }

    async fn nvme_admin_custom(&self, opcode: u8) -> Result<(), CoreError> {
        let mut cmd = spdk_sys::spdk_nvme_cmd::default();
        cmd.set_opc(opcode.into());
        self.nvme_admin(&cmd, None).await
    }

    async fn nvme_admin(
        &self,
        cmd: &spdk_sys::spdk_nvme_cmd,
        buffer: Option<&mut DmaBuf>,
    ) -> Result<(), CoreError> {
        let mut pcmd = *cmd; // Make a private mutable copy of the command.

        let inner = NvmeIoChannel::inner_from_channel(self.io_channel.as_ptr());

        // Make sure channel allows I/O.
        if inner.qpair.is_none() {
            return Err(CoreError::NvmeAdminDispatch {
                source: Errno::ENODEV,
                opcode: cmd.opc(),
            });
        }

        let (ptr, size) = match buffer {
            Some(buf) => (**buf, buf.len()),
            None => (std::ptr::null_mut(), 0),
        };

        let (s, r) = oneshot::channel::<bool>();

        let _rc = unsafe {
            spdk_nvme_ctrlr_cmd_admin_raw(
                self.ctrlr.as_ptr(),
                &mut pcmd,
                ptr,
                size as u32,
                Some(nvme_admin_passthru_done),
                cb_arg(s),
            )
        };

        inner.account_io();
        let ret = if r.await.expect("Failed awaiting NVMe Admin command I/O") {
            debug!("nvme_admin() done");
            Ok(())
        } else {
            Err(CoreError::NvmeAdminFailed {
                opcode: (*cmd).opc(),
            })
        };
        inner.discard_io();
        ret
    }

    fn reset(
        &self,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError> {
        let controller = NVME_CONTROLLERS.lookup_by_name(&self.name).ok_or(
            CoreError::BdevNotFound {
                name: self.name.to_string(),
            },
        )?;
        let mut controller = controller.lock().expect("lock poisoned");

        let ctx = Box::new(ResetCtx {
            cb,
            cb_arg,
            device: Self::get_nvme_device(&self.name, &self.ns),
        });

        // Schedule asynchronous controller reset.
        controller.reset(
            reset_callback,
            Box::into_raw(ctx) as *mut c_void,
            false,
        )
    }

    fn unmap_blocks(
        &self,
        offset_blocks: u64,
        num_blocks: u64,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError> {
        let num_ranges =
            (num_blocks + SPDK_NVME_DATASET_MANAGEMENT_RANGE_MAX_BLOCKS - 1)
                / SPDK_NVME_DATASET_MANAGEMENT_RANGE_MAX_BLOCKS;

        if num_ranges > SPDK_NVME_DATASET_MANAGEMENT_MAX_RANGES {
            return Err(CoreError::UnmapDispatch {
                source: Errno::EINVAL,
                offset: offset_blocks,
                len: num_blocks,
            });
        }

        let channel = self.io_channel.as_ptr();
        let inner = NvmeIoChannel::inner_from_channel(channel);

        // Make sure channel allows I/O.
        check_channel_for_io(IoType::Unmap, inner, offset_blocks, num_blocks)?;

        let bio = alloc_nvme_io_ctx(
            IoType::Unmap,
            NvmeIoCtx {
                cb,
                cb_arg,
                iov: std::ptr::null_mut() as *mut iovec, // No I/O vec involved.
                iovcnt: 0,
                iovpos: 0,
                iov_offset: 0,
                channel: channel,
                op: IoType::Unmap,
                num_blocks,
            },
            offset_blocks,
            num_blocks,
        )?;

        let l = Layout::array::<spdk_nvme_dsm_range>(
            SPDK_NVME_DATASET_MANAGEMENT_MAX_RANGES as usize,
        )
        .unwrap();
        let dsm_ranges =
            unsafe { std::alloc::alloc(l) as *mut spdk_nvme_dsm_range };

        let mut remaining = num_blocks;
        let mut offset = offset_blocks;
        let mut range_id: usize = 0;

        // Fill max-size ranges until the remaining blocks fit into one range.
        while remaining > SPDK_NVME_DATASET_MANAGEMENT_RANGE_MAX_BLOCKS {
            unsafe {
                let mut range = spdk_nvme_dsm_range::default();

                range.attributes.raw = 0;
                range.length =
                    SPDK_NVME_DATASET_MANAGEMENT_RANGE_MAX_BLOCKS as u32;
                range.starting_lba = offset;

                *dsm_ranges.add(range_id) = range;
            }

            offset += SPDK_NVME_DATASET_MANAGEMENT_RANGE_MAX_BLOCKS;
            remaining -= SPDK_NVME_DATASET_MANAGEMENT_RANGE_MAX_BLOCKS;
            range_id += 1;
        }

        // Setup range that describes the remaining blocks and schedule unmap.
        let rc = unsafe {
            let mut range = spdk_nvme_dsm_range::default();

            range.attributes.raw = 0;
            range.length = remaining as u32;
            range.starting_lba = offset;

            *dsm_ranges.add(range_id) = range;

            spdk_nvme_ns_cmd_dataset_management(
                self.ns.as_ptr(),
                inner.qpair.as_mut().unwrap().as_ptr(),
                utils::NvmeDsmAttribute::Deallocate as u32,
                dsm_ranges,
                num_ranges as u16,
                Some(nvme_unmap_completion),
                bio as *mut c_void,
            )
        };

        if rc < 0 {
            Err(CoreError::UnmapDispatch {
                source: Errno::from_i32(-rc),
                offset: offset_blocks,
                len: num_blocks,
            })
        } else {
            inner.account_io();
            Ok(())
        }
    }

    fn write_zeroes(
        &self,
        offset_blocks: u64,
        num_blocks: u64,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError> {
        // Write zeroes are done through unmap.
        self.unmap_blocks(offset_blocks, num_blocks, cb, cb_arg)
    }
}

impl Drop for NvmeDeviceHandle {
    fn drop(&mut self) {
        unsafe { ManuallyDrop::drop(&mut self.io_channel) }
    }
}
