use spdk_sys::{self, spdk_nvme_cpl};

#[derive(Debug, PartialEq)]
enum NvmeStatusCodeType {
    Generic = 0x0,
    MediaError = 0x2,
}
#[derive(Debug, PartialEq)]
enum NvmeMediaErrorStatusCode {
    Guard = 0x82,
    ApplicationTag = 0x83,
    ReferenceTag = 0x84,
}
#[derive(Debug, PartialEq)]
enum NvmeGenericCommandStatusCode {
    Success = 0x0,
}

/// Check if the Completion Queue Entry indicates abnormal termination of
/// request due to any of the following conditions:
///   - Any media specific errors that occur in the NVM or data integrity type
///     errors.
///   - The command was aborted due to an end-to-end guard check failure.
///   - The command was aborted due to an end-to-end application tag check
///     failure.
///   - The command was aborted due to an end-to-end reference tag check
///     failure.
#[inline]
pub(crate) fn nvme_cpl_is_pi_error(cpl: *const spdk_nvme_cpl) -> bool {
    let sct;
    let sc;

    unsafe {
        let cplr = &(*cpl);
        sct = cplr.__bindgen_anon_1.status.sct();
        sc = cplr.__bindgen_anon_1.status.sc();
    }

    sct == NvmeStatusCodeType::MediaError as u16
        || sc == NvmeMediaErrorStatusCode::Guard as u16
        || sc == NvmeMediaErrorStatusCode::ApplicationTag as u16
        || sc == NvmeMediaErrorStatusCode::ReferenceTag as u16
}

#[inline]
/// Check if NVMe controller command completed successfully.
pub(crate) fn nvme_cpl_succeeded(cpl: *const spdk_nvme_cpl) -> bool {
    let sct;
    let sc;

    unsafe {
        let cplr = &(*cpl);
        sct = cplr.__bindgen_anon_1.status.sct();
        sc = cplr.__bindgen_anon_1.status.sc();
    }

    sct == NvmeStatusCodeType::Generic as u16
        && sc == NvmeGenericCommandStatusCode::Success as u16
}
