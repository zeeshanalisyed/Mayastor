//!
//! This file implements operations to the child bdevs from the context of its
//! parent.
//!
//! `register_children` and `register_child` are should only be used when
//! building up a new nexus
//!
//! `offline_child` and `online_child` should be used to include the child into
//! the IO path of the nexus currently, online of a child will default the nexus
//! into the degraded mode as it (may) require a rebuild. This will be changed
//! in the near future -- online child will not determine if it SHOULD online
//! but simply does what its told. Therefore, the callee must be careful when
//! using this method.
//!
//! 'fault_child` will do the same as `offline_child` except, it will not close
//! the child.
//!
//! `add_child` will construct a new `NexusChild` and add the bdev given by the
//! uri to the nexus. The nexus will transition to degraded mode as the new
//! child requires rebuild first. If the rebuild flag is set then the rebuild
//! is also started otherwise it has to be started through `start_rebuild`.
//!
//! When reconfiguring the nexus, we traverse all our children, create new IO
//! channels for all children that are in the open state.

use futures::future::join_all;
use snafu::ResultExt;

use crate::{
    bdev::{
        nexus::{
            nexus_bdev::{
                CreateChild,
                Error,
                Nexus,
                NexusState,
                NexusStatus,
                OpenChild,
            },
            nexus_channel::DrEvent,
            nexus_child::{ChildState, NexusChild},
            nexus_child_status_config::ChildStatusConfig,
        },
        Reason,
        VerboseError,
    },
    core::Bdev,
    nexus_uri::{bdev_create, bdev_destroy, NexusBdevError},
};
use std::sync::Arc;
use tokio::sync::Mutex;

impl Nexus {
    /// register children with the nexus, only allowed during the nexus init
    /// phase
    pub fn register_children(&mut self, dev_name: &[String]) {
        assert_eq!(*self.state.lock().unwrap(), NexusState::Init);
        self.child_count = dev_name.len() as u32;
        dev_name
            .iter()
            .map(|c| {
                debug!("{}: Adding child {}", self.name, c);
                self.children.insert(self.name.clone(), Arc::new(Mutex::new(NexusChild::new(
                    c.clone(),
                    self.name.clone(),
                    Bdev::lookup_by_name(c),
                ))))
            })
            .for_each(drop);
    }

    /// Create and register a single child to nexus, only allowed during the
    /// nexus init phase
    pub async fn create_and_register(
        &mut self,
        uri: &str,
    ) -> Result<(), NexusBdevError> {
        assert_eq!(*self.state.lock().unwrap(), NexusState::Init);
        let name = bdev_create(&uri).await?;
        self.children.insert(self.name.clone(), Arc::new(Mutex::new(NexusChild::new(
            uri.to_string(),
            self.name.clone(),
            Bdev::lookup_by_name(&name),
        ))));

        self.child_count += 1;
        Ok(())
    }

    /// add a new child to an existing nexus. note that the child is added and
    /// opened but not taking part of any new IO's that are submitted to the
    /// nexus.
    ///
    /// The child may require a rebuild first, so the nexus will
    /// transition to degraded mode when the addition has been successful.
    /// The rebuild flag dictates wether we attempt to start the rebuild or not
    /// If the rebuild fails to start the child remains degraded until such
    /// time the rebuild is retried and complete
    pub async fn add_child(
        &mut self,
        uri: &str,
        norebuild: bool,
    ) -> Result<NexusStatus, Error> {
        let status = self.add_child_only(uri).await?;

        if !norebuild {
            if let Err(e) = self.start_rebuild(&uri).await {
                // todo: CAS-253 retry starting the rebuild again when ready
                error!(
                    "Child added but rebuild failed to start: {}",
                    e.verbose()
                );
                match self.get_child_by_name(uri) {
                    Ok(child_m) => {
                        let mut child = child_m.lock().await;
                        child.fault(Reason::RebuildFailed).await
                    },
                    Err(e) => error!(
                        "Failed to find newly added child {}, error: {}",
                        uri,
                        e.verbose()
                    ),
                };
            }
        }
        Ok(status)
    }

    /// The child may require a rebuild first, so the nexus will
    /// transition to degraded mode when the addition has been successful.
    async fn add_child_only(
        &mut self,
        uri: &str,
    ) -> Result<NexusStatus, Error> {
        let name = bdev_create(&uri).await.context(CreateChild {
            name: self.name.clone(),
        })?;

        let child_bdev = match Bdev::lookup_by_name(&name) {
            Some(child) => {
                if child.block_len() != self.bdev.block_len()
                    || self.min_num_blocks().await > child.num_blocks()
                {
                    if let Err(err) = bdev_destroy(uri).await {
                        error!(
                            "Failed to destroy child bdev with wrong geometry: {}",
                            err
                        );
                    }

                    return Err(Error::ChildGeometry {
                        child: name,
                        name: self.name.clone(),
                    });
                } else {
                    child
                }
            }
            None => {
                return Err(Error::ChildMissing {
                    child: name,
                    name: self.name.clone(),
                })
            }
        };

        let mut child = NexusChild::new(
            uri.to_owned(),
            self.name.clone(),
            Some(child_bdev),
        );
        match child.open(self.size) {
            Ok(name) => {
                // we have created the bdev, and created a nexusChild struct. To
                // make use of the device itself the
                // data and metadata must be validated. The child
                // will be added and marked as faulted, once the rebuild has
                // completed the device can transition to online
                info!("{}: child opened successfully {}", self.name, name);

                // it can never take part in the IO path
                // of the nexus until it's rebuilt from a healthy child.
                child.fault(Reason::OutOfSync).await;
                if ChildStatusConfig::add(&child).await.is_err() {
                    error!("Failed to add child status information");
                }

                self.children.insert(child.name.clone(), Arc::new(Mutex::new(child)));
                self.child_count += 1;

                if let Err(e) = self.sync_labels().await {
                    error!("Failed to sync labels {:?}", e);
                    // todo: how to signal this?
                }

                Ok(self.status().await)
            }
            Err(e) => {
                if let Err(err) = bdev_destroy(uri).await {
                    error!(
                        "Failed to destroy child which failed to open: {}",
                        err
                    );
                }
                Err(e).context(OpenChild {
                    child: uri.to_owned(),
                    name: self.name.clone(),
                })
            }
        }
    }

    /// Destroy child with given uri.
    /// If the child does not exist the method returns success.
    pub async fn remove_child(&mut self, uri: &str) -> Result<(), Error> {
        if self.child_count == 1 {
            return Err(Error::DestroyLastChild {
                name: self.name.clone(),
                child: uri.to_owned(),
            });
        }

        let cancelled_rebuilding_children =
            self.cancel_child_rebuild_jobs(uri).await;

        match self.children.get(uri) {
            None => return Ok(()),
            Some(child_m) => {
                let mut child = child_m.lock().await;
                if let Err(e) = child.close().await {
                    return Err(Error::CloseChild {
                        name: self.name.clone(),
                        child: child.name.clone(),
                        source: e,
                    });
                }
            },
        };

        self.children.remove(uri);
        self.child_count -= 1;

        // Update child status to remove this child
        NexusChild::save_state_change();

        self.start_rebuild_jobs(cancelled_rebuilding_children).await;
        Ok(())
    }

    /// offline a child device and reconfigure the IO channels
    pub async fn offline_child(
        &mut self,
        name: &str,
    ) -> Result<NexusStatus, Error> {
        trace!("{}: Offline child request for {}", self.name, name);

        let cancelled_rebuilding_children =
            self.cancel_child_rebuild_jobs(name).await;

        if let Some(child_m) = self.children.get(name) {
            let mut child = child_m.lock().await;
            child.offline().await;
        } else {
            return Err(Error::ChildNotFound {
                name: self.name.clone(),
                child: name.to_owned(),
            });
        }

        self.reconfigure(DrEvent::ChildOffline).await;
        self.start_rebuild_jobs(cancelled_rebuilding_children).await;

        Ok(self.status().await)
    }

    /// fault a child device and reconfigure the IO channels
    pub async fn fault_child(
        &mut self,
        name: &str,
        reason: Reason,
    ) -> Result<(), Error> {
        trace!("{}: fault child request for {}", self.name, name);

        if self.child_count < 2 {
            return Err(Error::RemoveLastChild {
                name: self.name.clone(),
                child: name.to_owned(),
            });
        }

        let mut healthy_children = Vec::new();
        for (_name, child_m) in self.children.iter() {
            let child = child_m.lock().await;
            if child.state() == ChildState::Open {
                healthy_children.push(child);
            }
        }

        if healthy_children.len() == 1 && healthy_children[0].name == name {
            // the last healthy child cannot be faulted
            return Err(Error::FaultingLastHealthyChild {
                name: self.name.clone(),
                child: name.to_owned(),
            });
        }
        drop(healthy_children);

        let cancelled_rebuilding_children =
            self.cancel_child_rebuild_jobs(name).await;

        let result = match self.children.get(name) {
            Some(child_m) => {
                let mut child = child_m.lock().await;
                match child.state() {
                    ChildState::Faulted(_) => {}
                    _ => {
                        child.fault(reason).await;
                        NexusChild::save_state_change();
                        self.reconfigure(DrEvent::ChildFault).await;
                    }
                }
                Ok(())
            }
            None => Err(Error::ChildNotFound {
                name: self.name.clone(),
                child: name.to_owned(),
            }),
        };

        // start rebuilding the children that previously had their rebuild jobs
        // cancelled, in spite of whether or not the child was correctly faulted
        self.start_rebuild_jobs(cancelled_rebuilding_children).await;
        result
    }

    /// online a child and reconfigure the IO channels. The child is already
    /// registered, but simply not opened. This can be required in case where
    /// a child is misbehaving.
    pub async fn online_child(
        &mut self,
        name: &str,
    ) -> Result<NexusStatus, Error> {
        trace!("{} Online child request", self.name);

        if let Some(child_m) = self.children.get(name) {
            let mut child = child_m.lock().await;
            child.online(self.size).await.context(OpenChild {
                child: name.to_owned(),
                name: self.name.clone(),
            })?;
        } else {
            return Err(Error::ChildNotFound {
                name: self.name.clone(),
                child: name.to_owned(),
            })
        }
        self.start_rebuild(name).await.map(|_| {})?;
        Ok(self.status().await)
    }

    /// Close each child that belongs to this nexus.
    pub(crate) async fn close_children(&mut self) {
        for (_name, child_m) in self.children.iter_mut() {
            let mut child = child_m.lock().await;
            if let Err(error) = child.close().await {
                error!(?error, "{}: Failed to close children", self.name);
            }
        }
    }

    /// Add a child to the configuration when an example callback is run.
    /// The nexus is not opened implicitly, call .open() for this manually.
    pub async fn examine_child(&mut self, name: &str) -> bool {
        match self.children.get(name) {
            Some(child_m) => {
                if let Some(bdev) = Bdev::lookup_by_name(name) {
                    let mut child = child_m.lock().await;
                    child.bdev = Some(bdev);
                    return true;
                } else {
                    false
                }
            },
            None => false,
        }
    }

    /// try to open all the child devices
    pub(crate) async fn try_open_children(&mut self) -> Result<(), Error> {
        // Set the common block size.
        let mut blk_size = None;
        for (_key, child_m) in &self.children {
            let child = child_m.lock().await;
            if child.bdev.is_none() {
                return Err(Error::NexusIncomplete {
                    name: self.name.clone(),
                });
            }
            if let Some(blk_size) = blk_size {
                if child.bdev.as_ref().unwrap().block_len() != blk_size {
                    return Err(Error::MixedBlockSizes {
                        name: self.name.clone(),
                    });
                }
            } else {
                blk_size = Some(child.bdev.as_ref().unwrap().block_len());
            }
        }
        self.bdev.set_block_len(blk_size.unwrap());

        let size = self.size;

        let (mut open, mut error) = (Vec::new(), Vec::new());
        for (_key, child_m) in &self.children {
            let mut child = child_m.lock().await;
            match child.open(size) {
                Ok(c) => open.push(c),
                Err(e) => error.push(e),
            }
        }

        // depending on IO consistency policies, we might be able to go online
        // even if one of the children failed to open. This is work is not
        // completed yet so we fail the registration all together for now.

        if !error.is_empty() {
            for open_child in open {
                let name = open_child;
                if let Some(child) =
                    self.children.get(&name)
                {
                    if let Err(e) = child.lock().await.close().await {
                        error!(
                            "{}: child {} failed to close with error {}",
                            self.name,
                            name,
                            e.verbose()
                        );
                    }
                } else {
                    error!("{}: child {} failed to open", self.name, name);
                }
            }
            return Err(Error::NexusIncomplete {
                name: self.name.clone(),
            });
        }

        for (_key, child_m) in &self.children {
            let child = child_m.lock().await;
            let alignment = child.bdev.as_ref().unwrap().alignment();
            if self.bdev.alignment() < alignment {
                trace!(
                    "{}: child has alignment {}, updating required_alignment from {}",
                    self.name, alignment, self.bdev.alignment()
                );
                unsafe {
                    (*self.bdev.as_ptr()).required_alignment = alignment as u8;
                }
            }
        }

        Ok(())
    }

    /// The nexus is allowed to be smaller then the underlying child devices
    /// this function returns the smallest blockcnt of all online children as
    /// they MAY vary in size.
    pub(crate) async fn min_num_blocks(&self) -> u64 {
        let mut smallest_blkcnt = std::u64::MAX;
        for (_name, child_m) in self.children.iter() {
            let child = child_m.lock().await;
            let num_blocks = child.bdev.as_ref().unwrap().num_blocks();
            if smallest_blkcnt > num_blocks {
                smallest_blkcnt = num_blocks;
            }
        }
        smallest_blkcnt
    }

    /// lookup a child by its name
    pub fn child_lookup(&self, name: &str) -> Option<&Arc<Mutex<NexusChild>>> {
        self.children.get(name)
    }

    pub fn get_child_by_name(
        &mut self,
        name: &str,
    ) -> Result<&Arc<Mutex<NexusChild>>, Error> {
        match self.children.get(name) {
            Some(child) => Ok(child),
            None => Err(Error::ChildNotFound {
                child: name.to_owned(),
                name: self.name.clone(),
            }),
        }
    }
}
