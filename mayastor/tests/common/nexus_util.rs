use mayastor::bdev::{nexus_lookup, NexusStatus};

// verify the state of a nexus
pub fn check_nexus_state_is(nexus_name: &str, expected_status: NexusStatus) {
    let nexus = nexus_lookup(nexus_name).unwrap();
    assert_eq!(nexus.status(), expected_status);
}

// delete the named nexus and verify it is gone
pub async fn delete_nexus(nexus_name: &str) {
    let n = nexus_lookup(nexus_name).unwrap();
    n.destroy().await.unwrap();
    assert!(nexus_lookup(nexus_name).is_none());
}
