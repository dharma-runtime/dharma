use crate::DharmaError;
use dharma::config::Config;
use dharma::store::Store;
use dharma::types::{ContractId, SchemaId, SubjectId};
use dharma::vault::{enqueue_archive_job, VaultArchiveJob};
pub(crate) fn maybe_archive_after_write(
    store: &Store,
    subject: SubjectId,
    ver: u64,
    schema_id: SchemaId,
    contract_id: ContractId,
) -> Result<(), DharmaError> {
    let root = match std::env::current_dir() {
        Ok(root) => root,
        Err(_) => return Ok(()),
    };
    let config = match Config::load(&root) {
        Ok(config) => config,
        Err(_) => return Ok(()),
    };
    if !config.vault.enabled {
        return Ok(());
    }
    let job = VaultArchiveJob {
        subject,
        ver,
        schema_id,
        contract_id,
    };
    if let Err(err) = enqueue_archive_job(store.root(), job) {
        eprintln!("[vault] queue failed: {err}");
    }
    Ok(())
}
