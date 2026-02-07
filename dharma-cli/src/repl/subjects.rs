use crate::error::DharmaError;
use crate::store::state::{read_manifest, ManifestEntry};
use crate::types::SubjectId;
use std::collections::HashSet;
use std::path::Path;

pub fn recent_subjects(root: &Path, limit: usize) -> Result<Vec<SubjectId>, DharmaError> {
    let env = dharma::env::StdEnv::new(root);
    let entries = read_manifest(&env)?;
    Ok(recent_from_entries(&entries, limit))
}

fn recent_from_entries(entries: &[ManifestEntry], limit: usize) -> Vec<SubjectId> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for entry in entries.iter().rev() {
        let Some(subject) = entry.subject else {
            continue;
        };
        if seen.insert(subject) {
            out.push(subject);
            if out.len() >= limit {
                break;
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::EnvelopeId;

    #[test]
    fn recent_subjects_respects_order() {
        let subject_a = SubjectId::from_bytes([1u8; 32]);
        let subject_b = SubjectId::from_bytes([2u8; 32]);
        let entries = vec![
            ManifestEntry {
                envelope_id: EnvelopeId::from_bytes([10u8; 32]),
                subject: Some(subject_a),
            },
            ManifestEntry {
                envelope_id: EnvelopeId::from_bytes([11u8; 32]),
                subject: Some(subject_b),
            },
            ManifestEntry {
                envelope_id: EnvelopeId::from_bytes([12u8; 32]),
                subject: Some(subject_a),
            },
        ];
        let recent = recent_from_entries(&entries, 10);
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].as_bytes(), subject_a.as_bytes());
        assert_eq!(recent[1].as_bytes(), subject_b.as_bytes());
    }
}
