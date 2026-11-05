use crate::{ApiError, input::response_message};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::VecDeque,
    fs::File,
    io::{BufWriter, ErrorKind, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

const MAX_BYTES: usize = 8 * 1024 * 1024;
const MAX_RECORDS: usize = 64;

#[derive(Default)]
pub struct State {
    path: Option<PathBuf>,
    entries: Mutex<VecDeque<(Arc<Record>, usize)>>,
    writer: Mutex<()>,
}

#[derive(Deserialize, Serialize)]
pub struct Record {
    id: String,
    history: Option<Vec<Value>>,
    output: Vec<Value>,
    #[serde(default = "legacy_history")]
    history_includes_output: bool,
}

fn legacy_history() -> bool {
    true
}

impl Record {
    pub fn new(id: String, history: Option<Vec<Value>>, output: Vec<Value>) -> Self {
        Self {
            id,
            history,
            output,
            history_includes_output: false,
        }
    }

    fn replay(&self) -> Option<Vec<Value>> {
        let mut history = self.history.clone()?;
        if !self.history_includes_output {
            history.extend(
                self.output
                    .iter()
                    .filter_map(|item| response_message(item.clone()).ok().flatten()),
            );
        }
        Some(history)
    }
}

impl State {
    pub fn open(path: Option<PathBuf>) -> Result<Self> {
        let Some(path) = path else {
            return Ok(Self::default());
        };
        let parent = parent_dir(&path);
        std::fs::create_dir_all(parent)?;
        let mut entries = VecDeque::new();
        match File::open(&path) {
            Ok(file) => {
                let mut bytes = Vec::new();
                file.take(MAX_BYTES as u64 + 1).read_to_end(&mut bytes)?;
                ensure!(
                    bytes.len() <= MAX_BYTES,
                    "Responses state file exceeds 8 MiB"
                );
                let records: Vec<Record> =
                    serde_json::from_slice(&bytes).context("Invalid Responses state file")?;
                ensure!(
                    records.len() <= MAX_RECORDS,
                    "Too many records in Responses state file"
                );
                for record in records {
                    let size = serde_json::to_vec(&record)?.len() + 1;
                    entries.push_back((Arc::new(record), size));
                }
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        Ok(Self {
            path: Some(path),
            entries: Mutex::new(entries),
            writer: Mutex::new(()),
        })
    }

    pub fn remember(&self, mut record: Record) -> Result<()> {
        let mut size = serde_json::to_vec(&record)?.len() + 1;
        if size + 2 > MAX_BYTES {
            record.history = None;
            size = serde_json::to_vec(&record)?.len() + 1;
            if size + 2 > MAX_BYTES {
                return Ok(());
            }
        }
        // Serialize commits while letting lookups read the last committed snapshot.
        let _writer = self.writer.lock().unwrap();
        let mut entries = self.entries.lock().unwrap().clone();
        let mut total = 2 + entries.iter().map(|(_, size)| size).sum::<usize>();
        while entries.len() >= MAX_RECORDS || total + size > MAX_BYTES {
            total -= entries.pop_front().unwrap().1;
        }
        entries.push_back((Arc::new(record), size));
        if let Some(path) = &self.path {
            save(path, entries.iter().map(|(record, _)| record.as_ref()))?;
        }
        *self.entries.lock().unwrap() = entries;
        Ok(())
    }

    pub fn resolve(&self, item: Value, remaining: &mut usize) -> Result<Value, ApiError> {
        if item["type"] != "item_reference" {
            return Ok(item);
        }
        let id = item["id"]
            .as_str()
            .ok_or_else(|| ApiError::bad_request("Missing Responses item reference ID"))?;
        let entries = self.entries.lock().unwrap();
        let item = entries
            .iter()
            .rev()
            .flat_map(|(record, _)| &record.output)
            .find(|item| item["id"] == id)
            .ok_or_else(|| {
                ApiError::missing_reference(
                    "Responses item reference expired; resend full input history",
                    "item_reference_not_found",
                )
            })?;
        let size = item.to_string().len();
        *remaining = remaining.checked_sub(size).ok_or_else(|| {
            ApiError::too_large("Expanded Responses item references exceed 8 MiB")
        })?;
        Ok(item.clone())
    }

    pub fn history(&self, id: &str) -> Result<Vec<Value>, ApiError> {
        self.replay(|record| record.id == id).ok_or_else(|| {
            ApiError::missing_reference(
                "Previous response not found; resend full input history",
                "previous_response_not_found",
            )
        })
    }

    pub fn tool_parent_history(&self, call_id: &str) -> Option<Vec<Value>> {
        self.replay(|record| {
            record
                .output
                .iter()
                .any(|item| item["type"] == "function_call" && item["call_id"] == call_id)
        })
    }

    fn replay(&self, matches: impl Fn(&Record) -> bool) -> Option<Vec<Value>> {
        let record = self
            .entries
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find(|(record, _)| matches(record))
            .map(|(record, _)| Arc::clone(record))?;
        record.replay()
    }
}

fn parent_dir(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn save<'a>(path: &Path, entries: impl Iterator<Item = &'a Record>) -> Result<()> {
    let mut file = tempfile::NamedTempFile::new_in(parent_dir(path))?;
    {
        let mut writer = BufWriter::new(file.as_file_mut());
        writer.write_all(b"[")?;
        for (index, record) in entries.enumerate() {
            if index != 0 {
                writer.write_all(b",")?;
            }
            serde_json::to_writer(&mut writer, record)?;
        }
        writer.write_all(b"]")?;
        writer.flush()?;
    }
    file.as_file().sync_all()?;
    file.persist(path)?;
    Ok(())
}
