use crc::{Crc, CRC_32_ISCSI};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    convert::TryInto,
    fs::File,
    io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex, RwLock},
};
use uuid::Uuid;

use crate::{common::now, KvError, KvsEngine};

use super::sstable::SSTable;

struct Segment {}

/// KvStore stores all the data for the kvstore
#[derive(Clone)]
pub struct KvStore {
    directory: Pin<Box<PathBuf>>,
    sstable: SSTable,
    segments: Vec<Segment>,
}

impl KvStore {
    fn write(&self, record: Record) -> crate::Result<()> {
        let keydir = self.key_directory.read().unwrap();
        keydir.write(record)?;

        // when we have finished writing, we need to check if we've hit our file
        // limit. If we have, we need to retire the current active file and open
        // a new one.
        // if self.key_directory.read().unwrap().active_file_length() > 55000000 {
        if keydir.active_file_length() > 55000000 {
            self.immutable_files.push(keydir.active_path.clone());
            debug!("Active file is too large, writing it to disk");

            let new_file_name = format!("{}.log", Uuid::new_v4());
            let active_file_path = self.directory.clone().join(new_file_name);
            *self.active_path.write().unwrap() = active_file_path.clone();
            debug!("Created new active log: {:?}", active_file_path);

            drop(keydir);
            let mut keydir = self.key_directory.write().unwrap();
            keydir.set_active_file(active_file_path)?;

            // 3. Compact
            if self.immutable_files.len() > 10 {
                debug!("Too many files found. Compacting them together.");
                let mut immutable_files = self.immutable_files.clone();
                let key_directory = self.key_directory.clone();
                let directory = self.directory.clone();

                std::thread::spawn(move || {
                    // old paths
                    let old_immutable_data_file_paths = immutable_files.as_paths();
                    let old_immutable_hint_file_path = immutable_files.hint_path();

                    // create merge file
                    let name = Uuid::new_v4();
                    let merge_path = directory.clone().join(format!("{}.log", name));
                    let hint_path = directory.clone().join(format!("{}.hint", name));

                    // build merge and hint files
                    let keydir = immutable_files.build_state(&merge_path).unwrap();
                    keydir.save_key_dir(&hint_path).unwrap();

                    // update key directory with hint file
                    let hint_path_pointer = immutable_files.overwrite_hint_pointer(hint_path);
                    let data_path_pointer = key_directory
                        .write()
                        .unwrap()
                        .read_in_hint_file(hint_path_pointer.clone())
                        .unwrap();

                    // overwrite immutable_files to point to the merged file
                    immutable_files.overwrite_pointers(data_path_pointer);

                    // delete all old immutable_files
                    if let Some(old_hint_path) = old_immutable_hint_file_path {
                        debug!("Removed old hint file: {:?}", old_hint_path);
                        std::fs::remove_file(old_hint_path).unwrap();
                    }
                    for path in old_immutable_data_file_paths {
                        if path.exists() {
                            debug!("Removed old immutable file: {:?}", path);
                            std::fs::remove_file(path).unwrap();
                        }
                    }
                    debug!("Successfully compacted log");
                });
            }
        }

        Ok(())
    }
}

impl KvsEngine for KvStore {
    fn open(folder: impl Into<PathBuf>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let directory: PathBuf = folder.into();
        if !directory.exists() {
            debug!("Failed to find {:?}; creating it", directory);
            std::fs::create_dir_all(&directory)?;
        } else {
            if !directory.is_dir() {
                debug!("Linked directory {:?} is a file", directory);
                return Err(KvError::Parse(
                    format!("{:?} is not a directory", directory).into(),
                ));
            }
        }

        let immutable_files = ImmutableFiles::new(directory.as_path())?;
        let log_file_path = directory.clone().join(format!("{}.log", Uuid::new_v4()));
        let key_directory = immutable_files.build_state(&log_file_path)?;

        let store = Self {
            immutable_files,
            directory: Pin::new(Box::new(directory)),
            active_path: Arc::new(RwLock::new(log_file_path.clone())),
            key_directory: Arc::new(RwLock::new(key_directory)),
        };
        info!("State read, application ready for requests");
        Ok(store)
    }

    fn set(&self, key: String, value: String) -> crate::Result<()> {
        let record = Record::new(key, Some(value));
        self.write(record)
    }

    fn get(&self, key: String) -> crate::Result<Option<String>> {
        let keydir = self.key_directory.read().unwrap().find(&key)?;
        let keydir_lock = keydir.read().unwrap();
        // Check if we are reading from the active file. If we are, we need to
        // flush the writer so that we know for sure that all changes have been
        // committed to disk.
        let file_id = &*keydir_lock.file_id.read().unwrap();
        let active_path = &*self.active_path.read().unwrap();
        if file_id == active_path {
            debug!("Reading from active file. Flushing write buffer");
            self.key_directory.read().unwrap().flush()?;
        }
        let mut reader = BufReader::new(File::open(file_id.as_path())?);
        let mut value = vec![0u8; keydir_lock.value_size];
        let seek_position: i64 = keydir_lock.value_position.try_into().unwrap();
        debug!(
            "Reading from string ({} to {}) in file: {:?}",
            seek_position,
            seek_position + keydir_lock.value_size as i64,
            file_id.as_path()
        );
        reader.seek_relative(seek_position)?;
        reader.read(&mut value)?;
        Ok(Some(String::from_utf8(value).unwrap()))
    }

    fn remove(&self, key: String) -> crate::Result<()> {
        self.key_directory.read().unwrap().find(&key)?;
        let record = Record::new(key, None);
        self.write(record)
    }
}
