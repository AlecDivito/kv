use crc::{Crc, CRC_32_ISCSI};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    convert::TryInto,
    ffi::OsStr,
    fs::File,
    io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    ops::Index,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex, RwLock, RwLockReadGuard},
};
use uuid::Uuid;

use crate::{common::now, KvError, KvsEngine};

#[derive(Default, Deserialize, Serialize, Debug)]
struct Hint {
    timestamp: u128,
    value_size: usize,
    value_position: usize,
    key: String,
}

impl Hint {
    pub fn new(record: &Record, value_position: usize) -> Self {
        Self {
            timestamp: record.timestamp,
            value_size: record
                .value
                .clone()
                .unwrap_or(String::with_capacity(0))
                .len(),
            value_position,
            key: record.key.clone(),
        }
    }
}

#[derive(Default, Deserialize, Serialize, Debug)]
struct Record {
    crc: u32,
    timestamp: u128,
    key: String,
    value: Option<String>,

    #[serde(skip)]
    trailing: Vec<u8>,
}

impl Record {
    pub fn new(key: String, value: Option<String>) -> Self {
        let timestamp = now();
        let mut record = Self {
            crc: 0,
            timestamp,
            key,
            value,
            trailing: Vec::with_capacity(0),
        };
        record.crc = record.calculate_crc();
        record
    }

    pub fn calculate_crc(&self) -> u32 {
        let crc = Crc::<u32>::new(&CRC_32_ISCSI);
        let mut digest = crc.digest();
        digest.update(&self.timestamp.to_be_bytes());
        digest.update(self.key.as_bytes());
        digest.update(
            self.value
                .clone()
                .unwrap_or(String::with_capacity(0))
                .as_bytes(),
        );
        digest.finalize()
    }
}

impl std::fmt::Display for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Record({}, {}): {} -> {}",
            self.crc,
            self.timestamp,
            self.key,
            self.value.as_ref().unwrap_or(&"".to_string())
        )
    }
}

struct Key {
    file_id: Arc<RwLock<PathBuf>>,
    value_size: usize,
    value_position: usize,
    timestamp: u128,
}

impl Key {
    pub fn from_record(path: Arc<RwLock<PathBuf>>, record: &Record, value_position: usize) -> Self {
        Self {
            file_id: path,
            value_size: record
                .value
                .clone()
                .unwrap_or(String::with_capacity(0))
                .len(),
            value_position,
            timestamp: record.timestamp,
        }
    }

    pub fn from_hint(path: Arc<RwLock<PathBuf>>, hint: &Hint) -> Self {
        Self {
            file_id: path,
            value_size: hint.value_size,
            value_position: hint.value_position,
            timestamp: hint.timestamp,
        }
    }
}

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Key({:?}, {}): {} -> {}",
            &*self.file_id.read().unwrap(),
            self.timestamp,
            self.value_position,
            self.value_position + self.value_size
        )
    }
}

pub struct KeyDir {
    inner: Arc<RwLock<BTreeMap<String, Arc<RwLock<Key>>>>>,
    active_file: Arc<Mutex<BufWriter<File>>>,
    active_path: Arc<RwLock<PathBuf>>,
}

impl KeyDir {
    pub fn new(path: impl Into<PathBuf>) -> crate::Result<Self> {
        let active_path = path.into();
        debug!("Created new file: {:?}", &active_path);
        let active_file = Arc::new(Mutex::new(BufWriter::new(File::create(
            &active_path.as_path(),
        )?)));
        Ok(Self {
            inner: Arc::new(RwLock::new(BTreeMap::new())),
            active_path: Arc::new(RwLock::new(active_path)),
            active_file,
        })
    }

    /// `find` the value if it exists inside of the keyed directory
    fn find(&self, key: &str) -> crate::Result<Arc<RwLock<Key>>> {
        let map = self.inner.read().unwrap();
        let keydir = map.get(key);
        match keydir {
            Some(key) => Ok(key.clone()),
            None => Err(KvError::KeyNotFound(
                format!("Key {:?} could not be found", key).into(),
            )),
        }
    }

    /// `write` assumes that the record that was given is the absolute truth. Using
    /// it, it will append the record to disk on the active file, then commit it
    /// to the interal hashmap.
    fn write(&self, record: Record) -> crate::Result<()> {
        let mut writer = self.active_file.lock().unwrap();
        let bytes = bincode::serialize(&record)?;
        writer.write(&bytes)?;
        writer.flush()?;
        let record_head = writer.seek(SeekFrom::Current(0)).unwrap() as usize;
        let value_position = record_head - record.value.as_ref().unwrap_or(&"".to_string()).len();
        trace!("Wrote record ({}) to buffer", &record);
        if record.value.is_none() {
            self.inner.write().unwrap().remove(&record.key);
        } else {
            let key = Key::from_record(self.active_path.clone(), &record, value_position);
            trace!("saving keydir ({})", key);
            self.inner
                .write()
                .unwrap()
                .insert(record.key, Arc::new(RwLock::new(key)));
        }
        Ok(())
    }

    /// `append` assumes that the user is reading from an existing file and is
    /// currently building the datastructure for whatever reason. It does not
    /// commit it's knowledge to a file and only saves the values to the internal
    /// hashmap.
    fn append(&self, record_head: usize, file_path: Arc<RwLock<PathBuf>>, record: Record) {
        if record.calculate_crc() != record.crc {
            error!(
                "Corruption found inside of record while merging: {:?}",
                record
            );
            return;
        }
        // 3. Write to our datastructe if our data is new
        let mut map = self.inner.write().unwrap();
        let key = map.get(&record.key);
        if key.is_none() || key.map(|e| e.read().unwrap().timestamp).unwrap() <= record.timestamp {
            if record.value.is_none() {
                map.remove(&record.key);
            } else {
                let value_position = record_head - record.value.as_ref().unwrap().len();
                let key = Key::from_record(file_path, &record, value_position);
                map.insert(record.key, Arc::new(RwLock::new(key)));
            }
        }
    }

    /// `insert` will insert the key data
    fn insert(&self, key: String, value: Key) {
        self.inner
            .write()
            .unwrap()
            .insert(key, Arc::new(RwLock::new(value)));
    }

    /// `merge` takes all of the data that is currently saved inside of the key
    /// value store and saves all of the values in one active file. It is assumed
    /// you call this from a seprate thread that contains a copy of all of the
    /// data.
    pub fn merge(&self, hint_path: Arc<RwLock<PathBuf>>) -> crate::Result<()> {
        let helper_path = &*hint_path.read().unwrap();
        let mut helper_writer = BufWriter::new(File::create(helper_path).unwrap());
        let mut writer = self.active_file.lock().unwrap();
        let map = self.inner.read().unwrap();
        for (key, pointer) in &*map {
            // read value
            let pointer_lock = pointer.read().unwrap();
            let mut reader =
                BufReader::new(File::open(pointer_lock.file_id.read().unwrap().as_path())?);
            let mut value = String::with_capacity(pointer_lock.value_size);
            reader.seek_relative(pointer_lock.value_position.try_into().unwrap())?;
            reader.read(unsafe { value.as_bytes_mut() })?;
            // write to active file
            let record = Record::new(key.clone(), Some(value.clone()));
            let bytes = bincode::serialize(&record)?;
            writer.write(&bytes)?;
            let value_position = writer.seek(SeekFrom::Current(0)).unwrap() as usize - value.len();
            let hint = Hint::new(&record, value_position);
            // no self write here because we are just handling the happy path
            // write to hint
            let hint_bytes = bincode::serialize(&hint)?;
            helper_writer.write(&hint_bytes)?;
        }

        Ok(())
    }

    pub fn set_active_file(&mut self, path: impl Into<PathBuf>) -> crate::Result<()> {
        let path = path.into();
        debug!("Creating new active file to write too: {:?}", &path);
        let new_active_file = BufWriter::new(
            std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(&path.as_path())?,
        );
        self.active_path = Arc::new(RwLock::new(path));
        *self.active_file.lock().unwrap() = new_active_file;
        Ok(())
    }

    pub fn active_file_length(&self) -> usize {
        let a = self
            .active_file
            .lock()
            .unwrap()
            .seek(SeekFrom::Current(0))
            .unwrap();
        a as usize
    }

    pub fn flush(&self) -> crate::Result<()> {
        Ok(self.active_file.lock().unwrap().flush()?)
    }
}

#[derive(Clone)]
struct ImmutableFiles {
    inner: Arc<RwLock<Vec<Arc<RwLock<PathBuf>>>>>,
    hint: Arc<Mutex<Option<PathBuf>>>,
}

impl ImmutableFiles {
    pub fn new(directory: impl Into<PathBuf>) -> crate::Result<Self> {
        let directory = directory.into();
        let mut inner = Vec::new();
        let mut hint = None;
        let dirs = std::fs::read_dir(&directory)?;
        for dir in dirs {
            let entry = dir?;
            if entry.path().is_dir() {
                continue;
            }
            let stem = entry
                .path()
                .file_stem()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string();
            let hint_id = format!("{}.hint", stem);
            let hint_path: PathBuf = directory.clone().join(hint_id);
            if hint.is_none() && hint_path.exists() && hint_path.is_file() {
                debug!("Found hint file: {:?}", hint_path.as_path());
                hint = Some(hint_path);
            } else {
                debug!("Added file to immutable files: {:?}", entry.path());
                inner.push(Arc::new(RwLock::new(entry.path())));
            }
        }
        Ok(Self {
            inner: Arc::new(RwLock::new(inner)),
            hint: Arc::new(Mutex::new(hint)),
        })
    }

    pub fn build_state(&self, active_path: Arc<RwLock<PathBuf>>) -> crate::Result<KeyDir> {
        let key_directory = KeyDir::new(active_path.read().unwrap().as_path())?;
        let paths = &*self.inner.read().unwrap();
        let hint = self.hint.lock().unwrap();
        let paths_lock = match &*hint {
            Some(hint) => {
                // check if a hint file exists
                let hint_stem = hint.file_stem().unwrap().to_str().unwrap().to_string();
                let hint_data_path = paths
                    .iter()
                    .find(|p| {
                        p.read()
                            .unwrap()
                            .file_stem()
                            .unwrap()
                            .to_str()
                            .unwrap()
                            .to_string()
                            == hint_stem
                    })
                    .unwrap();

                let mut reader = BufReader::new(File::open(&*hint_data_path.read().unwrap())?);
                while reader.fill_buf().unwrap().len() != 0 {
                    let hint = bincode::deserialize_from(&mut reader)?;
                    let key = Key::from_hint(hint_data_path.clone(), &hint);
                    key_directory.insert(hint.key, key);
                }

                // return all of the immutable files without the hint
                paths
                    .iter()
                    .filter(|p| {
                        p.read()
                            .unwrap()
                            .file_stem()
                            .unwrap()
                            .to_str()
                            .unwrap()
                            .to_string()
                            != hint_stem
                    })
                    .map(|a| a.clone())
                    .collect::<Vec<Arc<RwLock<PathBuf>>>>()
            }
            None => paths.clone(),
        };

        // loop over rest of files
        for path in &*paths_lock {
            let lock = path.read().unwrap();

            // skip if this is the hint_path
            let mut reader = BufReader::new(File::open(lock.as_path())?);
            while reader.fill_buf().unwrap().len() != 0 {
                let record: Record = match bincode::deserialize_from(&mut reader) {
                    Ok(r) => r,
                    Err(e) => {
                        error!("Decoding error: {}", e);
                        std::process::exit(1);
                    }
                };
                trace!("Adding record ({})", &record);
                let record_head = reader.seek(SeekFrom::Current(0)).unwrap() as usize;
                key_directory.append(record_head, path.clone(), record);
            }
        }
        Ok(key_directory)
    }

    pub fn overwrite_pointers(&mut self, new_source: impl Into<PathBuf>) {
        let source: PathBuf = new_source.into();
        let paths_lock = self.inner.read().unwrap();
        for path in &*paths_lock {
            let mut lock = path.write().unwrap();
            *lock = source.clone();
        }
    }

    pub fn overwrite_hint_pointer(&mut self, hint_path: impl Into<PathBuf>) {
        let hint_path = hint_path.into();
        let mut lock = self.hint.lock().unwrap();
        *lock = Some(hint_path);
    }

    pub fn as_paths(&self) -> Vec<PathBuf> {
        self.inner
            .read()
            .unwrap()
            .iter()
            .map(|rc| rc.read().unwrap().to_path_buf())
            .filter(|rc| rc.extension().unwrap_or(OsStr::new("")) == "log")
            .collect()
    }

    pub fn hint_path(&self) -> Option<PathBuf> {
        (&*self.hint.lock().unwrap()).to_owned()
    }

    pub fn push(&self, path: Arc<RwLock<PathBuf>>) {
        self.inner.write().unwrap().push(path);
    }

    pub fn len(&self) -> usize {
        self.inner.read().unwrap().len()
    }
}

/// KvStore stores all the data for the kvstore
#[derive(Clone)]
pub struct KvStore {
    directory: Pin<Box<PathBuf>>,
    active_path: Arc<RwLock<PathBuf>>,
    immutable_files: ImmutableFiles,
    key_directory: Arc<RwLock<KeyDir>>,
}

impl KvStore {
    fn write(&self, record: Record) -> crate::Result<()> {
        self.key_directory.read().unwrap().write(record)?;

        // when we have finished writing, we need to check if we've hit our file
        // limit. If we have, we need to retire the current active file and open
        // a new one.
        // if self.key_directory.read().unwrap().active_file_length() > 55000000 {
        if self.key_directory.read().unwrap().active_file_length() > 1000 {
            debug!("Active file is too large, writing it to disk");
            // 1. Append current active file to our stack
            self.immutable_files
                .push(self.key_directory.read().unwrap().active_path.clone());
            // 2. Create a new active file
            let active_file_path = self
                .directory
                .clone()
                .join(format!("{}.log", Uuid::new_v4()));
            *self.active_path.write().unwrap() = active_file_path.clone();
            self.key_directory
                .write()
                .unwrap()
                .set_active_file(active_file_path)?;
            // 3. Compact
            if self.immutable_files.len() > 3 {
                debug!("Too many files found. Compacting them together.");
                let mut immutable_files = self.immutable_files.clone();
                let name = Uuid::new_v4();
                let merge_path = self.directory.clone().join(format!("{}.log", name));
                let hint_path = self.directory.clone().join(format!("{}.hint", name));
                let merge_file = Arc::new(RwLock::new(merge_path.clone()));
                let hint_file = Arc::new(RwLock::new(hint_path.clone()));
                std::thread::spawn(move || {
                    // old paths
                    let old_immutable_file_paths = immutable_files.as_paths();
                    let old_immutable_hint_path = immutable_files.hint_path();

                    // build merge and hint files
                    let keydir = immutable_files.build_state(merge_file.clone()).unwrap();
                    keydir.merge(hint_file.clone()).unwrap();

                    // overwrite immutable_files to point to the merged file
                    immutable_files.overwrite_pointers(merge_path);
                    immutable_files.overwrite_hint_pointer(hint_path);
                    // delete all old immutable_files
                    if let Some(old_hint_path) = old_immutable_hint_path {
                        debug!("Removed old hint file: {:?}", old_hint_path);
                        std::fs::remove_file(old_hint_path).unwrap();
                    }
                    for path in old_immutable_file_paths {
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
        let active_file_name = PathBuf::from(format!("{}.log", Uuid::new_v4()));
        let active_path = Arc::new(RwLock::new(
            directory.clone().join(active_file_name.as_path()),
        ));
        let key_directory = immutable_files.build_state(Arc::new(RwLock::new(
            directory.clone().join(active_file_name.as_path()),
        )))?;
        info!("State read, application ready for requests");

        Ok(Self {
            directory: Pin::new(Box::new(directory)),
            immutable_files,
            active_path,
            key_directory: Arc::new(RwLock::new(key_directory)),
        })
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
