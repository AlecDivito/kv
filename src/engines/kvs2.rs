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
            value_size: record.value().len(),
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
}

impl Record {
    pub fn new(key: String, value: Option<String>) -> Self {
        let timestamp = now();
        let mut record = Self {
            crc: 0,
            timestamp,
            key,
            value,
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

    pub fn value(&self) -> String {
        self.value.clone().unwrap_or("".to_string())
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
    file_id: Arc<RwLock<ImmutableCache>>,
    value_size: usize,
    value_position: usize,
    timestamp: u128,
}

impl Key {
    pub fn from_record(
        path: Arc<RwLock<ImmutableCache>>,
        record: &Record,
        value_position: usize,
    ) -> Self {
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

    pub fn from_hint(path: Arc<RwLock<ImmutableCache>>, hint: &Hint) -> Self {
        Self {
            file_id: path,
            value_size: hint.value_size,
            value_position: hint.value_position,
            timestamp: hint.timestamp,
        }
    }

    pub fn to_record(&self, key: String) -> crate::Result<Record> {
        let mut cache = self.file_id.write().unwrap();
        let mut value = vec![0u8; self.value_size];
        cache
            .reader
            .seek(SeekFrom::Start(self.value_position as u64))?;
        cache.reader.read(&mut value)?;
        Ok(Record::new(key, Some(String::from_utf8(value).unwrap())))
    }
}

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Key({:?}, {}): {} -> {}",
            &*self.file_id.read().unwrap().path,
            self.timestamp,
            self.value_position,
            self.value_position + self.value_size
        )
    }
}

pub struct KeyDir {
    inner: Arc<RwLock<BTreeMap<String, Arc<RwLock<Key>>>>>,
    active_file: Arc<Mutex<BufWriter<File>>>,
    active_path: Arc<RwLock<ImmutableCache>>,
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
            active_path: Arc::new(RwLock::new(ImmutableCache {
                reader: BufReader::new(File::open(&active_path)?),
                path: active_path,
            })),
            active_file,
        })
    }

    fn read_in_hint_file(
        &self,
        hint_path: Arc<Mutex<Option<PathBuf>>>,
    ) -> crate::Result<Arc<RwLock<ImmutableCache>>> {
        let hint_file_path = hint_path.lock().unwrap().as_ref().unwrap().clone();
        let mut data_file_path = hint_file_path.clone();
        data_file_path.set_extension("log");
        let data_file_path = Arc::new(RwLock::new(ImmutableCache {
            reader: BufReader::new(File::open(&data_file_path)?),
            path: data_file_path,
        }));

        let mut reader = BufReader::new(File::open(&*hint_file_path)?);
        while reader.fill_buf().unwrap().len() != 0 {
            let hint = bincode::deserialize_from(&mut reader)?;
            let key = Key::from_hint(data_file_path.clone(), &hint);
            self.insert(hint.key, key);
        }
        Ok(data_file_path)
    }

    fn read_in_record_file(&self, record_path: Arc<RwLock<ImmutableCache>>) -> crate::Result<()> {
        let mut cache = record_path.write().unwrap();
        while cache.reader.fill_buf().unwrap().len() != 0 {
            let record: Record = bincode::deserialize_from(&mut cache.reader).unwrap();
            trace!("Adding record ({})", &record);
            let record_head = cache.reader.seek(SeekFrom::Current(0)).unwrap() as usize;
            self.append(record_head, record_path.clone(), record);
        }
        Ok(())
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
    fn append(&self, record_head: usize, file_path: Arc<RwLock<ImmutableCache>>, record: Record) {
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

    /// `save_key_dir` takes all of the data that is currently saved inside of the key
    /// value store and saves all of the values in one active file. It is assumed
    /// you call this from a seprate thread that contains a copy of all of the
    /// data.
    pub fn save_key_dir(&self, hint_path: impl Into<PathBuf>) -> crate::Result<()> {
        let mut hint_writer = BufWriter::new(File::create(hint_path.into())?);
        let mut data_writer = self.active_file.lock().unwrap();
        for (key, value) in &*self.inner.read().unwrap() {
            let record = value.read().unwrap().to_record(key.clone())?;
            // write data
            data_writer.write(&bincode::serialize(&record)?)?;
            let data_writer_head = data_writer.seek(SeekFrom::Current(0)).unwrap() as usize;
            let value_position = data_writer_head - record.value().len();
            let hint = Hint::new(&record, value_position);
            hint_writer.write(&bincode::serialize(&hint)?)?;
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
        self.active_path = Arc::new(RwLock::new(ImmutableCache {
            reader: BufReader::new(File::open(&path)?),
            path,
        }));
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

struct ImmutableCache {
    reader: BufReader<File>,
    path: PathBuf,
}

#[derive(Clone)]
struct ImmutableFiles {
    inner: Arc<RwLock<Vec<Arc<RwLock<ImmutableCache>>>>>,
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
            if hint.is_none() && entry.path().extension().unwrap() == "hint" {
                debug!("Found hint file: {:?}", entry.path());
                hint = Some(entry.path());
            } else {
                debug!("Added file to immutable files: {:?}", entry.path());
                inner.push(Arc::new(RwLock::new(ImmutableCache {
                    reader: BufReader::new(File::open(entry.path())?),
                    path: entry.path(),
                })));
            }
        }
        Ok(Self {
            inner: Arc::new(RwLock::new(inner)),
            hint: Arc::new(Mutex::new(hint)),
        })
    }

    pub fn build_state(&self, path: impl Into<PathBuf>) -> crate::Result<KeyDir> {
        let key_directory = KeyDir::new(path)?;
        let paths = &*self.inner.read().unwrap();
        let hint = self.hint.lock().unwrap();

        let record_paths = match &*hint {
            Some(hint) => {
                // find the hints log file
                let hint_path = Arc::new(Mutex::new(Some(hint.clone())));
                key_directory.read_in_hint_file(hint_path)?;

                // build data_file_path
                let mut data_file_path = hint.clone();
                data_file_path.set_extension("log");
                // return all of the immutable files without the hint
                paths
                    .iter()
                    .filter(|p| p.read().unwrap().path != data_file_path)
                    .map(|a| a.clone())
                    .collect::<Vec<Arc<RwLock<ImmutableCache>>>>()
            }
            None => paths.clone(),
        };

        // loop over rest of files
        for record_path in &*record_paths {
            key_directory.read_in_record_file(record_path.clone())?;
        }
        Ok(key_directory)
    }

    pub fn overwrite_pointers(&mut self, data_path_pointer: Arc<RwLock<ImmutableCache>>) {
        *self.inner.write().unwrap() = vec![data_path_pointer];
    }

    pub fn overwrite_hint_pointer(
        &mut self,
        hint_path: impl Into<PathBuf>,
    ) -> Arc<Mutex<Option<PathBuf>>> {
        let hint_path = hint_path.into();
        let mut lock = self.hint.lock().unwrap();
        *lock = Some(hint_path);
        drop(lock);
        self.hint.clone()
    }

    pub fn as_paths(&self) -> Vec<PathBuf> {
        self.inner
            .read()
            .unwrap()
            .iter()
            .map(|rc| rc.read().unwrap().path.clone())
            .collect()
    }

    pub fn hint_path(&self) -> Option<PathBuf> {
        (&*self.hint.lock().unwrap()).to_owned()
    }

    pub fn push(&self, path: Arc<RwLock<ImmutableCache>>) {
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
                        if old_hint_path.exists() {
                            std::fs::remove_file(old_hint_path).unwrap();
                        }
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
        // let file_id = &*keydir_lock.file_id.read().unwrap();
        // let active_path = &*self.active_path.read().unwrap();
        // if file_id == active_path {
        //     debug!("Reading from active file. Flushing write buffer");
        //     self.key_directory.read().unwrap().flush()?;
        // }
        // let mut reader = BufReader::new(File::open(file_id.as_path())?);
        let mut value = vec![0u8; keydir_lock.value_size];
        let seek_position: u64 = keydir_lock.value_position.try_into().unwrap();
        // debug!(
        //     "Reading from string ({} to {}) in file: {:?}",
        //     seek_position,
        //     seek_position + keydir_lock.value_size as i64,
        //     file_id.as_path()
        // );
        let mut cache = keydir_lock.file_id.write().unwrap();
        cache.reader.seek(SeekFrom::Start(seek_position))?;
        cache.reader.read(&mut value)?;
        Ok(Some(String::from_utf8(value).unwrap()))
    }

    fn remove(&self, key: String) -> crate::Result<()> {
        self.key_directory.read().unwrap().find(&key)?;
        let record = Record::new(key, None);
        self.write(record)
    }
}
