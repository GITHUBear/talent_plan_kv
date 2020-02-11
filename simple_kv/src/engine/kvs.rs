use std::{
    collections::{
        BTreeMap,
    },
    path::{ self, PathBuf, Path },
    io::{ self, BufWriter, BufReader, Write, Read, Seek, SeekFrom },
    fs::{ self, File, OpenOptions },
    sync::{ Arc, Mutex },
    sync::atomic::{
        AtomicUsize,
        Ordering,
    },
    cell::RefCell,
};
use serde::{
    Serialize, Deserialize,
};
use serde_json::{ self, Deserializer };
use crossbeam_skiplist::SkipMap;
use crate::{KvsError, Result};
use super::KvsEngine;

const COMPACTION: u64 = 1024 * 1024;

/// `BufWriterWithPos` is a wrapper of `BufWriter` to simplify positioning.
struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize>{
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()>{
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64>{
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(mut writable: W) -> Result<Self> {
        let pos = writable.seek(SeekFrom::Current(0))?;
        Ok(BufWriterWithPos {
            writer: BufWriter::new(writable),
            pos,
        })
    }
}

/// `BufReaderWithPos` is a wrapper of `BufReader` to simplify positioning.
struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize>{
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64>{
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    fn new(mut readable: R) -> Result<Self> {
        let pos = readable.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithPos {
            reader: BufReader::new(readable),
            pos,
        })
    }
}

/// `CmdPos` illustrates the cmd position in log files.
///
/// `gen` is the generation number of log file.
/// `pos` is the offset of the specified `gen` log file.
/// `len` is the length of the cmd.
#[derive(Clone)]
struct CmdPos {
    gen: u64,
    pos: u64,
    len: u64,
}

struct KvStoreReader {
    path: Arc<PathBuf>,
    safe_point: Arc<AtomicUsize>,
    // for single thread
    readers: RefCell<BTreeMap<u64, BufReaderWithPos<File>>>,
}

impl KvStoreReader {
    // Close stale handle before read to remove stale log files.
    fn close_stale_handle(&self) {
        let mut readers = self.readers.borrow_mut();
        while !readers.is_empty() {
            let first_key = *(readers.keys().next().unwrap());
            if self.safe_point.load(Ordering::SeqCst) <= (first_key as usize) {
                break;
            }
            readers.remove(&first_key);
        }
    }

    fn read_command(&self, cmd_pos: CmdPos) -> Result<Cmd> {
        self.close_stale_handle();
        let mut readers = self.readers.borrow_mut();
        if !readers.contains_key(&cmd_pos.gen) {
            let new_reader =
                BufReaderWithPos::new(
                    File::open(log_path(&self.path, cmd_pos.gen))?)?;
            readers.insert(cmd_pos.gen, new_reader);
        }
        let reader = readers.get_mut(&cmd_pos.gen).unwrap();
        reader.seek(SeekFrom::Start(cmd_pos.pos))?;
        let reader = reader.take(cmd_pos.len);
        Ok(serde_json::from_reader(reader)?)
    }
}

impl Clone for KvStoreReader {
    fn clone(&self) -> Self {
        KvStoreReader {
            path: Arc::clone(&self.path),
            safe_point: Arc::clone(&self.safe_point),
            readers: RefCell::new(BTreeMap::new()),
        }
    }
}

struct KvStoreWriter {
    reader: KvStoreReader,
    writer: BufWriterWithPos<File>,
    cur_gen: u64,
    compaction_size: u64,
    path: Arc<PathBuf>,
    key_gen_map: Arc<SkipMap<String, CmdPos>>,
    compaction_threshold: u64,
}

impl KvStoreWriter {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Cmd::Set { key, value };
        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;

        if let Cmd::Set { key, value: _ } = cmd {
            if let Some(old_cmd_pos) = self.key_gen_map.get(&key) {
                self.compaction_size += old_cmd_pos.value().len;
            }
            self.key_gen_map.insert(key, CmdPos {
                gen: self.cur_gen,
                pos,
                len: self.writer.pos - pos,
            });
        }

        if self.compaction_size > self.compaction_threshold {
            self.compaction()?;
        }
        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if self.key_gen_map.contains_key(&key) {
            let cmd = Cmd::Rm { key };
            let pos = self.writer.pos;
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;
            if let Cmd::Rm { key } = cmd {
                let old_cmd = self.key_gen_map.remove(&key).expect("key not found");
                self.compaction_size += old_cmd.value().len;
                self.compaction_size += self.writer.pos - pos;
            }

            if self.compaction_size > self.compaction_threshold {
                self.compaction()?;
            }
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    fn compaction(&mut self) -> Result<()> {
        let compaction_gen = self.cur_gen + 1;
        self.cur_gen += 2;
        self.writer = new_log_file(&self.path, self.cur_gen)?;
        let mut compaction_writer = new_log_file(&self.path, compaction_gen)?;

        let mut new_pos = 0;
        for entry in self.key_gen_map.iter() {
            let cmd_pos = entry.value().clone();
            let cmd = self.reader.read_command(cmd_pos.clone())?;
            serde_json::to_writer(&mut compaction_writer, &cmd)?;
            self.key_gen_map.insert(
                entry.key().clone(),
                CmdPos {
                    gen: compaction_gen,
                    pos: new_pos,
                    len: cmd_pos.len,
                }
            );
            new_pos += cmd_pos.len;
        }
        compaction_writer.flush()?;

        self.reader.safe_point
            .store(compaction_gen as usize, Ordering::SeqCst);
        self.reader.close_stale_handle();

        let stale_gens: Vec<u64> = get_sorted_gen_list(&self.path)?
            .into_iter()
            .filter(|&n| n < compaction_gen)
            .collect();

        for &gen in &stale_gens {
            if let Err(e) = fs::remove_file(log_path(&self.path, gen)) {
                error!("[kvs-engine] File {:?} cannot be remove now: {}", self.path, e);
            }
        }

        self.compaction_size = 0;

        Ok(())
    }
}

/// `KvStore` for multi-thread.
#[derive(Clone)]
pub struct KvStore {
    /// `db_path` represents the dir path of log files.
    db_path: Arc<PathBuf>,
    /// `key_gen_map` is an in-memory map that maintains
    /// a map between specified `key` and cmd position in disk.
    key_gen_map: Arc<SkipMap<String, CmdPos>>,
    /// Writer of current `cur_gen` log file.
    writer: Arc<Mutex<KvStoreWriter>>,
    /// Readers of log files in `db_path`
    reader: KvStoreReader,
}

impl KvsEngine for KvStore {
    /// Set the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    fn set(&self, key: String, value: String) -> Result<()> {
        self.writer.lock().unwrap().set(key, value)
    }

    /// Get the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.key_gen_map.get(&key) {
            let cmd_pos = cmd_pos.value().clone();
            if let Cmd::Set { key:_, value } = self.reader.read_command(cmd_pos)? {
                Ok(Some(value))
            } else {
                Err(KvsError::UndefCmdline)
            }
        } else {
            Ok(None)
        }
    }

    /// Remove a given key.
    fn remove(&self, key: String) -> Result<()> {
        self.writer.lock().unwrap().remove(key)
    }
}

impl KvStore {
    /// Create a `KvStore` with a given `path`.
    pub fn open(path: &path::Path) -> Result<Self> {
        let path = Arc::new(path.to_path_buf());
        fs::create_dir_all(&*path)?;

        let mut readers = BTreeMap::new();
        let key_gen_map = SkipMap::new();
        let mut compaction_size = 0 as u64;
        let gen_list = get_sorted_gen_list(&path)?;
        for &gen in &gen_list {
            let mut reader =
                BufReaderWithPos::new(File::open(log_path(&path, gen))?)?;
            compaction_size += load(gen, &mut reader, &key_gen_map)?;
            readers.insert(gen, reader);
        }

        let cur_gen = *(gen_list.last().unwrap_or(&0)) + 1;
        let writer = new_log_file(&path, cur_gen)?;
        let safe_point = Arc::new(AtomicUsize::new(0));

        let reader = KvStoreReader {
            path: Arc::clone(&path),
            safe_point,
            readers: RefCell::new(readers),
        };

        let key_gen_map = Arc::new(key_gen_map);
        let writer = KvStoreWriter {
            reader: reader.clone(),
            writer,
            cur_gen,
            compaction_size,
            path: Arc::clone(&path),
            key_gen_map: Arc::clone(&key_gen_map),
            compaction_threshold: COMPACTION,
        };

        Ok(KvStore {
            db_path: Arc::clone(&path),
            key_gen_map: Arc::clone(&key_gen_map),
            writer: Arc::new(Mutex::new(writer)),
            reader,
        })
    }
}

fn log_path(path: &Path, gen: u64) -> PathBuf {
    let mut buf = path.to_path_buf();
    buf.push(Path::new(&format!("{}.log", gen)));
    buf
}

fn new_log_file(path: &Path, gen: u64)
                -> Result<BufWriterWithPos<File>> {
    let new_log_path = log_path(path, gen);
    let writer = OpenOptions::new()
        .create(true).append(true).write(true).open(&new_log_path)?;
    Ok(BufWriterWithPos::new(writer)?)
}

fn get_sorted_gen_list(path: &PathBuf) -> Result<Vec<u64>> {
    let read_dir = fs::read_dir(path)?;
    let mut gen_list: Vec<u64> = read_dir
        .flat_map(|res_dir_entry| -> Result<_> {
            Ok(res_dir_entry?.path()) })
        .filter(|path|
            path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(|file_name| file_name.to_str())
                .map(|str| str.trim_end_matches(".log").parse::<u64>()) })
        .flatten()
        .collect();
    // from smallest to largest
    gen_list.sort_unstable();
    Ok(gen_list)
}

fn load(gen: u64,
        reader: &mut BufReaderWithPos<File>,
        key_gen_map: &SkipMap<String, CmdPos>)
        -> Result<u64> {
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream =
        Deserializer::from_reader(reader).into_iter::<Cmd>();
    let mut compaction_size = 0 as u64;
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Cmd::Set { key, value:_ } => {
                if let Some(old_cmd) = key_gen_map.get(&key) {
                    compaction_size += old_cmd.value().len;
                }

                key_gen_map.insert(key, CmdPos {
                    gen,
                    pos,
                    len: new_pos - pos,
                });
            },
            Cmd::Rm { key } => {
                if let Some(old_cmd) = key_gen_map.remove(&key) {
                    compaction_size += old_cmd.value().len;
                }

                compaction_size += new_pos - pos;
            },
        }
        pos = new_pos;
    }

    Ok(compaction_size)
}

/// `Cmd` is Serializable & Deserializable
#[derive(Serialize, Deserialize, Debug)]
enum Cmd {
    /// cmd: `Set` <key> <value>
    Set {
        /// The key in a key/value pair that is ready to insert into kvs.
        key: String,
        /// The value in a key/value pair.
        value: String,
    },
    ///  cmd: `Rm` <key>
    Rm {
        /// The key in a key/value pair.
        key: String,
    },
}
