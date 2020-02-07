use std::{
    collections::HashMap,
    path::{ self, PathBuf, Path },
    io::{ self, BufWriter, BufReader, Write, Read, Seek, SeekFrom },
    fs::{ self, File, OpenOptions },
};
use serde::{
    Serialize, Deserialize,
};
use serde_json::{ self, Deserializer };
use crate::{KvsError, Result};

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
struct CmdPos {
    gen: u64,
    pos: u64,
    len: u64,
}

/// `KvStore` is an in-memory key-value pairs storage.
///
/// Simply use `HashMap` to store key-value in memory without persistence.
///
/// Example:
/// ```rust
/// use simple_kv::{KvStore, Result};
/// use std::env::current_dir;
///
/// fn main() -> Result<()> {
///     let mut kvs = KvStore::open(&current_dir()?)?;
///     kvs.set("Hello".to_string(), "world".to_string())?;
///
///     drop(kvs);
///     let mut kvs = KvStore::open(&current_dir()?)?;
///     let val = kvs.get("Hello".to_string())?;
///     assert_eq!(val, Some("world".to_string()));
///
///     Ok(())
/// }
/// ```
pub struct KvStore {
    /// `db_path` represents the dir path of log files.
    db_path: PathBuf,
    /// `key_gen_map` is an in-memory map that maintains
    /// a map between specified `key` and cmd position in disk.
    key_gen_map: HashMap<String, CmdPos>,
    /// current generation number.
    cur_gen: u64,
    /// Writer of current `cur_gen` log file.
    writer: BufWriterWithPos<File>,
    /// Readers of log files in `db_path`
    readers: HashMap<u64, BufReaderWithPos<File>>,
    /// Compaction threshold
    compaction_threshold: u64,
    /// Compaction Size is the size of stale data in kvs
    compaction_size: u64,
}

impl KvStore {
    /// Create a `KvStore` with a given `path`.
    pub fn open(path: &path::Path) -> Result<Self> {
        let path = path.to_path_buf();
        fs::create_dir_all(&path)?;

        let mut readers = HashMap::new();
        let mut key_gen_map = HashMap::new();
        let mut compaction_size = 0 as u64;
        let gen_list = get_sorted_gen_list(&path)?;
        for &gen in &gen_list {
            let mut reader =
                BufReaderWithPos::new(File::open(log_path(&path, gen))?)?;
            compaction_size += load(gen, &mut reader, &mut key_gen_map)?;
            readers.insert(gen, reader);
        }

        let cur_gen = *(gen_list.last().unwrap_or(&0)) + 1;
        let writer = new_log_file(&path, cur_gen, &mut readers)?;

        Ok(KvStore {
            db_path: path,
            key_gen_map,
            cur_gen,
            writer,
            readers,
            compaction_threshold: COMPACTION,
            compaction_size,
        })
    }

    /// Set the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Cmd::Set { key, value };
        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;

        if let Cmd::Set { key, value: _ } = cmd {
            if let Some(old_cmd_pos) = self.key_gen_map.insert(key, CmdPos {
                gen: self.cur_gen,
                pos,
                len: self.writer.pos - pos,
            }) {
                self.compaction_size += old_cmd_pos.len;
            }
        }

        if self.compaction_size > self.compaction_threshold {
            self.compaction()?;
        }
        Ok(())
    }

    /// Get the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.key_gen_map.get(&*key) {
            let reader = self
                .readers
                .get_mut(&cmd_pos.gen)
                .expect("Cannot find log reader");
            reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            let cmd_reader = reader.take(cmd_pos.len);
            if let Cmd::Set { value, .. } = serde_json::from_reader(cmd_reader)? {
                Ok(Some(value))
            } else {
                Err(KvsError::UndefCmdline)
            }
        } else {
            Ok(None)
        }
    }

    /// Remove a given key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.key_gen_map.contains_key(&key) {
            let cmd = Cmd::Rm { key };
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;
            if let Cmd::Rm { key } = cmd {
                let old_cmd = self.key_gen_map.remove(&key).expect("key not found");
                self.compaction_size += old_cmd.len;
            }
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    /// `Compaction` the log files
    pub fn compaction(&mut self) -> Result<()> {
        let compaction_gen = self.cur_gen + 1;
        self.cur_gen += 2;
        self.writer = self.new_log_file(self.cur_gen)?;
        let mut compaction_writer = self.new_log_file(compaction_gen)?;

        let mut new_pos = 0;
        for cmd_pos in self.key_gen_map.values_mut() {
            let reader = self.readers.get_mut(&cmd_pos.gen)
                .expect("Cannot find log reader");
            reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            let mut reader = reader.take(cmd_pos.len);

            let len = io::copy(&mut reader, &mut compaction_writer)?;
            *cmd_pos = CmdPos {
                gen: compaction_gen,
                pos: new_pos,
                len,
            };

            new_pos += len;
        }

        let stale_gens: Vec<u64> = self.readers.keys()
            .filter(|&&num| num < compaction_gen)
            .map(|&x| x)
            .collect();

        for &gen in &stale_gens {
            self.readers.remove(&gen);
            fs::remove_file(log_path(&self.db_path, gen))?;
        }

        self.compaction_size = 0;

        Ok(())
    }

    fn new_log_file(&mut self, gen: u64) -> Result<BufWriterWithPos<File>> {
        new_log_file(&self.db_path, gen, &mut self.readers)
    }
}

fn log_path(path: &Path, gen: u64) -> PathBuf {
    let mut buf = path.to_path_buf();
    buf.push(Path::new(&format!("{}.log", gen)));
    buf
}

fn new_log_file(path: &Path,
                gen: u64,
                readers: &mut HashMap<u64, BufReaderWithPos<File>>)
    -> Result<BufWriterWithPos<File>> {
    let new_log_path = log_path(path, gen);
    let writer = OpenOptions::new()
        .create(true).append(true).write(true).open(&new_log_path)?;
    readers.insert(gen, BufReaderWithPos::new(File::open(&new_log_path)?)?);
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
        key_gen_map: &mut HashMap<String, CmdPos>)
    -> Result<u64> {
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream =
        Deserializer::from_reader(reader).into_iter::<Cmd>();
    let mut compaction_size = 0 as u64;
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Cmd::Set { key, value:_ } => {
                if let Some(old_cmd_pos) = key_gen_map.insert(key, CmdPos {
                    gen,
                    pos,
                    len: new_pos - pos,
                }) {
                    compaction_size += old_cmd_pos.len;
                }
            },
            Cmd::Rm { key } => {
                if let Some(old_cmd_pos) = key_gen_map.remove(&*key) {
                    compaction_size += old_cmd_pos.len;
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
