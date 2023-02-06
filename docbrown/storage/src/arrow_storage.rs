use std::{io::Write, path::PathBuf};

use arrow2::{array::Array, chunk::Chunk, datatypes::Schema, io::ipc::write::FileWriter};

// error enum that covers arrow error and io error
#[derive(Debug)]
pub enum WriterError {
    ArrowError(arrow2::error::Error),
    IoError(std::io::Error),
}

struct WritePage {
    chunk: Chunk<Box<dyn Array>>,
    response: tokio::sync::oneshot::Sender<Result<usize, WriterError>>,
}

// basically a sink where we flush pages that are full
struct AsyncPageWriter<W: Write> {
    file_writer: FileWriter<W>,
    file_path: PathBuf,
    ipc_fields: Option<Vec<arrow2::io::ipc::IpcField>>,
    receiver: tokio::sync::mpsc::Receiver<WritePage>,
    current_chunk_id: usize,
}

impl AsyncPageWriter<std::fs::File> {
    fn new(
        file_path: PathBuf,
        receiver: tokio::sync::mpsc::Receiver<WritePage>,
        schema: &Schema,
        ipc_fields: Option<Vec<arrow2::io::ipc::IpcField>>,
        compression: Option<arrow2::io::ipc::write::Compression>,
    ) -> Result<Self, WriterError> {
        let file = std::fs::File::create(file_path.clone()).map_err(WriterError::IoError)?;
        let options = arrow2::io::ipc::write::WriteOptions { compression };
        let file_writer = FileWriter::try_new(file, schema.clone(), ipc_fields.clone(), options)
            .map_err(WriterError::ArrowError)?;

        Ok(Self {
            file_writer,
            file_path,
            ipc_fields,
            receiver,
            current_chunk_id: 0,
        })
    }

    async fn run(mut self) {
        while let Some(WritePage { chunk, .. }) = self.receiver.recv().await {
            self.file_writer
                .write(&chunk, self.ipc_fields.as_ref().map(|x| x.as_ref()))
                .map_err(WriterError::ArrowError)
                .unwrap();
        }
    }
}

struct PageWriter {
    sender: tokio::sync::mpsc::Sender<WritePage>,
}

impl PageWriter {
    pub fn new(
        file_path: PathBuf,
        schema: &Schema,
        ipc_fields: Option<Vec<arrow2::io::ipc::IpcField>>,
        compression: Option<arrow2::io::ipc::write::Compression>,
    ) -> Result<Self, WriterError> {
        let (sender, receiver) = tokio::sync::mpsc::channel(100);
        let writer = AsyncPageWriter::new(file_path, receiver, schema, ipc_fields, compression)?;
        tokio::spawn(writer.run());
        Ok(Self { sender })
    }
}

// simple tokio inside a module test to get things rolling
#[cfg(test)]
mod arrow_storage_test {
    use std::{error::Error, path::PathBuf};

    use arrow2::{
        array::{Int32Array, Utf8Array},
        chunk::Chunk,
        datatypes::{DataType, Field, Schema},
        io::ipc::{
            read::read_file_metadata,
            write::{FileWriter, WriteOptions},
        },
    };

    use rand::distributions::Alphanumeric;
    use rand::{distributions::Uniform, Rng};

    fn random_chunks(
        size_of_chunk: usize,
        num_chunks: usize,
    ) -> Vec<Chunk<Box<dyn arrow2::array::Array>>> {
        let mut chunks = vec![];
        for _ in 0..num_chunks {
            let rng = rand::thread_rng();

            let range = Uniform::from(0i32..size_of_chunk as i32);
            let random_iter_i32 = rng
                .sample_iter(&range)
                .take(size_of_chunk)
                .collect::<Vec<i32>>();
            let a = Int32Array::from_slice(&random_iter_i32[..]);

            let random_vec_utf8 = (0..size_of_chunk)
                .map(|_| {
                    let rand_string: String = rand::thread_rng()
                        .sample_iter(&Alphanumeric)
                        .take(30)
                        .map(char::from)
                        .collect();
                    rand_string
                })
                .collect::<Vec<String>>();

            let b = Utf8Array::<i32>::from_slice(&random_vec_utf8[..]);
            let chunk = Chunk::try_new(vec![a.boxed(), b.boxed()]).unwrap();

            chunks.push(chunk)
        }
        chunks
    }

    // test that after writing a arrow IPC file using the arrow2 API we can read random blocks from it
    // by passing the FileMetadata to FileReader
    #[test]
    fn read_random_blocks_from_arrow_ipc_file() -> Result<(), arrow2::error::Error> {
        let schema = Schema::from(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]);

        let chunks = random_chunks(100, 5);

        let chunkds_0 = chunks[0].clone();
        let chunkds_1 = chunks[1].clone();
        let chunkds_4 = chunks[4].clone();

        let file_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "../../target/test.arrow"]
            .iter()
            .collect();

        let file = std::fs::File::create(file_path.clone())?;
        let options = WriteOptions { compression: None };
        let mut writer = FileWriter::try_new(file, schema, None, options)?;

        for chunk in chunks {
            let chunk = writer.write(&chunk, None)?;
        }

        writer.finish()?;

        let mut file = std::fs::File::open(file_path)?;
        let mut metadata = read_file_metadata(&mut file)?;
        // we can choose to read only the first two blocks, or any other block like the 4th
        // this allows us to seek into the arrow IPC file and treat it as a stream of chunks
        metadata.blocks = vec![metadata.blocks[0].clone(), metadata.blocks[1].clone(), metadata.blocks[4].clone()];
        let mut reader = arrow2::io::ipc::read::FileReader::new(file, metadata, None, None);


        assert_eq!(reader.next().unwrap().unwrap(), chunkds_0);
        assert_eq!(reader.next().unwrap().unwrap(), chunkds_1);
        assert_eq!(reader.next().unwrap().unwrap(), chunkds_4);
        assert!(reader.next().is_none());
        Ok(())
    }

    #[tokio::test]
    async fn my_test() {
        assert!(true);
    }
}
