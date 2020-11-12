use super::decompress;
use super::index::SkipIndex;
use crate::common::VInt;
use crate::common::{BinarySerializable, HasLen};
use crate::directory::{FileSlice, OwnedBytes};
use crate::schema::Document;
use crate::space_usage::StoreSpaceUsage;
use crate::DocId;
use std::cell::RefCell;
use std::io;
use std::mem::size_of;
use std::sync::Arc;

/// Reads document off tantivy's [`Store`](./index.html)
pub struct StoreReader {
    data: FileSlice,
    skip_index: Arc<SkipIndex>,
    current_block_offset: RefCell<u64>,
    current_block: RefCell<Vec<u8>>,
    space_usage: StoreSpaceUsage,
}

impl StoreReader {
    /// Opens a store reader
    pub fn open(store_file: FileSlice) -> io::Result<StoreReader> {
        let (data_file, offset_index_file) = split_file(store_file)?;
        let index_data = offset_index_file.read_bytes()?;
        let space_usage = StoreSpaceUsage::new(data_file.len(), offset_index_file.len());
        let skip_index = SkipIndex::from(index_data);
        Ok(StoreReader {
            data: data_file,
            skip_index: Arc::new(skip_index),
            current_block_offset: RefCell::new(u64::max_value()),
            current_block: RefCell::new(Vec::new()),
            space_usage,
        })
    }

    pub(crate) fn iter_blocks<'a>(&'a self) -> impl Iterator<Item = (DocId, (u64, u64))> + 'a {
        self.skip_index.cursor()
    }

    fn block_offset(&self, doc_id: DocId) -> Option<(DocId, (u64, u64))> {
        self.skip_index.seek(doc_id)
    }

    pub(crate) fn block_data(&self) -> io::Result<OwnedBytes> {
        self.data.read_bytes()
    }

    fn compressed_block(&self, start_offset: u64, end_offset: u64) -> io::Result<OwnedBytes> {
        self.data
            .slice(start_offset as usize, end_offset as usize)
            .read_bytes()
    }

    fn read_block(&self, start_offset: u64, end_offset: u64) -> io::Result<()> {
        if start_offset != *self.current_block_offset.borrow() {
            let mut current_block_mut = self.current_block.borrow_mut();
            current_block_mut.clear();
            let compressed_block = self.compressed_block(start_offset, end_offset)?;
            decompress(compressed_block.as_slice(), &mut current_block_mut)?;
            *self.current_block_offset.borrow_mut() = start_offset;
        }
        Ok(())
    }

    /// Reads a given document.
    ///
    /// Calling `.get(doc)` is relatively costly as it requires
    /// decompressing a compressed block.
    ///
    /// It should not be called to score documents
    /// for instance.
    pub fn get(&self, doc_id: DocId) -> crate::Result<Document> {
        let (first_doc_id, (start_offset, end_offset)) = self.block_offset(doc_id).unwrap(); // TODO
                                                                                             // .ok_or_else(err)?;
        self.read_block(start_offset, end_offset)?;
        let current_block_mut = self.current_block.borrow_mut();
        let mut cursor = &current_block_mut[..];
        for _ in first_doc_id..doc_id {
            let doc_length = VInt::deserialize(&mut cursor)?.val() as usize;
            cursor = &cursor[doc_length..];
        }
        let doc_length = VInt::deserialize(&mut cursor)?.val() as usize;
        cursor = &cursor[..doc_length];
        Ok(Document::deserialize(&mut cursor)?)
    }

    /// Summarize total space usage of this store reader.
    pub fn space_usage(&self) -> StoreSpaceUsage {
        self.space_usage.clone()
    }
}

fn split_file(data: FileSlice) -> io::Result<(FileSlice, FileSlice)> {
    let (data, footer_len_bytes) = data.split_from_end(size_of::<u64>());
    let serialized_offset: OwnedBytes = footer_len_bytes.read_bytes()?;
    let mut serialized_offset_buf = serialized_offset.as_slice();
    let offset = u64::deserialize(&mut serialized_offset_buf)? as usize;
    Ok(data.split(offset))
}
