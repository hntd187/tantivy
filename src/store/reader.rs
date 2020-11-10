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
    current_block_offset: RefCell<usize>,
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
            current_block_offset: RefCell::new(usize::max_value()),
            current_block: RefCell::new(Vec::new()),
            space_usage,
        })
    }

    pub(crate) fn iter_blocks<'a>(&'a self) -> impl Iterator<Item = (DocId, u64)> + 'a {
        self.skip_index.cursor()
    }

    fn block_offset(&self, doc_id: DocId) -> (DocId, u64) {
        self.skip_index
            .cursor()
            .seek(doc_id + 1u32)
            .map(|(doc, offset)| (doc, offset))
            .unwrap_or((0u32, 0u64))
    }

    pub(crate) fn block_data(&self) -> io::Result<OwnedBytes> {
        self.data.read_bytes()
    }

    fn compressed_block(&self, addr: usize) -> io::Result<OwnedBytes> {
        let (block_len_bytes, block_body) = self.data.slice_from(addr).split(4);
        let block_len = u32::deserialize(&mut block_len_bytes.read_bytes()?)?;
        block_body.slice_to(block_len as usize).read_bytes()
    }

    fn read_block(&self, block_offset: usize) -> io::Result<()> {
        if block_offset != *self.current_block_offset.borrow() {
            let mut current_block_mut = self.current_block.borrow_mut();
            current_block_mut.clear();
            let compressed_block = self.compressed_block(block_offset)?;
            decompress(compressed_block.as_slice(), &mut current_block_mut)?;
            *self.current_block_offset.borrow_mut() = block_offset;
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
        let (first_doc_id, block_offset) = self.block_offset(doc_id);
        self.read_block(block_offset as usize)?;
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
