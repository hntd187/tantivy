use crate::common::VInt;
use crate::store::index::PERIOD;
use crate::DocId;
use std::io;

pub struct Block {
    pub doc_offsets: Vec<(DocId, u64)>,
}

impl Default for Block {
    fn default() -> Block {
        Block {
            doc_offsets: Vec::with_capacity(PERIOD),
        }
    }
}

impl Block {
    pub fn last_doc(&self) -> Option<DocId> {
        self.doc_offsets
            .last()
            .cloned()
            .map(|(last_doc, _)| last_doc)
    }

    pub fn push(&mut self, doc: DocId, offset: u64) {
        self.doc_offsets.push((doc, offset));
    }

    pub fn len(&self) -> usize {
        self.doc_offsets.len()
    }

    pub fn get(&self, idx: usize) -> (u32, u64) {
        self.doc_offsets[idx]
    }

    pub fn clear(&mut self) {
        self.doc_offsets.clear();
    }

    pub fn serialize(&mut self, buffer: &mut Vec<u8>) {
        assert!(self.doc_offsets.len() < 256);
        buffer.push(self.doc_offsets.len() as u8);
        if let Some((doc, val)) = self.doc_offsets.first().cloned() {
            VInt(doc as u64).serialize_into_vec(buffer);
            VInt(val).serialize_into_vec(buffer);
        } else {
            return;
        }
        for i in 1..self.doc_offsets.len() {
            let (prev_doc, prev_val) = self.doc_offsets[i - 1];
            let (doc, val) = self.doc_offsets[i];
            let delta_doc = doc - prev_doc;
            let delta_val = val - prev_val;
            VInt(delta_doc as u64).serialize_into_vec(buffer);
            VInt(delta_val).serialize_into_vec(buffer);
        }
    }

    pub fn deserialize(&mut self, data: &mut &[u8]) -> io::Result<()> {
        if data.is_empty() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, ""));
        }
        self.doc_offsets.clear();
        let len = data[0] as usize;
        *data = &data[1..];
        let first_doc = VInt::deserialize_u64(data)? as DocId;
        let first_offset = VInt::deserialize_u64(data)?;
        self.doc_offsets.push((first_doc, first_offset));
        let mut prev_doc = first_doc;
        let mut prev_offset = first_offset;
        for _ in 1..len {
            let doc_delta = VInt::deserialize_u64(data)? as DocId;
            let offset_delta = VInt::deserialize_u64(data)?;
            let doc = prev_doc + doc_delta;
            let offset = prev_offset + offset_delta;
            self.doc_offsets.push((doc, offset));
            prev_doc = doc;
            prev_offset = offset;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::store::index::block::Block;
    use std::io;

    #[test]
    fn test_block_serialize() -> io::Result<()> {
        let mut block = Block::default();
        for i in 0..10 {
            block.push((i * i) as u32, i * i * i);
        }
        let mut buffer = Vec::new();
        block.serialize(&mut buffer);
        let mut block_deser = Block::default();
        block_deser.push(1, 2); // < check that value is erased before deser
        let mut data = &buffer[..];
        block_deser.deserialize(&mut data)?;
        assert!(data.is_empty());
        assert_eq!(&block.doc_offsets[..], &block_deser.doc_offsets[..]);
        Ok(())
    }

    #[test]
    fn test_block_deserialize_empty() {
        let mut block = Block::default();
        let empty = &[];
        let err = block.deserialize(&mut &empty[..]).err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }
}
