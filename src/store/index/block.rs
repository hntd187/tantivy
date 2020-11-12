use crate::common::VInt;
use crate::store::index::{Checkpoint, PERIOD};
use crate::DocId;
use std::io;

pub struct Block {
    pub checkpoints: Vec<Checkpoint>,
}

impl Default for Block {
    fn default() -> Block {
        Block {
            checkpoints: Vec::with_capacity(PERIOD),
        }
    }
}

impl Block {
    pub fn first_last_doc(&self) -> Option<(DocId, DocId)> {
        let first_doc_opt = self.checkpoints
            .last()
            .cloned()
            .map(|checkpoint| checkpoint.first_doc);
        let last_doc_opt = self.checkpoints
            .last()
            .cloned()
            .map(|checkpoint| checkpoint.last_doc);
        match (first_doc_opt, last_doc_opt) {
            (Some(first_doc), Some(last_doc)) => {
                Some((first_doc, last_doc))
            },
            _ => None
        }
    }

    pub fn push(&mut self, checkpoint: Checkpoint) {
        self.checkpoints.push(checkpoint);
    }

    pub fn len(&self) -> usize {
        self.checkpoints.len()
    }

    pub fn get(&self, idx: usize) -> Checkpoint {
        self.checkpoints[idx]
    }

    pub fn clear(&mut self) {
        self.checkpoints.clear();
    }

    pub fn serialize(&mut self, buffer: &mut Vec<u8>) {
        VInt(self.checkpoints.len() as u64).serialize_into_vec(buffer);
        if self.checkpoints.is_empty() {
            return;
        }
        VInt(self.checkpoints[0].first_doc as u64).serialize_into_vec(buffer);
        for checkpoint in &self.checkpoints {
            let delta_doc = checkpoint.last_doc - checkpoint.first_doc + 1;
            VInt(delta_doc as u64).serialize_into_vec(buffer);
            VInt(checkpoint.end_offset - checkpoint.start_offset).serialize_into_vec(buffer);
        }
    }

    pub fn deserialize(&mut self, data: &mut &[u8]) -> io::Result<()> {
        if data.is_empty() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, ""));
        }
        self.checkpoints.clear();
        let len = VInt::deserialize_u64(data)? as usize;
        if len == 0 {
            return Ok(());
        }
        let mut doc= VInt::deserialize_u64(data)? as DocId;
        let mut start_offset = 0u64;
        for _ in 0..len {
            let num_docs = VInt::deserialize_u64(data)? as DocId;
            let block_num_bytes = VInt::deserialize_u64(data)?;
            self.checkpoints.push(Checkpoint {
                first_doc: doc,
                last_doc: doc + num_docs - 1,
                start_offset,
                end_offset: start_offset + block_num_bytes
            });
            doc += num_docs;
            start_offset += block_num_bytes;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::DocId;
    use crate::store::index::Checkpoint;
    use crate::store::index::block::Block;
    use std::io;

    #[test]
    fn test_block_serialize() -> io::Result<()> {
        let mut block = Block::default();
        let offsets: Vec<u64> = (0..11).map(|i| i * i * i).collect();
        let mut first_doc =0 ;
        for i in 0..10 {
            let last_doc = (i * i) as DocId;
            block.push(Checkpoint {
                first_doc,
                last_doc: (i * i) as u32,
                start_offset: offsets[i], 
                end_offset: offsets[i + 1]
            });
            first_doc = last_doc + 1;
        }
        let mut buffer = Vec::new();
        block.serialize(&mut buffer);
        let mut block_deser = Block::default();
        let checkpoint = Checkpoint {
            first_doc: 0,
            last_doc: 1,
            start_offset: 2,
            end_offset: 3
        };
        block_deser.push(checkpoint); // < check that value is erased before deser
        let mut data = &buffer[..];
        block_deser.deserialize(&mut data)?;
        assert!(data.is_empty());
        assert_eq!(&block.checkpoints[..], &block_deser.checkpoints[..]);
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
