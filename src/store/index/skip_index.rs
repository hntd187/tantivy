use crate::common::{BinarySerializable, VInt};
use crate::directory::OwnedBytes;
use crate::store::index::block::Block;
use crate::DocId;

pub struct LayerCursor<'a> {
    remaining: &'a [u8],
    block: Block,
    cursor: usize,
}

impl<'a> LayerCursor<'a> {
    fn empty() -> Self {
        LayerCursor {
            remaining: &[][..],
            block: Block::default(),
            cursor: 0,
        }
    }
}

impl<'a> Iterator for LayerCursor<'a> {
    type Item = (DocId, (u64, u64));

    fn next(&mut self) -> Option<(DocId, (u64, u64))> {
        if self.cursor == self.block.len() {
            if self.remaining.is_empty() {
                return None;
            }
            let (block_mut, remaining_mut) = (&mut self.block, &mut self.remaining);
            if let Err(e) = block_mut.deserialize(remaining_mut) {
                return None;
            }
            self.cursor = 0;
        }
        let res = Some(self.block.get(self.cursor));
        self.cursor += 1;
        res
    }
}

struct Layer {
    data: OwnedBytes,
}

impl Layer {
    fn cursor(&self) -> LayerCursor {
        self.cursor_at_offset(0u64)
    }

    fn cursor_at_offset(&self, start_offset: u64) -> LayerCursor {
        let data = self.data.as_slice();
        LayerCursor {
            remaining: &data[start_offset as usize..],
            block: Block::default(),
            cursor: 0,
        }
    }

    fn seek_start_at_offset(
        &self,
        target: DocId,
        mut first_doc_in_block: u32,
        offset: u64,
    ) -> Option<(DocId, (u64, u64))> {
        let cursor = self.cursor_at_offset(offset);
        for (last_doc_in_block, block_offset) in cursor {
            if last_doc_in_block >= target {
                return Some((first_doc_in_block, block_offset));
            } else {
                first_doc_in_block = last_doc_in_block + 1;
            }
        }
        None
    }
}

pub struct SkipIndex {
    layers: Vec<Layer>,
}

impl SkipIndex {
    pub(crate) fn cursor(&self) -> LayerCursor {
        self.layers
            .last()
            .map(|layer| layer.cursor())
            .unwrap_or_else(LayerCursor::empty)
    }

    pub fn seek(&self, target: DocId) -> Option<(DocId, (u64, u64))> {
        let mut first_doc: u32 = 0;
        let first_layer_len = self
            .layers
            .first()
            .map(|layer| layer.data.len() as u64)
            .unwrap_or(0u64);
        let mut start_end_offset = (0u64, first_layer_len);
        for layer in &self.layers {
            if let Some((first_doc_in_block, block_start_end_offset)) =
                layer.seek_start_at_offset(target, first_doc, start_end_offset.0)
            {
                first_doc = first_doc_in_block;
                start_end_offset = block_start_end_offset;
            } else {
                return None;
            }
        }
        Some((first_doc, start_end_offset))
    }
}

impl From<OwnedBytes> for SkipIndex {
    fn from(mut data: OwnedBytes) -> SkipIndex {
        let offsets: Vec<u64> = Vec::<VInt>::deserialize(&mut data)
            .unwrap()
            .into_iter()
            .map(|el| el.0)
            .collect();
        let mut start_offset = 0;
        let mut layers = Vec::new();
        for end_offset in offsets {
            layers.push(Layer {
                data: data.slice(start_offset as usize, end_offset as usize),
            });
            start_offset = end_offset;
        }
        SkipIndex { layers }
    }
}
