use crate::common::{BinarySerializable, VInt};
use crate::store::index::block::Block;
use crate::store::index::PERIOD;
use crate::DocId;
use std::io;
use std::io::Write;

// Each skip contains iterator over pairs (last doc in block, offset to start of block).

struct LayerBuilder {
    buffer: Vec<u8>,
    pub block: Block,
}

impl LayerBuilder {
    fn finish(self) -> Vec<u8> {
        self.buffer
    }

    fn new() -> LayerBuilder {
        LayerBuilder {
            buffer: Vec::new(),
            block: Block::default(),
        }
    }

    fn flush_block(&mut self) -> Option<(DocId, (u64, u64))> {
        self.block.last_doc().map(|last_doc| {
            let start_offset = self.buffer.len() as u64;
            self.block.serialize(&mut self.buffer);
            let end_offset = self.buffer.len() as u64;
            self.block.clear();
            (last_doc, (start_offset, end_offset))
        })
    }

    fn push(&mut self, doc: DocId, start_offset: u64, end_offset: u64) {
        self.block.push(doc, start_offset, end_offset);
    }

    fn insert(
        &mut self,
        doc: DocId,
        start_offset: u64,
        end_offset: u64,
    ) -> Option<(DocId, (u64, u64))> {
        self.push(doc, start_offset, end_offset);
        let emit_skip_info = (self.block.len() % PERIOD) == 0;
        if emit_skip_info {
            self.flush_block()
        } else {
            None
        }
    }
}

pub struct SkipIndexBuilder {
    layers: Vec<LayerBuilder>,
}

impl SkipIndexBuilder {
    pub fn new() -> SkipIndexBuilder {
        SkipIndexBuilder { layers: Vec::new() }
    }

    fn get_layer(&mut self, layer_id: usize) -> &mut LayerBuilder {
        if layer_id == self.layers.len() {
            let layer_builder = LayerBuilder::new();
            self.layers.push(layer_builder);
        }
        &mut self.layers[layer_id]
    }

    pub fn insert(&mut self, doc: DocId, start_offset: u64, stop_offset: u64) {
        let mut skip_pointer = Some((doc, (start_offset, stop_offset)));
        for layer_id in 0.. {
            if let Some((skip_doc_id, (start_offset, stop_offset))) = skip_pointer {
                skip_pointer =
                    self.get_layer(layer_id)
                        .insert(skip_doc_id, start_offset, stop_offset);
            } else {
                break;
            }
        }
    }

    pub fn write<W: Write>(mut self, output: &mut W) -> io::Result<()> {
        let mut last_pointer = None;
        for skip_layer in self.layers.iter_mut() {
            if let Some((first_doc, (start_offset, end_offset))) = last_pointer {
                skip_layer.push(first_doc, start_offset, end_offset);
            }
            last_pointer = skip_layer.flush_block();
        }
        let layer_buffers: Vec<Vec<u8>> = self
            .layers
            .into_iter()
            .rev()
            .map(|layer| layer.finish())
            .collect();

        let mut layer_offset = 0;
        let mut layer_sizes = Vec::new();
        for layer_buffer in &layer_buffers {
            layer_offset += layer_buffer.len() as u64;
            layer_sizes.push(VInt(layer_offset));
        }
        layer_sizes.serialize(output)?;
        for layer_buffer in layer_buffers {
            output.write_all(&layer_buffer[..])?;
        }
        Ok(())
    }
}
