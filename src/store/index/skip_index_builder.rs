use crate::common::{BinarySerializable, VInt};
use crate::store::index::PERIOD;
use crate::DocId;
use std::io;
use std::io::Write;

struct LayerBuilder {
    buffer: Vec<u8>,
    len: usize,
    prev_doc: DocId,
    prev_value: u64,
}

impl LayerBuilder {
    fn written_size(&self) -> usize {
        self.buffer.len()
    }

    fn write(&self, output: &mut dyn Write) -> Result<(), io::Error> {
        output.write_all(&self.buffer)?;
        Ok(())
    }

    fn new() -> LayerBuilder {
        LayerBuilder {
            buffer: Vec::new(),
            len: 0,
            prev_doc: 0,
            prev_value: 0u64,
        }
    }

    fn insert(&mut self, doc: DocId, value: u64) -> Option<(DocId, u64)> {
        self.len += 1;
        VInt((doc - self.prev_doc) as u64).serialize_into_vec(&mut self.buffer);
        self.prev_doc = doc;
        let offset = self.written_size() as u64;
        let emit_skip_info = (self.len % PERIOD) == 0;
        let prev_value = self.prev_value;
        self.prev_value = value;
        if emit_skip_info {
            VInt(value).serialize_into_vec(&mut self.buffer);
            Some((doc, offset))
        } else {
            VInt(value - prev_value).serialize_into_vec(&mut self.buffer);
            None
        }
    }
}

pub struct SkipIndexBuilder {
    data_layer: LayerBuilder,
    skip_layers: Vec<LayerBuilder>,
}

impl SkipIndexBuilder {
    pub fn new() -> SkipIndexBuilder {
        SkipIndexBuilder {
            data_layer: LayerBuilder::new(),
            skip_layers: Vec::new(),
        }
    }

    fn get_skip_layer(&mut self, layer_id: usize) -> &mut LayerBuilder {
        if layer_id == self.skip_layers.len() {
            let layer_builder = LayerBuilder::new();
            self.skip_layers.push(layer_builder);
        }
        &mut self.skip_layers[layer_id]
    }

    pub fn insert(&mut self, doc: DocId, dest: u64) {
        let mut skip_pointer = self.data_layer.insert(doc, dest);
        for layer_id in 0.. {
            if let Some((skip_doc_id, skip_offset)) = skip_pointer {
                skip_pointer = self
                    .get_skip_layer(layer_id)
                    .insert(skip_doc_id, skip_offset);
            } else {
                break;
            }
        }
    }

    pub fn write<W: Write>(self, output: &mut W) -> io::Result<()> {
        let mut layer_offset: u64 = self.data_layer.buffer.len() as u64;
        let mut layer_sizes = vec![VInt(layer_offset)];
        for layer in self.skip_layers.iter().rev() {
            layer_offset += layer.buffer.len() as u64;
            layer_sizes.push(VInt(layer_offset));
        }
        layer_sizes.serialize(output)?;
        self.data_layer.write(output)?;
        for layer in self.skip_layers.iter().rev() {
            layer.write(output)?;
        }
        Ok(())
    }
}
