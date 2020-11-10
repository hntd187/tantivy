use crate::common::{BinarySerializable, VInt};
use crate::directory::OwnedBytes;
use crate::store::index::PERIOD;
use crate::DocId;
use std::cmp::max;

pub struct LayerCursor<'a> {
    remaining: &'a [u8],
    next_doc: Option<DocId>,
    data: &'a [u8],
    prev_val: u64,
    inner_id: usize,
}

impl<'a> Iterator for LayerCursor<'a> {
    type Item = (DocId, u64);

    fn next(&mut self) -> Option<(DocId, u64)> {
        if let Some(cur_doc) = self.next_doc {
            let mut cur_offset = VInt::deserialize_u64(&mut self.remaining).unwrap();
            self.inner_id += 1;
            if self.inner_id == PERIOD {
                self.inner_id = 0;
            } else {
                cur_offset += self.prev_val;
            }
            self.prev_val = cur_offset;
            if let Ok(doc_delta) = VInt::deserialize_u64(&mut self.remaining) {
                let next_doc = cur_doc + doc_delta as DocId;
                self.next_doc = Some(next_doc);
            } else {
                self.next_doc = None;
            }
            Some((cur_doc, cur_offset))
        } else {
            None
        }
    }
}

impl<'a> LayerCursor<'a> {
    fn seek_offset(&mut self, offset: usize, doc: DocId) {
        self.remaining = &self.data[offset..];
        self.next_doc = Some(doc);
        self.inner_id = PERIOD - 1;
    }

    // Returns the last element (key, val)
    // such that (key < doc_id)
    //
    // If there is no such element anymore,
    // returns None.
    //
    // If the element exists, it will be returned
    // at the next call to `.next()`.
    fn seek(&mut self, target: DocId) -> Option<(DocId, u64)> {
        let mut result: Option<(DocId, u64)> = None;
        loop {
            if let Some(next_doc) = self.next_doc {
                if next_doc < target {
                    if let Some(v) = self.next() {
                        result = Some(v);
                        continue;
                    }
                }
            }
            return result;
        }
    }
}

struct Layer {
    data: OwnedBytes,
}

impl Layer {
    fn cursor(&self) -> LayerCursor {
        let mut cursor = self.data.as_slice();
        let next_doc = VInt::deserialize_u64(&mut cursor)
            .ok()
            .map(|doc| doc as DocId);
        LayerCursor {
            remaining: cursor,
            next_doc,
            data: self.data.as_slice(),
            inner_id: 0,
            prev_val: 0,
        }
    }

    fn empty() -> Layer {
        Layer {
            data: OwnedBytes::empty(),
        }
    }
}

pub struct SkipIndex {
    data_layer: Layer,
    skip_layers: Vec<Layer>,
}

impl SkipIndex {
    pub(crate) fn cursor(&self) -> SkipCursor {
        SkipCursor {
            data_layer: self.data_layer.cursor(),
            skip_layers: self
                .skip_layers
                .iter()
                .map(|layer| layer.cursor())
                .collect(),
        }
    }
}

pub struct SkipCursor<'a> {
    data_layer: LayerCursor<'a>,
    skip_layers: Vec<LayerCursor<'a>>,
}

impl<'a> SkipCursor<'a> {
    pub fn seek(&mut self, doc: DocId) -> Option<(DocId, u64)> {
        let mut next_layer_skip: Option<(DocId, u64)> = None;
        for skip_layer in &mut self.skip_layers {
            if let Some((doc, offset)) = next_layer_skip {
                skip_layer.seek_offset(offset as usize, doc);
            }
            next_layer_skip = skip_layer.seek(doc);
        }
        if let Some((doc, offset)) = next_layer_skip {
            self.data_layer.seek_offset(offset as usize, doc);
        }
        self.data_layer.seek(doc)
    }
}

impl<'a> Iterator for SkipCursor<'a> {
    type Item = (DocId, u64);

    fn next(&mut self) -> Option<(DocId, u64)> {
        self.data_layer
            .next()
            .map(|(doc, offset)| (doc as DocId, offset))
    }
}

impl From<OwnedBytes> for SkipIndex {
    fn from(mut data: OwnedBytes) -> SkipIndex {
        let offsets: Vec<u64> = Vec::<VInt>::deserialize(&mut data)
            .unwrap()
            .into_iter()
            .map(|el| el.0)
            .collect();
        let num_layers = offsets.len();
        let layers_data = data.clone();
        let data_layer: Layer = if num_layers == 0 {
            Layer::empty()
        } else {
            Layer {
                data: layers_data.slice(0, offsets[0] as usize),
            }
        };
        let skip_layers = (0..max(1, num_layers) - 1)
            .map(|i| (offsets[i] as usize, offsets[i + 1] as usize))
            .map(|(start, stop)| Layer {
                data: layers_data.slice(start, stop),
            })
            .collect();
        SkipIndex {
            skip_layers,
            data_layer,
        }
    }
}
