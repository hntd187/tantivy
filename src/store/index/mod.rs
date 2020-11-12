// #![allow(dead_code)]

const PERIOD: usize = 8;

mod block;
mod skip_index;
mod skip_index_builder;

use crate::DocId;

pub use self::skip_index::SkipIndex;
pub use self::skip_index_builder::SkipIndexBuilder;

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub struct Checkpoint {
    pub first_doc: DocId,
    pub last_doc: DocId,
    pub start_offset: u64,
    pub end_offset: u64
}

#[cfg(test)]
mod tests {

    use std::io;

    use proptest::strategy::{BoxedStrategy, Strategy};

    use crate::directory::OwnedBytes;
    use crate::DocId;
    use crate::store::index::Checkpoint;

    use super::{SkipIndex, SkipIndexBuilder};

    #[test]
    fn test_skip_index_empty() -> io::Result<()> {
        let mut output: Vec<u8> = Vec::new();
        let skip_index_builder: SkipIndexBuilder = SkipIndexBuilder::new();
        skip_index_builder.write(&mut output)?;
        let skip_index: SkipIndex = SkipIndex::from(OwnedBytes::new(output));
        let mut skip_cursor = skip_index.cursor();
        assert!(skip_cursor.next().is_none());
        Ok(())
    }

    #[test]
    fn test_skip_index_single_el() -> io::Result<()> {
        let mut output: Vec<u8> = Vec::new();
        let mut skip_index_builder: SkipIndexBuilder = SkipIndexBuilder::new();
        let checkpoint = Checkpoint {
            first_doc: 0,
            last_doc: 2,
            start_offset: 0,
            end_offset: 3
        };
        skip_index_builder.insert(checkpoint);
        skip_index_builder.write(&mut output)?;
        let skip_index: SkipIndex = SkipIndex::from(OwnedBytes::new(output));
        let mut skip_cursor = skip_index.cursor();
        assert_eq!(skip_cursor.next(), Some(checkpoint));
        assert_eq!(skip_cursor.next(), None);
        Ok(())
    }

    #[test]
    fn test_skip_index3() -> io::Result<()> {
        let mut output: Vec<u8> = Vec::new();
        let checkpoints = vec![
            Checkpoint { first_doc: 0, last_doc: 2, start_offset: 4, end_offset: 9},
            Checkpoint { first_doc: 3, last_doc: 3, start_offset: 9, end_offset: 25},
            Checkpoint { first_doc: 4, last_doc: 5, start_offset: 25, end_offset: 49},
            Checkpoint { first_doc: 6, last_doc: 7, start_offset: 49, end_offset: 81},
            Checkpoint { first_doc: 8, last_doc: 9, start_offset: 81, end_offset: 100}
        ];

        let mut skip_index_builder: SkipIndexBuilder = SkipIndexBuilder::new();
        for &checkpoint in &checkpoints {
            skip_index_builder.insert(checkpoint);

        }
        skip_index_builder.write(&mut output)?;

        let skip_index: SkipIndex = SkipIndex::from(OwnedBytes::new(output));
        assert_eq!(
            &skip_index.cursor().collect::<Vec<_>>()[..],
            &checkpoints[..]
        );
        Ok(())
    }
    fn offset_test(doc: DocId) -> u64 {
        (doc as u64) * (doc as u64)
    }

    #[test]
    fn test_skip_index9() -> io::Result<()> {
        let mut output: Vec<u8> = Vec::new();
        let mut skip_index_builder: SkipIndexBuilder = SkipIndexBuilder::new();
        for i in 0..4 * 4 * 4 {
            skip_index_builder.insert(Checkpoint {
                first_doc: i,
                last_doc: i,
                start_offset: offset_test(i),
                end_offset: offset_test(i + 1)
            });
        }
        skip_index_builder.write(&mut output)?;
        assert_eq!(output.len(), 177);
        assert_eq!(output[0], 131u8);
        Ok(())
    }

    #[test]
    fn test_skip_index10() -> io::Result<()> {
        // checking that void gets serialized to nothing.
        let mut output: Vec<u8> = Vec::new();
        let mut skip_index_builder = SkipIndexBuilder::new();
        for i in 0..((4 * 4 * 4) - 1) {
            skip_index_builder.insert(Checkpoint {
                first_doc: i,
                last_doc: i,
                start_offset: offset_test(i),
                end_offset: offset_test(i + 1)
            });
        }
        skip_index_builder.write(&mut output)?;
        assert_eq!(output.len(), 170);
        assert_eq!(output[0], 130u8);
        Ok(())
    }

    #[test]
    fn test_skip_index11() -> io::Result<()> {
        // checking that void gets serialized to nothing.
        let mut output: Vec<u8> = Vec::new();
        let mut skip_index_builder = SkipIndexBuilder::new();
        for i in 0..(4 * 4) {
            skip_index_builder.insert(Checkpoint {
                first_doc: i,
                last_doc: i,
                start_offset: offset_test(i),
                end_offset: offset_test(i + 1)
            });
        }
        skip_index_builder.write(&mut output)?;
        assert_eq!(output.len(), 45);
        assert_eq!(output[0], 130u8);
        Ok(())
    }

    #[test]
    fn test_skip_index_simple() -> io::Result<()> {
        let mut skip_index_builder = SkipIndexBuilder::new();
        let mut expected = vec![];
        for doc in 0..1000 {
            let start_offset = offset_test(doc);
            let end_offset = offset_test(doc + 1);
            let checkpoint = Checkpoint {
                first_doc: doc,
                last_doc: doc,
                start_offset, end_offset
            };
            skip_index_builder.insert(checkpoint);
            expected.push(checkpoint);
        }
        let mut output: Vec<u8> = Vec::new();
        skip_index_builder.write(&mut output)?;
        let skip_index = SkipIndex::from(OwnedBytes::new(output));
        let skip_cursor = skip_index.cursor();
        let vals = skip_cursor.collect::<Vec<_>>();
        assert_eq!(&vals, &expected);
        Ok(())
    }

    // #[test]
    // fn test_skip_index_long() -> io::Result<()> {
    //     let mut skip_index_builder = SkipIndexBuilder::new();
    //     for doc in (0..1000).map(|doc| doc * 3) {
    //         skip_index_builder.insert(doc, doc as u64, (doc + 3) as u64);
    //     }
    //     let mut output: Vec<u8> = Vec::new();
    //     skip_index_builder.write(&mut output)?;
    //     let skip_index = SkipIndex::from(OwnedBytes::new(output));
    //     for i in 0..2997 {
    //         if i == 0 {
    //             assert_eq!(skip_index.seek(i), Some((0, (0, 3))));
    //         } else {
    //             let first_doc_in_block = i - (i - 1) % 3;
    //             assert_eq!(
    //                 skip_index.seek(i),
    //                 Some((
    //                     first_doc_in_block,
    //                     (first_doc_in_block as u64 + 2, first_doc_in_block as u64 + 5)
    //                 )),
    //                 "Failed for i={}",
    //                 i
    //             );
    //         }
    //     }
    //     Ok(())
    // }

    fn integrate_delta(mut vals: Vec<u64>) -> Vec<u64> {
        let mut prev = 0u64;
        for val in vals.iter_mut() {
            let new_val = *val + prev;
            prev = new_val;
            *val = new_val;
        }
        vals
    }

    fn monotonic(max_len: usize) -> BoxedStrategy<Vec<Checkpoint>> {
        (1..max_len)
            .prop_flat_map(move |len: usize| {
                (
                    proptest::collection::vec(1u64..260u64, len as usize).prop_map(integrate_delta),
                    proptest::collection::vec(1u64..260u64, len  as usize)
                        .prop_map(integrate_delta),
                )
                    .prop_map(|(docs, offsets)| {
                        (0..docs.len() - 1)
                            .map(move |i| { 
                                Checkpoint {
                                    first_doc: docs[i] as DocId,
                                    last_doc: (docs[i + 1] - 1) as DocId,
                                    start_offset: offsets[i],
                                    end_offset: offsets[i+1],
                                }
                            })
                            .collect::<Vec<Checkpoint>>()
                    })
            })
            .boxed()
    }

    fn seek_manual<I: Iterator<Item = Checkpoint>>(
        checkpoints: I,
        target: DocId,
    ) -> Option<Checkpoint> {
        for checkpoint in checkpoints {
            if checkpoint.last_doc >= target {
                return Some(checkpoint);
            }
        }
        None
    }

    fn test_skip_index_aux(skip_index: SkipIndex, checkpoints: &[Checkpoint]) {
        if let Some(checkpoint) = checkpoints.last() {
            for doc in 0u32..checkpoint.last_doc + 1 {
                let expected = seek_manual(skip_index.cursor(), doc);
                assert_eq!(expected, skip_index.seek(doc), "Doc {}", doc);
            }
        }
    }

    use proptest::proptest;

    proptest! {
         #[test]
         fn test_proptest_skip(checkpoints in monotonic(15)) {
             let mut skip_index_builder = SkipIndexBuilder::new();
             for checkpoint in checkpoints.iter().cloned() {
                 skip_index_builder.insert(checkpoint);
             }
             let mut buffer = Vec::new();
             skip_index_builder.write(&mut buffer).unwrap();
             let skip_index = SkipIndex::from(OwnedBytes::new(buffer));
             let iter_checkpoints: Vec<Checkpoint> = skip_index.cursor().collect();
             assert_eq!(&checkpoints[..], &iter_checkpoints[..]);
             test_skip_index_aux(skip_index, &checkpoints[..]);
         }
    }
}
