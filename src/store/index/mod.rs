// #![allow(dead_code)]

const PERIOD: usize = 8;

mod block;
mod skip_index;
mod skip_index_builder;

pub use self::skip_index::SkipIndex;
pub use self::skip_index_builder::SkipIndexBuilder;

#[cfg(test)]
mod tests {

    use std::io;

    use proptest::strategy::{BoxedStrategy, Strategy};

    use crate::directory::OwnedBytes;
    use crate::DocId;

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
        skip_index_builder.insert(2, 0, 3);
        skip_index_builder.write(&mut output)?;
        let skip_index: SkipIndex = SkipIndex::from(OwnedBytes::new(output));
        let mut skip_cursor = skip_index.cursor();
        assert_eq!(skip_cursor.next(), Some((2, (0, 3))));
        assert_eq!(skip_cursor.next(), None);
        Ok(())
    }

    #[test]
    fn test_skip_index3() -> io::Result<()> {
        let mut output: Vec<u8> = Vec::new();
        let mut skip_index_builder: SkipIndexBuilder = SkipIndexBuilder::new();
        skip_index_builder.insert(2, 4, 9);
        skip_index_builder.insert(3, 9, 25);
        skip_index_builder.insert(5, 25, 49);
        skip_index_builder.insert(7, 49, 81);
        skip_index_builder.insert(9, 81, 100);
        skip_index_builder.write(&mut output)?;
        let skip_index: SkipIndex = SkipIndex::from(OwnedBytes::new(output));
        assert_eq!(
            skip_index.cursor().collect::<Vec<_>>(),
            vec![
                (2, (4, 9)),
                (3, (9, 25)),
                (5, (25, 49)),
                (7, (49, 81)),
                (9, (81, 100))
            ]
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
            skip_index_builder.insert(i, offset_test(i), offset_test(i + 1));
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
            skip_index_builder.insert(i, offset_test(i), offset_test(i + 1));
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
            skip_index_builder.insert(i, offset_test(i), offset_test(i + 1));
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
            skip_index_builder.insert(doc, start_offset, end_offset);
            expected.push((doc, (start_offset, end_offset)));
        }
        let mut output: Vec<u8> = Vec::new();
        skip_index_builder.write(&mut output)?;
        let skip_index = SkipIndex::from(OwnedBytes::new(output));
        let skip_cursor = skip_index.cursor();
        let vals = skip_cursor.collect::<Vec<_>>();
        assert_eq!(&vals, &expected);
        Ok(())
    }

    #[test]
    fn test_skip_index_long() -> io::Result<()> {
        let mut skip_index_builder = SkipIndexBuilder::new();
        for doc in (0..1000).map(|doc| doc * 3) {
            skip_index_builder.insert(doc, doc as u64, (doc + 3) as u64);
        }
        let mut output: Vec<u8> = Vec::new();
        skip_index_builder.write(&mut output)?;
        let skip_index = SkipIndex::from(OwnedBytes::new(output));
        for i in 0..2997 {
            if i == 0 {
                assert_eq!(skip_index.seek(i), Some((0, (0, 3))));
            } else {
                let first_doc_in_block = i - (i - 1) % 3;
                assert_eq!(
                    skip_index.seek(i),
                    Some((
                        first_doc_in_block,
                        (first_doc_in_block as u64 + 2, first_doc_in_block as u64 + 5)
                    )),
                    "Failed for i={}",
                    i
                );
            }
        }
        Ok(())
    }

    fn integrate_delta(mut vals: Vec<u64>) -> Vec<u64> {
        let mut prev = 0u64;
        for val in vals.iter_mut() {
            let new_val = *val + prev;
            prev = new_val;
            *val = new_val;
        }
        vals
    }

    fn monotonic(max_len: usize) -> BoxedStrategy<Vec<(DocId, (u64, u64))>> {
        (1..max_len)
            .prop_flat_map(move |len: usize| {
                (
                    proptest::collection::vec(1u64..260u64, len as usize).prop_map(integrate_delta),
                    proptest::collection::vec(1u64..260u64, len + 1 as usize)
                        .prop_map(integrate_delta),
                )
                    .prop_map(|(docs, offsets)| {
                        docs.into_iter()
                            .enumerate()
                            .map(|(i, doc)| (doc as DocId, (offsets[i], offsets[i + 1])))
                            .collect::<Vec<_>>()
                    })
            })
            .boxed()
    }

    fn seek_manual<I: Iterator<Item = (DocId, (u64, u64))>>(
        doc_vals: I,
        target: DocId,
    ) -> Option<(DocId, (u64, u64))> {
        let mut first_doc = 0;
        for (last_doc, block_offset) in doc_vals {
            if last_doc >= target {
                return Some((first_doc, block_offset));
            } else {
                first_doc = last_doc + 1;
            }
        }
        None
    }

    fn test_skip_index_aux(skip_index: SkipIndex, doc_offsets: &[(DocId, (u64, u64))]) {
        if let Some((last_doc, _)) = doc_offsets.last() {
            for doc in 0u32..*last_doc + 1 {
                let expected = seek_manual(skip_index.cursor(), doc);
                assert_eq!(expected, skip_index.seek(doc), "Doc {}", doc);
            }
        }
    }

    use proptest::proptest;

    proptest! {
         #[test]
         fn test_proptest_skip(doc_offsets in monotonic(15)) {
             let mut skip_index_builder = SkipIndexBuilder::new();
             for (doc, (start_offset, end_offset)) in doc_offsets.iter().cloned() {
                 skip_index_builder.insert(doc, start_offset, end_offset);
             }
             let mut buffer = Vec::new();
             skip_index_builder.write(&mut buffer).unwrap();
             let skip_index = SkipIndex::from(OwnedBytes::new(buffer));
             let vals: Vec<(DocId, (u64, u64))> = skip_index.cursor().collect();
             assert_eq!(&vals[..], &doc_offsets[..]);
             test_skip_index_aux(skip_index, &doc_offsets[..]);
         }
    }
}
