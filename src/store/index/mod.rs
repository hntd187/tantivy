// #![allow(dead_code)]

const PERIOD: usize = 8;

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
    fn test_skip_index() -> io::Result<()> {
        let mut output: Vec<u8> = Vec::new();
        let mut skip_index_builder: SkipIndexBuilder = SkipIndexBuilder::new();
        skip_index_builder.insert(2, 3);
        skip_index_builder.write(&mut output)?;
        let skip_index: SkipIndex = SkipIndex::from(OwnedBytes::new(output));
        let mut skip_cursor = skip_index.cursor();
        assert_eq!(skip_cursor.next(), Some((2, 3)));
        Ok(())
    }

    #[test]
    fn test_skip_index2() -> io::Result<()> {
        let mut output: Vec<u8> = Vec::new();
        let skip_index_builder: SkipIndexBuilder = SkipIndexBuilder::new();
        skip_index_builder.write(&mut output)?;
        let skip_index: SkipIndex = SkipIndex::from(OwnedBytes::new(output));
        let mut skip_cursor = skip_index.cursor();
        assert!(skip_cursor.next().is_none());
        Ok(())
    }

    #[test]
    fn test_skip_index3() -> io::Result<()> {
        let mut output: Vec<u8> = Vec::new();
        let mut skip_index_builder: SkipIndexBuilder = SkipIndexBuilder::new();
        skip_index_builder.insert(2, 4);
        skip_index_builder.insert(3, 9);
        skip_index_builder.insert(5, 25);
        skip_index_builder.insert(7, 49);
        skip_index_builder.insert(9, 81);
        skip_index_builder.write(&mut output)?;
        let skip_index: SkipIndex = SkipIndex::from(OwnedBytes::new(output));
        assert_eq!(
            skip_index.cursor().collect::<Vec<_>>(),
            vec![(2, 4), (3, 9), (5, 25), (7, 49), (9, 81)]
        );
        Ok(())
    }

    #[test]
    fn test_skip_index4() -> io::Result<()> {
        let mut output: Vec<u8> = Vec::new();
        let mut skip_index_builder: SkipIndexBuilder = SkipIndexBuilder::new();
        skip_index_builder.insert(2, 4);
        skip_index_builder.insert(3, 9);
        skip_index_builder.insert(5, 25);
        skip_index_builder.insert(7, 49);
        skip_index_builder.insert(9, 81);
        skip_index_builder.write(&mut output)?;
        let skip_index: SkipIndex = SkipIndex::from(OwnedBytes::new(output));
        let mut skip_cursor = skip_index.cursor();
        assert_eq!(skip_cursor.next(), Some((2, 4)));
        assert_eq!(skip_cursor.seek(5), Some((3, 9)));
        assert_eq!(
            skip_cursor.collect::<Vec<_>>(),
            vec![(5, 25), (7, 49), (9, 81)]
        );
        Ok(())
    }

    #[test]
    fn test_skip_index5() -> io::Result<()> {
        let mut output: Vec<u8> = Vec::new();
        let mut skip_index_builder: SkipIndexBuilder = SkipIndexBuilder::new();
        skip_index_builder.insert(2, 4);
        skip_index_builder.insert(3, 9);
        skip_index_builder.insert(5, 25);
        skip_index_builder.insert(6, 36);
        skip_index_builder.write(&mut output)?;
        let skip_index: SkipIndex = SkipIndex::from(OwnedBytes::new(output));
        let mut skip_cursor = skip_index.cursor();
        assert_eq!(skip_cursor.next(), Some((2, 4)));
        assert_eq!(skip_cursor.seek(6), Some((5, 25)));
        assert_eq!(skip_cursor.next(), Some((6, 36)));
        assert_eq!(skip_cursor.next(), None);
        Ok(())
    }

    #[test]
    fn test_skip_index6() -> io::Result<()> {
        let mut output: Vec<u8> = Vec::new();
        let mut skip_index_builder: SkipIndexBuilder = SkipIndexBuilder::new();
        skip_index_builder.insert(2, 4);
        skip_index_builder.insert(3, 9);
        skip_index_builder.insert(5, 25);
        skip_index_builder.insert(7, 49);
        skip_index_builder.insert(9, 81);
        skip_index_builder.write(&mut output)?;
        let skip_index = SkipIndex::from(OwnedBytes::new(output));
        let mut skip_cursor = skip_index.cursor();
        assert_eq!(skip_cursor.next(), Some((2, 4)));
        assert_eq!(skip_cursor.seek(10), Some((9, 81)));
        assert_eq!(skip_cursor.next(), None);
        Ok(())
    }

    fn offset_test(doc: DocId) -> u64 {
        (doc as u64) * (doc as u64)
    }

    #[test]
    fn test_skip_index7() -> io::Result<()> {
        let mut output: Vec<u8> = Vec::new();
        let mut skip_index_builder = SkipIndexBuilder::new();
        for i in 0..1000 {
            skip_index_builder.insert(i, offset_test(i));
        }
        skip_index_builder.insert(1004, 1004 * 1004);
        skip_index_builder.write(&mut output)?;
        let skip_index = SkipIndex::from(OwnedBytes::new(output));
        let mut skip_cursor = skip_index.cursor();
        assert_eq!(skip_cursor.next(), Some((0, 0)));
        skip_cursor.seek(431);
        assert_eq!(skip_cursor.next(), Some((431, 431 * 431)));
        skip_cursor.seek(1003);
        assert_eq!(skip_cursor.next(), Some((1004, 1004 * 1004)));
        assert_eq!(skip_cursor.next(), None);
        Ok(())
    }

    #[test]
    fn test_skip_index8() -> io::Result<()> {
        let mut output: Vec<u8> = Vec::new();
        let mut skip_index_builder = SkipIndexBuilder::new();
        skip_index_builder.insert(2, 3);
        skip_index_builder.write(&mut output)?;
        assert_eq!(output.len(), 4);
        assert_eq!(output[0], 1u8 + 128u8);
        Ok(())
    }

    #[test]
    fn test_skip_index9() -> io::Result<()> {
        let mut output: Vec<u8> = Vec::new();
        let mut skip_index_builder: SkipIndexBuilder = SkipIndexBuilder::new();
        for i in 0..4 * 4 * 4 {
            skip_index_builder.insert(i, offset_test(i));
        }
        skip_index_builder.write(&mut output)?;
        assert_eq!(output.len(), 161);
        assert_eq!(output[0], 131u8);
        Ok(())
    }

    #[test]
    fn test_skip_index10() -> io::Result<()> {
        // checking that void gets serialized to nothing.
        let mut output: Vec<u8> = Vec::new();
        let mut skip_index_builder = SkipIndexBuilder::new();
        for i in 0..((4 * 4 * 4) - 1) {
            skip_index_builder.insert(i, offset_test(i));
        }
        skip_index_builder.write(&mut output)?;
        assert_eq!(output.len(), 151);
        assert_eq!(output[0], 130u8);
        Ok(())
    }

    #[test]
    fn test_skip_index11() -> io::Result<()> {
        // checking that void gets serialized to nothing.
        let mut output: Vec<u8> = Vec::new();
        let mut skip_index_builder = SkipIndexBuilder::new();
        for i in 0..(4 * 4) {
            skip_index_builder.insert(i, offset_test(i));
        }
        skip_index_builder.write(&mut output)?;
        assert_eq!(output.len(), 40);
        assert_eq!(output[0], 130u8);
        Ok(())
    }

    #[test]
    fn test_skip_index_simple() -> io::Result<()> {
        let mut output: Vec<u8> = Vec::new();
        let mut skip_index_builder = SkipIndexBuilder::new();
        let mut expected = vec![];
        for doc in 0..1000 {
            let offset = offset_test(doc);
            skip_index_builder.insert(doc, offset);
            expected.push((doc, offset));
        }
        skip_index_builder.write(&mut output)?;
        let skip_index = SkipIndex::from(OwnedBytes::new(output));
        let skip_cursor = skip_index.cursor();
        let vals = skip_cursor.collect::<Vec<_>>();
        assert_eq!(&vals, &expected);
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

    fn monotonic(max_len: usize) -> BoxedStrategy<Vec<(u32, u64)>> {
        (1..max_len)
            .prop_flat_map(move |len: usize| {
                (
                    proptest::collection::vec(1u64..260u64, len as usize).prop_map(integrate_delta),
                    proptest::collection::vec(1u64..260u64, len as usize).prop_map(integrate_delta),
                )
                    .prop_map(|(docs, offsets)| {
                        docs.into_iter()
                            .zip(offsets.into_iter())
                            .map(|(doc, offset)| (doc as DocId, offset))
                            .collect::<Vec<(DocId, u64)>>()
                    })
            })
            .boxed()
    }

    fn seek_manual<I: Iterator<Item = (DocId, u64)>>(
        doc_vals: I,
        target: DocId,
    ) -> Option<(DocId, u64)> {
        let mut res = None;
        for (doc, val) in doc_vals {
            if doc >= target {
                break;
            }
            res = Some((doc, val));
        }
        res
    }

    fn test_skip_index_aux(skip_index: SkipIndex, doc_offsets: &[(DocId, u64)]) {
        if let Some((last_doc, _)) = doc_offsets.last() {
            for doc in 0u32..*last_doc + 1 {
                let expected = seek_manual(skip_index.cursor(), doc);
                assert_eq!(expected, skip_index.cursor().seek(doc), "Doc {}", doc);
            }
        }
    }

    use proptest::proptest;

    proptest! {
         #[test]
         fn test_proptest_skip(doc_offsets in monotonic(15)) {
             let mut skip_index_builder = SkipIndexBuilder::new();
             for (doc, val) in &doc_offsets {
                 skip_index_builder.insert(*doc, *val);
             }
             let mut buffer = Vec::new();
             skip_index_builder.write(&mut buffer).unwrap();
             let skip_index = SkipIndex::from(OwnedBytes::new(buffer));
             let vals: Vec<(u32, u64)> = skip_index.cursor().collect();
             assert_eq!(&vals[..], &doc_offsets[..]);
             test_skip_index_aux(skip_index, &doc_offsets[..]);
         }
    }
}
