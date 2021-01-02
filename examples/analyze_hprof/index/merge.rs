use crate::index::*;
use std::iter;

/// Repeatedly do a parallel n-way merge of sorted chunk files until there's only 1 file.
/// Total items/sec falls off precipitously when merging more than 8 sources:
/// 4x -> 49m / sec, 8 -> 41m, 16 -> 29, 32 -> 20, 64 -> 12, 128 -> 6
/// So, it's faster to do a few rounds of 8-way merge than one slower 128-way (or worse).
pub(crate) fn merge_chunk_type<T, W: DatumDeserializer<T> + DatumSerializer<T> + Send + Sync>(
    index_dir: &path::Path,
    subdir: &str,
) -> Result<path::PathBuf, anyhow::Error> {
    let mut chunks_dir = index_dir.to_path_buf();
    chunks_dir.push("chunks");
    chunks_dir.push(subdir);

    let mut merge_root_dir = index_dir.to_path_buf();
    merge_root_dir.push("merge");
    merge_root_dir.push(subdir);

    let mut files_to_merge = fs::read_dir(path::PathBuf::from(chunks_dir))?
        .map(|r| r.unwrap())
        .collect_vec();

    let mut counter = 0;
    loop {
        if files_to_merge.len() == 1 {
            // we have just one ordered file; ready to move to the next stage
            return Ok(files_to_merge[0].path());
        }
        if files_to_merge.is_empty() {
            panic!("Should always have at least one chunk");
        }

        let merge_factor = 8;
        println!(
            "Merge round {}, {} merged files to write",
            counter,
            files_to_merge.as_slice().chunks(merge_factor).count()
        );

        let mut merge_dir = merge_root_dir.to_path_buf();
        merge_dir.push(format!("{:02}", counter));
        fs::create_dir_all(&merge_dir)?;

        files_to_merge
            .as_slice()
            .chunks(merge_factor)
            .enumerate()
            .par_bridge()
            .panic_fuse()
            .map(|(chunk_index, files_to_merge_at_this_step)| {
                // iterators for all the chunks from the previous merge step that are about to be
                // merged into one
                let iters: Result<Vec<_>, _> = files_to_merge_at_this_step
                    .iter()
                    .map(|d| {
                        fs::File::open(d.path())
                            .map(|f| io::BufReader::new(f))
                            .map(|r| {
                                ChunkDatumIterator::<_, _, W>::new(r)
                                    // TODO bubble Result up from this layer
                                    .map(|r| r.unwrap())
                            })
                    })
                    .collect();

                let merged = MergeSortIterator::new(iters?, |elem| W::extract_key(elem));

                let mut merged_output = merge_dir.clone();
                merged_output.push(format!("chunk-{:03}", chunk_index));

                let mut writer = io::BufWriter::new(fs::File::create(&merged_output).unwrap());

                for datum in merged {
                    W::serialize(&datum, &mut writer)?;
                }

                writer.flush()?;

                print!(".");
                io::stdout().flush()?;

                // ensure output is sorted
                assert!(
                    // use UFCS to use crate's version instead of unstable stdlib is_sorted_by_key
                    IsSorted::is_sorted_by_key(
                        &mut ChunkDatumIterator::<_, _, W>::new(io::BufReader::new(
                            fs::File::open(&merged_output,)?
                        ))
                        .map(|res| res.unwrap()),
                        |datum| W::extract_key(datum),
                    ),
                    "{:?} was not sorted",
                    &merged_output
                );

                Ok(())
            })
            .for_each(|r: Result<(), anyhow::Error>| r.unwrap());

        // remove the files from the previous round that have now been merged
        for de in files_to_merge.iter() {
            fs::remove_file(de.path())?
        }

        println!();

        // now, entries are what we just merged
        files_to_merge = fs::read_dir(&merge_dir)?.map(|r| r.unwrap()).collect();
        counter += 1;
    }
}

/// An Iterator for merging already sorted iterators into one sorted iterator, smallest item first.
///
/// If used on not-sorted iterators, the output order is undefined.
pub(crate) struct MergeSortIterator<T, I: Iterator<Item = T>, O: Ord, K: Fn(&T) -> O> {
    iterators: Vec<iter::Peekable<I>>,
    key_extractor: K,
}

impl<T, I: Iterator<Item = T>, O: Ord, K: Fn(&T) -> O> MergeSortIterator<T, I, O, K> {
    pub(crate) fn new<II: IntoIterator<Item = T, IntoIter = I>>(
        iterators: Vec<II>,
        key_extractor: K,
    ) -> MergeSortIterator<T, I, O, K> {
        MergeSortIterator {
            iterators: iterators
                .into_iter()
                .map(|i| i.into_iter().peekable())
                .collect(),
            key_extractor,
        }
    }
}

impl<T, I: Iterator<Item = T>, O: Ord, K: Fn(&T) -> O> Iterator for MergeSortIterator<T, I, O, K> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        // if we get here, that means that there was at least one iterator that produced a
        // Some, and this is the index (and element) of the iterator that produced the smallest
        // element
        let extr = &self.key_extractor;
        let (iter_index, _elem) = self
            .iterators
            .iter_mut()
            .enumerate()
            .filter_map(|(index, iter)| iter.peek().map(|elem| (index, elem)))
            .min_by_key(|(_index, elem)| (extr)(elem))?;

        // that was only a peek, so we need to actually advance that iterator
        self.iterators[iter_index].next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::Itertools;
    use rand;
    use rand::{distributions, distributions::Distribution, Rng};

    #[test]
    fn merged_iterator_works_random() {
        let mut rng = rand::thread_rng();
        for iteration in 0..1000 {
            let vecs = (0..distributions::Uniform::from(0_usize..20).sample(&mut rng))
                .map(|_| {
                    let len = distributions::Uniform::from(0_usize..100).sample(&mut rng);
                    let mut vec = (0..len).map(|_| rng.gen::<u64>()).collect_vec();
                    vec.sort();
                    vec
                })
                .collect_vec();

            let mut all_data = vecs.iter().flat_map(|v| v.iter()).map(|&n| n).collect_vec();
            all_data.sort();

            let merged_iter = MergeSortIterator::new(vecs, |&num| num);

            assert_eq!(
                all_data,
                merged_iter.collect_vec(),
                "iteration {}",
                iteration
            );
        }
    }
}
