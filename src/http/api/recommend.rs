use failure::Error;
use fallible_iterator::{convert, FallibleIterator};
use rouille::{Request, Response};
use uuid::Uuid;

use crate::http::Context;
use crate::storage::{Item, ItemStorage, TimeScope};

pub fn apply(request: &Request, context: &Context) -> Response {
    let request: RecommendRequest = try_or_400!(rouille::input::json_input(request));
    unimplemented!()
}

#[derive(Debug, Serialize, Deserialize)]
struct RecommendRequest {
    #[serde(alias = "p")]
    part: String,
    #[serde(alias = "u")]
    user: Uuid,
    #[serde(alias = "c")]
    current: Uuid,
    #[serde(alias = "w")]
    whitelist: Option<Vec<Uuid>>,
}

impl RecommendRequest {
    #[allow(dead_code)]
    fn items<'c>(
        &'c self,
        context: &'c Context,
    ) -> Result<Box<dyn FallibleIterator<Item = (Item, f64), Error = Error> + 'c>, Error> {
        match self.whitelist {
            Some(ref items) => {
                let items = context
                    .storage
                    .find_items(&self.part, Box::new(items.iter().cloned()))?;
                Ok(Box::new(convert(
                    items.into_iter().flatten().map(|v| Ok((v, 1.0))),
                )))
            }
            None => {
                let base = context.storage.find_items_near(&self.part, self.current)?;
                let with_top_pop = convert(base.items.into_iter().map(Ok))
                    .chain(top_pop_list(&self.part, &context));
                let buf = BufIter::new(with_top_pop, 32).flat_map(move |items| {
                    context
                        .storage
                        .find_items(&self.part, Box::new(items.iter().map(|(v, _)| *v)))
                        .map(|block: Vec<Option<Item>>| {
                            convert(
                                block
                                    .into_iter()
                                    .zip(items.into_iter())
                                    .flat_map(|(item, (_, count))| {
                                        if let Some(item) = item {
                                            Some((item, count))
                                        } else {
                                            None
                                        }
                                    })
                                    .map(Ok),
                            )
                        })
                });

                Ok(Box::new(buf))
            }
        }
    }
}

struct BufIter<I>(Option<I>, usize);

impl<I> BufIter<I> {
    pub fn new(iter: I, buf: usize) -> BufIter<I> {
        BufIter(Some(iter), buf)
    }
}

impl<I> FallibleIterator for BufIter<I>
where
    I: FallibleIterator,
{
    type Item = Vec<<I as FallibleIterator>::Item>;
    type Error = <I as FallibleIterator>::Error;

    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        match &mut self.0 {
            Some(i) => {
                let mut buf = Vec::with_capacity(self.1);
                while buf.len() < self.1 {
                    match i.next() {
                        Ok(Some(v)) => buf.push(v),
                        Ok(None) if !buf.is_empty() => {
                            self.0 = None;
                            return Ok(Some(buf));
                        }
                        Ok(None) => {
                            self.0 = None;
                            return Ok(None);
                        }
                        Err(e) => return Err(e),
                    }
                }

                Ok(Some(buf))
            }
            None => Ok(None),
        }
    }
}

// fn top_pop_list(part: &str, context: &Context) -> impl Iterator<Item = (Uuid, f64)> {
//     lazy_iter(|| {
//         TimeScope::variants().flat_map(|v| {
//             context.storage.find_items_top(part, v).ok()
//         }).zip(TimeScope::variants().flat_map(|v| {
//             context.storage.find_items_popular(part, v).ok()
//         })).flat_map(|(a, b)| a.items.into_iter().chain(b.items.into_iter()))
//     })
// }

fn top_pop_list<'c>(
    part: &'c str,
    context: &'c Context,
) -> impl FallibleIterator<Item = (Uuid, f64), Error = Error> + 'c {
    convert(std::iter::once(Ok(()))).flat_map(move |_| {
        let tops = convert(TimeScope::variants().map(Ok))
            .map(move |v| context.storage.find_items_top(part, v))
            .map(|v| Ok(v.items)); // impl Iterator<Item = Vec<(Uuid, f64)>>
        let pops = convert(TimeScope::variants().map(Ok))
            .map(move |v| context.storage.find_items_popular(part, v))
            .map(|v| Ok(v.items)); // impl Iterator<Item = Vec<(Uuid, f64)>>

        Ok(tops
            .zip(pops)
            .flat_map(move |(a, b)| Ok(convert(a.into_iter().chain(b.into_iter()).map(Ok)))))
    })
}
