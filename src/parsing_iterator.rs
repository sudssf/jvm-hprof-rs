use crate::*;
use std::marker;

/// Common "iterate over n things that need id size" pattern
pub struct ParsingIterator<'a, T, P: Parser<T>> {
    parser: P,
    num_remaining: u32,
    remaining: &'a [u8],
    phantom: marker::PhantomData<T>,
}

impl<'a, S: StatelessParserWithId> ParsingIterator<'a, S, IdSizeParserWrapper<S>> {
    pub fn new_stateless_id_size(
        id_size: IdSize,
        remaining: &'a [u8],
        num_remaining: u32,
    ) -> ParsingIterator<'a, S, IdSizeParserWrapper<S>> {
        ParsingIterator {
            parser: IdSizeParserWrapper::<S>::new(id_size),
            num_remaining,
            remaining,
            phantom: marker::PhantomData,
        }
    }
}

impl<'a, S: StatelessParser> ParsingIterator<'a, S, StatelessParserWrapper<S>> {
    pub fn new_stateless(
        remaining: &'a [u8],
        num_remaining: u32,
    ) -> ParsingIterator<'a, S, StatelessParserWrapper<S>> {
        ParsingIterator {
            parser: StatelessParserWrapper::<S>::new(),
            num_remaining,
            remaining,
            phantom: marker::PhantomData,
        }
    }
}

impl<'a, T, P: Parser<T>> Iterator for ParsingIterator<'a, T, P> {
    type Item = ParseResult<'a, T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.num_remaining == 0 {
            debug_assert_eq!(0, self.remaining.len());
            return None;
        }

        let res = self.parser.parse(self.remaining);

        match res {
            Ok((input, val)) => {
                self.remaining = input;
                self.num_remaining -= 1;
                Some(Ok(val))
            }
            Err(e) => Some(Err(e)),
        }
    }
}

/// A parser that needs state (id size, primitive type, etc).
/// Used with `ParsingIterator` to handle the common iterate-and-parse pattern.
pub trait Parser<T>: Sized {
    fn parse<'a>(&self, input: &'a [u8]) -> nom::IResult<&'a [u8], T>;
}

/// Convenience for simpler types to avoid needing a separate struct
pub trait StatelessParser: Sized {
    fn parse(input: &[u8]) -> nom::IResult<&[u8], Self>;
}

/// A shortcut for the common case of deserializing something that needs id size
pub trait StatelessParserWithId: Sized {
    fn parse(input: &[u8], id_size: IdSize) -> nom::IResult<&[u8], Self>;
}

/// Adapt `StatelessParserWithId` into a `Parser`
pub struct IdSizeParserWrapper<P: StatelessParserWithId> {
    id_size: IdSize,
    phantom: marker::PhantomData<P>,
}

impl<P: StatelessParserWithId> IdSizeParserWrapper<P> {
    pub fn new(id_size: IdSize) -> IdSizeParserWrapper<P> {
        IdSizeParserWrapper {
            id_size,
            phantom: marker::PhantomData,
        }
    }
}

impl<P: StatelessParserWithId> Parser<P> for IdSizeParserWrapper<P> {
    fn parse<'a>(&self, input: &'a [u8]) -> nom::IResult<&'a [u8], P> {
        P::parse(input, self.id_size)
    }
}

/// Adapt a `StatelessParser` into a `Parser`
pub struct StatelessParserWrapper<P: StatelessParser> {
    phantom: marker::PhantomData<P>,
}

impl<P: StatelessParser> StatelessParserWrapper<P> {
    pub fn new() -> StatelessParserWrapper<P> {
        StatelessParserWrapper {
            phantom: marker::PhantomData,
        }
    }
}

impl<P: StatelessParser> Parser<P> for StatelessParserWrapper<P> {
    fn parse<'a>(&self, input: &'a [u8]) -> nom::IResult<&'a [u8], P> {
        P::parse(input)
    }
}
