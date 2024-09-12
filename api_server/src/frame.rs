//! Provides a type representing a message frame as well as utilities for
//! parsing frames from a byte array.

use crate::message::MessageHeader;
use bytes::{Buf, Bytes};
use std::fmt;
use std::io::{self, Cursor};

#[derive(Clone)]
pub struct Frame {
    pub header: MessageHeader,
    pub body: Bytes,
}

#[derive(Debug)]
pub enum Error {
    /// Not enough data is available to parse a message
    Incomplete,

    /// Invalid message encoding
    Other(io::Error),
}

impl Frame {
    /// Checks if an entire message can be decoded from `src`
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        let header_size = MessageHeader::encode_len();
        if src.remaining() <= header_size {
            return Err(Error::Incomplete);
        }
        let size = MessageHeader::get_size(src);
        if src.remaining() < size {
            return Err(Error::Incomplete);
        }
        src.set_position(size as u64);
        Ok(())
    }

    /// The message has already been validated with `check`.
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        let header_size = MessageHeader::encode_len();
        let header = MessageHeader::decode(src);
        let body = Bytes::copy_from_slice(&src.chunk()[header_size..header.size as usize]);
        Ok(Self { header, body })
    }
}

#[allow(unused)]
fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
    if src.remaining() < n {
        return Err(Error::Incomplete);
    }

    src.advance(n);
    Ok(())
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(fmt),
            Error::Other(err) => err.fmt(fmt),
        }
    }
}
