use std;
use reqwest;
use thrift_codec;
use trackable::error::ErrorKindExt;

use {Error, ErrorKind};

pub fn from_io_error(f: std::io::Error) -> Error {
    ErrorKind::Other.cause(f).into()
}

pub fn from_parse_int_error(f: std::num::ParseIntError) -> Error {
    ErrorKind::InvalidInput.cause(f).into()
}

pub fn from_utf8_error(f: std::str::Utf8Error) -> Error {
    ErrorKind::InvalidInput.cause(f).into()
}

pub fn from_thrift_error(f: thrift_codec::Error) -> Error {
    match *f.kind() {
        thrift_codec::ErrorKind::InvalidInput => ErrorKind::InvalidInput.cause(f).into(),
        thrift_codec::ErrorKind::Other => ErrorKind::Other.cause(f).into(),
    }
}

pub fn from_reqwest_error(f: reqwest::Error) -> Error {
    ErrorKind::Other.cause(f).into()
}
