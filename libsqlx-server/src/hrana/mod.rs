use std::fmt;

pub mod batch;
pub mod http;
pub mod proto;
mod result_builder;
pub mod stmt;
pub mod error;
// pub mod ws;

#[derive(Debug, Copy, Clone, PartialOrd, Ord, PartialEq, Eq)]
pub enum Version {
    Hrana1,
    Hrana2,
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Version::Hrana1 => write!(f, "hrana1"),
            Version::Hrana2 => write!(f, "hrana2"),
        }
    }
}
