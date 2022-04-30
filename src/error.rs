use std::io;

#[derive(Debug)]
pub enum Error {
    Fst(fst::Error),
    IO(io::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fst(e) => e.fmt(f),
            Self::IO(e) => e.fmt(f),
        }
    }
}
impl std::error::Error for Error {}

impl From<fst::Error> for Error {
    fn from(e: fst::Error) -> Self {
        Self::Fst(e)
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::IO(e)
    }
}
