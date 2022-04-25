use std::io;

#[derive(Debug)]
pub enum Error {
    Fst(fst::Error),
    IO(io::Error),
}

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
