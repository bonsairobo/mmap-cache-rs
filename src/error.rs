use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Fst(#[from] fst::Error),
    #[error(transparent)]
    IO(#[from] io::Error),
}
