use std::error::Error;

use std::io;
// pub type Result = Result;

#[derive(Debug)]
pub enum ReplicatorError {
    Error,
    Io(io::Error),
}

// impl From<io::Error> for CliError {
//     fn from(err: io::Error) -> CliError {
//         CliError::Io(err)
//     }
// }
