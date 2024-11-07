mod connection;
mod db;
mod frame;
mod parse;
mod shutdown;

pub mod clients;
pub mod commands;
pub mod constants;
pub mod server;

// Global types
pub type GenericError = Box<dyn std::error::Error + Send + Sync>; // boxed generic error type
pub type FnResult<T> = Result<T, GenericError>;