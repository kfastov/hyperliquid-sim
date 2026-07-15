mod config;

use config::Config;
use std::process::ExitCode;

fn main() -> ExitCode {
    match Config::from_env() {
        Ok(config) => {
            println!("{}", config.validation_summary());
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("sim-server configuration invalid: {error}");
            ExitCode::from(2)
        }
    }
}
