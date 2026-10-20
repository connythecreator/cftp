use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use cftp_cli::cli::Cli;
use clap::CommandFactory;
use clap_complete::{Shell, generate};
use clap_mangen::Man;

const BIN_NAME: &str = "cftp";

fn main() -> Result<()> {
    let out = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("packaging/generated"));
    fs::create_dir_all(&out).with_context(|| format!("failed to create {}", out.display()))?;

    let mut command = Cli::command();

    let man_path = out.join("cftp.1");
    let mut man = fs::File::create(&man_path)
        .with_context(|| format!("failed to create {}", man_path.display()))?;
    Man::new(command.clone())
        .render(&mut man)
        .with_context(|| format!("failed to render {}", man_path.display()))?;

    for (shell, file_name) in [
        (Shell::Bash, "cftp.bash"),
        (Shell::Zsh, "_cftp"),
        (Shell::Fish, "cftp.fish"),
    ] {
        let path = out.join(file_name);
        let mut file = fs::File::create(&path)
            .with_context(|| format!("failed to create {}", path.display()))?;
        generate(shell, &mut command, BIN_NAME, &mut file);
    }

    Ok(())
}
