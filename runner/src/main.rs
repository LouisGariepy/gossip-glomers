use std::{env::set_current_dir, path::PathBuf, process::Command};

use anyhow::{anyhow, Context};
use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(author, long_about = None)]
struct Cli {
    /// Which challenge to run
    #[command(subcommand)]
    challenge: Challenge,
}

#[derive(Subcommand)]
enum Challenge {
    Echo,
    UniqueIds,
    BroadcastA,
    BroadcastB,
    BroadcastC,
    BroadcastD {
        #[arg(short, long)]
        partition: bool,
        #[arg(default_value_t = Topology::Tree4)]
        topology: Topology,
    },
    BroadcastE {
        #[arg(short, long)]
        partition: bool,
        #[arg(default_value_t = Topology::Tree4)]
        topology: Topology,
    },
}

impl Challenge {
    fn as_str(&self) -> &'static str {
        match self {
            Challenge::Echo => "echo",
            Challenge::UniqueIds => "unique-ids",
            Challenge::BroadcastA => "broadcast-a",
            Challenge::BroadcastB => "broadcast-b",
            Challenge::BroadcastC => "broadcast-c",
            Challenge::BroadcastD { .. } => "broadcast-d",
            Challenge::BroadcastE { .. } => "broadcast-e",
        }
    }
}

impl std::fmt::Display for Topology {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.as_str(), f)
    }
}

#[derive(Clone, ValueEnum)]
enum Topology {
    Grid,
    Line,
    Total,
    Tree2,
    Tree3,
    Tree4,
}

impl Topology {
    fn as_str(&self) -> &'static str {
        match self {
            Topology::Grid => "grid",
            Topology::Line => "line",
            Topology::Total => "total",
            Topology::Tree2 => "tree2",
            Topology::Tree3 => "tree3",
            Topology::Tree4 => "tree4",
        }
    }
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let binary_name = cli.challenge.as_str();

    Command::new("cargo")
        .arg("build")
        .args(["-p", binary_name])
        .arg("--release")
        .spawn()
        .context("could not build challenges")?
        .wait()
        .context("error while waiting on build process")?;

    let binary_path = PathBuf::from("target/release")
        .join(binary_name)
        .canonicalize()
        .context("could not canonicalize path to binary")?;
    binary_path
        .is_file()
        .then_some(())
        .ok_or(anyhow!("path `target/release/{binary_name}` is not a file"))?;

    let maelstrom_path = PathBuf::from("maelstrom");
    maelstrom_path
        .is_dir()
        .then_some(())
        .ok_or(anyhow!("path `maelstrom` is not a folder"))?;
    set_current_dir("maelstrom").context("could not set current directory")?;

    let mut command = Command::new("./maelstrom");
    match cli.challenge {
        Challenge::Echo => command
            .arg("test")
            .args(["-w", "echo"])
            .args(["--bin", &binary_path.to_string_lossy()])
            .args(["--node-count", "1"])
            .args(["--time-limit", "10"]),
        Challenge::UniqueIds => command
            .arg("test")
            .args(["-w", "unique-ids"])
            .args(["--bin", &binary_path.to_string_lossy()])
            .args(["--node-count", "3"])
            .args(["--time-limit", "30"])
            .args(["--rate", "1000"])
            .args(["--availability", "total"])
            .args(["--nemesis", "partition"]),
        Challenge::BroadcastA => command
            .arg("test")
            .args(["-w", "broadcast"])
            .args(["--bin", &binary_path.to_string_lossy()])
            .args(["--node-count", "1"])
            .args(["--time-limit", "20"])
            .args(["--rate", "10"]),
        Challenge::BroadcastB => command
            .arg("test")
            .args(["-w", "broadcast"])
            .args(["--bin", &binary_path.to_string_lossy()])
            .args(["--node-count", "5"])
            .args(["--time-limit", "20"])
            .args(["--rate", "10"]),
        Challenge::BroadcastC => command
            .arg("test")
            .args(["-w", "broadcast"])
            .args(["--bin", &binary_path.to_string_lossy()])
            .args(["--node-count", "5"])
            .args(["--time-limit", "20"])
            .args(["--rate", "10"])
            .args(["--nemesis", "partition"]),
        Challenge::BroadcastD {
            partition,
            topology,
        }
        | Challenge::BroadcastE {
            partition,
            topology,
        } => {
            let c = command
                .arg("test")
                .args(["-w", "broadcast"])
                .args(["--bin", &binary_path.to_string_lossy()])
                .args(["--node-count", "25"])
                .args(["--time-limit", "20"])
                .args(["--rate", "100"])
                .args(["--latency", "100"])
                .args(["--topology", topology.as_str()]);
            if partition {
                c.args(["--nemesis", "partition"]);
            };
            c
        }
    };

    command
        .spawn()
        .context("failed to spawn challenge run command")?
        .wait()
        .context("error while waiting for run command")?;

    set_current_dir("..").context("could not set current directory")?;

    Ok(())
}
