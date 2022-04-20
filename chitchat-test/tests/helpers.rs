// Copyright (C) 2021 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::path::PathBuf;
use std::process::{Child, Stdio};
use std::{env, io};

// Adapted from
// https://github.com/rust-lang/cargo/blob/485670b3983b52289a2f353d589c57fae2f60f82/tests/testsuite/support/mod.rs#L507
fn target_dir() -> PathBuf {
    env::current_exe()
        .ok()
        .map(|mut path| {
            path.pop();
            if path.ends_with("deps") {
                path.pop();
            }
            path
        })
        .unwrap()
}

/// Look up the path to a cargo-built binary within an integration test.
pub fn cargo_bin<S: AsRef<str>>(name: S) -> PathBuf {
    cargo_bin_str(name.as_ref())
}

fn cargo_bin_str(name: &str) -> PathBuf {
    let env_var = format!("CARGO_BIN_EXE_{}", name);
    std::env::var_os(&env_var)
        .map(|p| p.into())
        .unwrap_or_else(|| target_dir().join(format!("{}{}", name, env::consts::EXE_SUFFIX)))
}

/// Creates a `chitchat-test` command with provided list of arguments.
pub fn spawn_command(arguments: &str) -> io::Result<Child> {
    std::process::Command::new(cargo_bin("chitchat-test"))
        .args(arguments.split_whitespace())
        .stdout(Stdio::piped())
        .spawn()
}
