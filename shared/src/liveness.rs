//! Liveness heartbeat for a service with no port to probe.
//!
//! The monitor binaries expose no socket, so a container healthcheck has nothing
//! to ask. `restart: unless-stopped` then only covers a process that exits: one
//! that is running but no longer doing its work is invisible. A file whose
//! modification time is refreshed while the service is working closes that gap,
//! and a healthcheck compares that time against now.
//!
//! The time is the whole signal, so the file's contents are irrelevant and it is
//! truncated rather than appended to. Failing to write it is a warning: the
//! service is working, and a monitor of the monitor must not be able to stop it.

use std::fs;
use std::path::{Path, PathBuf};

/// Refreshes a file's modification time to say the service is still working.
///
/// Disabled when no path is configured, which is how a run without a
/// healthcheck avoids writing anything at all.
#[derive(Clone, Debug, Default)]
pub struct Heartbeat {
    path: Option<PathBuf>,
}

impl Heartbeat {
    /// Build a heartbeat, disabled when `path` is `None`.
    pub const fn new(path: Option<PathBuf>) -> Self {
        Self { path }
    }

    /// Record that the service just completed a unit of its work.
    ///
    /// Call this from something that only succeeds when the service is healthy,
    /// not from a timer alone: a heartbeat that beats regardless of progress
    /// reports a wedged process as healthy.
    pub fn beat(&self) {
        let Some(path) = self.path.as_deref() else {
            return;
        };
        if let Err(error) = write_now(path) {
            tracing::warn!(
                path = %path.display(),
                %error,
                "could not refresh the liveness file; the service itself is unaffected"
            );
        }
    }

    /// Path being refreshed, for diagnostics.
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }
}

fn write_now(path: &Path) -> std::io::Result<()> {
    // Truncating a tiny file is one syscall more than `utimensat` and does not
    // need the file to exist yet, which is the common case on a fresh tmpfs.
    fs::write(path, b"ok\n")
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::Heartbeat;

    #[test]
    fn a_disabled_heartbeat_writes_nothing() {
        // The default is off, so a deployment without a healthcheck does not
        // need a writable path at all.
        Heartbeat::new(None).beat();
        assert!(Heartbeat::new(None).path().is_none());
    }

    #[test]
    fn a_beat_creates_the_file_and_then_refreshes_it() {
        let directory = std::env::temp_dir().join(format!(
            "bip300-liveness-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        fs::create_dir_all(&directory).expect("create the test directory");
        let path = directory.join("liveness");

        let heartbeat = Heartbeat::new(Some(path.clone()));
        heartbeat.beat();
        let first = fs::metadata(&path)
            .expect("the first beat creates the file")
            .modified()
            .expect("a modification time");

        heartbeat.beat();
        let second = fs::metadata(&path)
            .expect("the file survives")
            .modified()
            .expect("a modification time");
        assert!(second >= first, "a beat never moves the time backwards");

        fs::remove_dir_all(&directory).expect("clean up the test directory");
    }

    #[test]
    fn an_unwritable_path_is_a_warning_rather_than_a_panic() {
        // A misconfigured healthcheck must not be able to stop the service.
        Heartbeat::new(Some(
            std::env::temp_dir().join("bip300-liveness-missing-dir/liveness"),
        ))
        .beat();
    }
}
