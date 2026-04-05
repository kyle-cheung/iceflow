pub mod commands;
pub mod config;

use anyhow::{Error, Result};
use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cli;

impl Cli {
    pub fn command() -> CommandSpec {
        CommandSpec::new("iceflow")
            .with_subcommand(CommandSpec::new("run"))
            .with_subcommand(CommandSpec::new("compact"))
            .with_subcommand(CommandSpec::new("source").with_subcommand(CommandSpec::new("check")))
    }

    pub fn parse_from<I, S>(args: I) -> Result<Commands>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut args = args.into_iter().map(Into::into);
        let _program = args.next();
        let Some(subcommand) = args.next() else {
            return Err(Error::msg("expected a subcommand"));
        };

        let remaining: Vec<String> = args.collect();

        match subcommand.as_str() {
            "run" => Ok(Commands::Run(commands::run::Args::parse(remaining)?)),
            "compact" => Ok(Commands::Compact(commands::compact::Args::parse(
                remaining,
            )?)),
            "source" => parse_source_subcommand(remaining),
            other => Err(Error::msg(format!("unknown subcommand: {other}"))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Commands {
    Run(commands::run::Args),
    Compact(commands::compact::Args),
    SourceCheck(commands::source_cmd::Args),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandSpec {
    name: &'static str,
    subcommands: Vec<CommandSpec>,
}

impl CommandSpec {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            subcommands: Vec::new(),
        }
    }

    pub fn with_subcommand(mut self, subcommand: CommandSpec) -> Self {
        self.subcommands.push(subcommand);
        self
    }

    pub fn debug_assert(&self) {
        let mut previous = None;
        let mut names: Vec<_> = self
            .subcommands
            .iter()
            .map(|subcommand| subcommand.name)
            .collect();
        names.sort_unstable();
        for name in names {
            if previous == Some(name) {
                panic!("duplicate subcommand: {name}");
            }
            previous = Some(name);
        }
    }

    pub fn get_name(&self) -> &'static str {
        self.name
    }

    pub fn get_subcommands(&self) -> impl Iterator<Item = &CommandSpec> {
        self.subcommands.iter()
    }
}

pub fn run_env() -> Result<()> {
    match Cli::parse_from(std::env::args())? {
        Commands::Run(args) => {
            commands::run::execute_blocking(args)?;
            Ok(())
        }
        Commands::Compact(args) => {
            let report = commands::compact::execute_blocking(args)?;
            println!("{}", report.to_json());
            Ok(())
        }
        Commands::SourceCheck(args) => {
            let report = commands::source_cmd::execute_blocking(args)?;
            println!("{}", commands::source_cmd::format_report_json(&report));
            Ok(())
        }
    }
}

fn parse_source_subcommand(args: Vec<String>) -> Result<Commands> {
    let Some(subcommand) = args.first() else {
        return Err(Error::msg("expected a source subcommand"));
    };

    match subcommand.as_str() {
        "check" => Ok(Commands::SourceCheck(commands::source_cmd::Args::parse(
            &args[1..],
        )?)),
        other => Err(Error::msg(format!("unknown source subcommand: {other}"))),
    }
}

pub(crate) fn block_on<F>(future: F) -> F::Output
where
    F: Future,
{
    let parker = Arc::new(ThreadWaker {
        thread: std::thread::current(),
        notified: AtomicBool::new(false),
    });
    let waker = Waker::from(Arc::clone(&parker));
    let mut future = pin!(future);
    let mut context = Context::from_waker(&waker);

    loop {
        parker.notified.store(false, Ordering::Release);
        match Future::poll(future.as_mut(), &mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => {
                while !parker.notified.swap(false, Ordering::AcqRel) {
                    std::thread::park();
                }
            }
        }
    }
}

struct ThreadWaker {
    thread: std::thread::Thread,
    notified: AtomicBool,
}

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.notified.store(true, Ordering::Release);
        self.thread.unpark();
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Poll};

    #[test]
    fn cli_exposes_run_and_compact_subcommands() {
        let cmd = crate::Cli::command();
        cmd.debug_assert();

        assert!(cmd
            .get_subcommands()
            .any(|subcommand| subcommand.get_name() == "run"));
        assert!(cmd
            .get_subcommands()
            .any(|subcommand| subcommand.get_name() == "compact"));
        assert!(cmd
            .get_subcommands()
            .any(|subcommand| subcommand.get_name() == "source"));
    }

    #[test]
    fn cli_parses_source_check_subcommand() {
        let parsed = crate::Cli::parse_from([
            "iceflow",
            "source",
            "check",
            "--source",
            "fixtures/config_samples/sources/local_file.toml",
        ])
        .expect("parsed command");

        assert!(matches!(
            parsed,
            crate::Commands::SourceCheck(crate::commands::source_cmd::Args { .. })
        ));
    }

    #[test]
    fn cli_rejects_source_check_without_source_flag() {
        let err = crate::Cli::parse_from(["iceflow", "source", "check"])
            .expect_err("missing --source should fail");

        assert_eq!(err.to_string(), "--source <path> is required");
    }

    #[test]
    fn cli_rejects_unknown_source_subcommand() {
        let err = crate::Cli::parse_from(["iceflow", "source", "discover"])
            .expect_err("unknown source subcommand should fail");

        assert_eq!(err.to_string(), "unknown source subcommand: discover");
    }

    #[test]
    fn cli_rejects_source_without_subcommand() {
        let err =
            crate::Cli::parse_from(["iceflow", "source"]).expect_err("missing source subcommand");

        assert_eq!(err.to_string(), "expected a source subcommand");
    }

    #[test]
    fn block_on_advances_futures_that_need_multiple_polls() {
        struct ReadyOnWake {
            started: bool,
            woke: Arc<AtomicBool>,
        }

        impl Future for ReadyOnWake {
            type Output = &'static str;

            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                if self.woke.load(Ordering::Acquire) {
                    return Poll::Ready("done");
                }

                if self.started {
                    panic!("future was polled again before its waker fired");
                }

                self.started = true;
                let woke = Arc::clone(&self.woke);
                let waker = cx.waker().clone();
                std::thread::spawn(move || {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                    woke.store(true, Ordering::Release);
                    waker.wake();
                });
                Poll::Pending
            }
        }

        assert_eq!(
            crate::block_on(ReadyOnWake {
                started: false,
                woke: Arc::new(AtomicBool::new(false)),
            }),
            "done"
        );
    }
}
