use anyhow::Result;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Args;

impl Args {
    pub fn parse(args: Vec<String>) -> Result<Self> {
        if args.is_empty() {
            Ok(Self)
        } else {
            anyhow::bail!("compact does not accept arguments yet");
        }
    }
}
