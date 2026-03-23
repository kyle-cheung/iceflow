use std::marker::PhantomData;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Utc;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DateTime<Tz> {
    instant: SystemTime,
    _tz: PhantomData<Tz>,
}

impl<Tz> Clone for DateTime<Tz> {
    fn clone(&self) -> Self {
        Self {
            instant: self.instant,
            _tz: PhantomData,
        }
    }
}

impl Utc {
    pub fn now() -> DateTime<Utc> {
        DateTime {
            instant: SystemTime::now(),
            _tz: PhantomData,
        }
    }
}

impl<Tz> DateTime<Tz> {
    pub fn from_timestamp(secs: i64, nanos: u32) -> Option<Self> {
        if nanos >= 1_000_000_000 {
            return None;
        }

        let total_nanos = (secs as i128) * 1_000_000_000i128 + i128::from(nanos);
        let instant = if total_nanos >= 0 {
            let secs = (total_nanos / 1_000_000_000i128) as u64;
            let nanos = (total_nanos % 1_000_000_000i128) as u32;
            UNIX_EPOCH.checked_add(Duration::new(secs, nanos))?
        } else {
            let abs_nanos = total_nanos.unsigned_abs();
            let secs = (abs_nanos / 1_000_000_000u128) as u64;
            let nanos = (abs_nanos % 1_000_000_000u128) as u32;
            UNIX_EPOCH.checked_sub(Duration::new(secs, nanos))?
        };

        Some(Self {
            instant,
            _tz: PhantomData,
        })
    }

    pub fn timestamp(&self) -> i64 {
        match self.instant.duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_secs() as i64,
            Err(err) => {
                let duration = err.duration();
                let secs = duration.as_secs() as i64;
                if duration.subsec_nanos() == 0 {
                    -secs
                } else {
                    -secs - 1
                }
            }
        }
    }

    pub fn timestamp_subsec_nanos(&self) -> u32 {
        match self.instant.duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.subsec_nanos(),
            Err(err) => {
                let nanos = err.duration().subsec_nanos();
                if nanos == 0 {
                    0
                } else {
                    1_000_000_000 - nanos
                }
            }
        }
    }
}
