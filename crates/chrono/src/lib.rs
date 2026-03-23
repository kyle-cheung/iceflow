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

        let instant = if secs >= 0 {
            UNIX_EPOCH.checked_add(Duration::new(secs as u64, nanos))?
        } else {
            UNIX_EPOCH.checked_sub(Duration::new(secs.unsigned_abs(), nanos))?
        };

        Some(Self {
            instant,
            _tz: PhantomData,
        })
    }

    pub fn timestamp(&self) -> i64 {
        match self.instant.duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_secs() as i64,
            Err(err) => -(err.duration().as_secs() as i64),
        }
    }

    pub fn timestamp_subsec_nanos(&self) -> u32 {
        match self.instant.duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.subsec_nanos(),
            Err(err) => err.duration().subsec_nanos(),
        }
    }
}
