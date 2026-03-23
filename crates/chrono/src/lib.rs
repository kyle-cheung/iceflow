use std::marker::PhantomData;
use std::time::SystemTime;

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

    pub fn from_system_time(instant: SystemTime) -> DateTime<Utc> {
        DateTime {
            instant,
            _tz: PhantomData,
        }
    }
}

impl<Tz> DateTime<Tz> {
    pub fn from_system_time(instant: SystemTime) -> Self {
        Self {
            instant,
            _tz: PhantomData,
        }
    }
}
