use std::fmt::{Display, Formatter, Result as FmtResult};

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
/// This represents the half-life of the decay of the popularity scores
/// and top scores.
pub enum TimeScope {
    HalfHour,
    Hour,
    TwoHours,
    FourHours,
    EightHours,
    Day,
    Month,
}

impl TimeScope {
    pub fn name(self) -> &'static str {
        match self {
            TimeScope::HalfHour => "half-hour",
            TimeScope::Hour => "hour",
            TimeScope::TwoHours => "two-hours",
            TimeScope::FourHours => "four-hours",
            TimeScope::EightHours => "eight-hours",
            TimeScope::Day => "day",
            TimeScope::Month => "month",
        }
    }

    pub fn decay(self, value: f64, since: f64) -> f64 {
        let hl = self.half_life();
        value * (2.0f64.powf(since / hl))
    }

    pub fn half_life(self) -> f64 {
        match self {
            TimeScope::HalfHour => 30.0,
            TimeScope::Hour => 60.0,
            TimeScope::TwoHours => 60.0 * 2.0,
            TimeScope::FourHours => 60.0 * 4.0,
            TimeScope::EightHours => 60.0 * 8.0,
            TimeScope::Day => 60.0 * 24.0,
            TimeScope::Month => 60.0 * 24.0 * 30.0,
        }
    }

    pub fn variants() -> impl Iterator<Item = TimeScope> {
        [
            TimeScope::HalfHour,
            TimeScope::Hour,
            TimeScope::TwoHours,
            TimeScope::FourHours,
            TimeScope::EightHours,
            TimeScope::Day,
            TimeScope::Month,
        ]
        .iter()
        .cloned()
    }
}

impl Display for TimeScope {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{}", self.name())
    }
}
