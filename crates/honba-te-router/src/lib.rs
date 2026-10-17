//! OTR-aware smart order router.
use chrono::NaiveDate;
use rust_decimal::Decimal;
use std::collections::HashMap;

pub struct ExecRouter {
    pub otr_limit_per_scrip: u32,
    counters: HashMap<(NaiveDate, String), u32>,
}

impl ExecRouter {
    pub fn new(limit: u32) -> Self {
        Self {
            otr_limit_per_scrip: limit,
            counters: HashMap::new(),
        }
    }

    pub fn check_otr(&mut self, scrip: &str, day: NaiveDate) -> bool {
        let c = self.counters.entry((day, scrip.to_string())).or_insert(0);
        *c += 1;
        *c <= self.otr_limit_per_scrip
    }

    pub fn reset_day(&mut self, day: NaiveDate) {
        self.counters.retain(|(d, _), _| *d == day);
    }

    pub fn notional(&self, p: Decimal, q: u64) -> Decimal {
        p * Decimal::from(q)
    }
}
