//! SEBI OTR balancing across multi-account deployments (plan §3.5).
//!
//! OTR is the Order-to-Trade Ratio SEBI monitors per account: every order
//! placement, modification, and cancellation is numerator weight; executed
//! trades are the denominator. The router keeps a live [`OtrLedger`] per
//! registered account and steers away from accounts whose projected OTR is
//! at or above the configured ceiling, distributing pressure toward accounts
//! with ample buffer.
//!
//! Enforcement arms only after [`OtrBalanceConfig::
//! min_operations_before_enforcement`] actions accumulate on an account, so
//! brand-new accounts (whose provisional OTR denominator is 1) are not
//! spuriously locked out — mirroring `honba_mon::OtrConfig`
//! `min_orders_before_enforcement`.

use rust_decimal::Decimal;

/// OTR balancing thresholds shared by all accounts on the router.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OtrBalanceConfig {
    /// Maximum allowed Order-to-Trade Ratio per account (e.g. 50.0).
    pub max_otr: Decimal,
    /// Minimum order operations on an account before the ceiling is
    /// strictly enforced (protects new accounts from cold-start rejection).
    pub min_operations_before_enforcement: u64,
}

impl Default for OtrBalanceConfig {
    fn default() -> Self {
        Self {
            max_otr: Decimal::new(50, 0),
            min_operations_before_enforcement: 25,
        }
    }
}

/// Live OTR accounting for one registered broker account.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct OtrLedger {
    /// Order placements that reached the gateway (placed or broker-rejected).
    pub orders: u64,
    /// Modify requests.
    pub modifications: u64,
    /// Cancel requests.
    pub cancellations: u64,
    /// Executed trades (numerator denominator of OTR).
    pub trades: u64,
}

impl OtrLedger {
    /// Total SEBI numerator weight: every placement, modification, and
    /// cancellation counts as an order action.
    pub fn total_actions(&self) -> u64 {
        self.orders + self.modifications + self.cancellations
    }

    /// Current Order-to-Trade Ratio. Accounts without trades use a
    /// provisional denominator of 1, reflecting the regulatory worst case.
    pub fn otr_ratio(&self) -> Decimal {
        let denominator = self.trades.max(1);
        Decimal::from(self.total_actions()) / Decimal::from(denominator)
    }

    /// True when the OTR ceiling is enforced for this account and currently
    /// breached: the router must allocate new pressure elsewhere.
    pub fn breaches(&self, config: &OtrBalanceConfig) -> bool {
        // Not enforcing yet → cannot breach (cold-start protection).
        if self.total_actions() < config.min_operations_before_enforcement {
            return false;
        }
        self.otr_ratio() >= config.max_otr
    }

    /// Remaining OTR headroom under `config.max_otr` (0+ when in surplus,
    /// negative when already over).
    pub fn headroom(&self, config: &OtrBalanceConfig) -> Decimal {
        config.max_otr - self.otr_ratio()
    }
}
