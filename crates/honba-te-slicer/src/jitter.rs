//! Deterministic, zero-float jitter source for execution-schedule masking.
//!
//! SEBI algo-audit trail analysis can fingerprint naive TWAP schedules from
//! their perfectly even inter-child spacing. The plan (§3.4) mandates
//! randomized sub-second Gaussian noise on top of the nominal schedule.
//!
//! Timing jitter is *not* a monetary value, so the zero-floats policy
//! (AGENTS §2.4) is implementable as exact integer arithmetic: three uniform
//! 32-bit draws summed via the Irwin–Hall approximation of the normal
//! distribution. With uniforms `U ~ [0, 2^32)`, the sum `S`,
//! `S - 3·2^31` has mean 0 and standard deviation exactly `2^31`, so
//! `jitter(ms) = (S − 3·2^31) · σ_ms / 2^31` with all steps in i64/i128
//! integers. Maximum |jitter| is bounded at `3·σ` by construction (the full
//! support of the Irwin–Hall-3 sum).
//!
//! The PRNG is mulberry32: small, fast, stream-stable. Seeding the
//! orchestrator with the parent's epoch keeps schedules reproducible from the
//! audit journal (deterministic replay mandate, AGENTS §2.5).

/// Deterministic PRNG seeded jitter stream (mulberry32).
#[derive(Debug, Clone)]
pub struct JitterSource {
    state: u64,
}

impl JitterSource {
    /// New jitter stream from an explicit seed.
    pub fn from_seed(seed: u64) -> Self {
        Self { state: seed }
    }

    /// mulberry32 next raw 32-bit output.
    fn next_u32(&mut self) -> u32 {
        self.state = self.state.wrapping_add(0x6D2B79F5);
        let mut t = self.state;
        t = t ^ (t >> 17); // mulberry32 mix1
        t = t.wrapping_mul(0xED5AD4BB);
        t = t ^ (t >> 11); // mulberry32 mix2
        t = t.wrapping_mul(0xAC4C1B51);
        t = t ^ (t >> 15); // mulberry32 mix3
        t.wrapping_mul(0x3186389B) as u32
    }

    /// Draw one Irwin–Hall(3) sample scaled to standard normal units:
    /// returns `(S − 3·2^31)`, i.e. the raw integer z-score numerator whose
    /// standard deviation is exactly `2^31`.
    fn next_irwin_hall3(&mut self) -> i128 {
        const HALF: i128 = 1 << 31;
        let s = self.next_u32() as i128 + self.next_u32() as i128 + self.next_u32() as i128;
        s - 3 * HALF
    }

    /// Gaussian jitter in whole milliseconds for the requested standard
    /// deviation `sd_millis` (i64 exact; bounded in |·| ≤ 3·sd).
    pub fn gaussian_millis(&mut self, sd_millis: i64) -> i64 {
        const DENOM: i128 = 1 << 31;
        let sd = sd_millis.max(0) as i128;
        let jitter = (self.next_irwin_hall3() * sd).div_euclid(DENOM);
        jitter as i64
    }

    /// Advanced draw: reseeded stream variant (e.g. parent hash) for tests
    /// that need independent streams with identical statistics.
    pub fn reseed(&mut self, seed: u64) {
        self.state = seed;
    }
}
