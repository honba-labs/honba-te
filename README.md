# 本場・手 honba-te

> *The hand that executes.*

SEBI-compliant order placement for Indian equities. Every order carries a mandatory registered algo ID; the OTR router blocks runs that would breach per-scrip thresholds.

## Crates
- `honba-te-core` — BrokerExecutor, OrderRequest (with algo_id)
- `honba-te-{zerodha,upstox,angelone}`
- `honba-te-router` — OTR counters

License: AGPL-3.0.
