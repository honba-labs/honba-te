//! M3 freeze-limit catalog contract tests (plan §3.4 item 2).

use honba_core::{Exchange, InstrumentId};
use honba_te_slicer::{FreezeLimitCatalog, builtin_index_freezes};

fn sbin() -> InstrumentId {
    InstrumentId {
        exchange: Exchange::Nse,
        symbol: "SBIN".into(),
        token: Some("779521".into()),
        isin: None,
    }
}

fn sbin_untokenized() -> InstrumentId {
    InstrumentId::new(Exchange::Nse, "sbin")
}

#[test]
fn loads_nse_style_csv_with_case_insensitive_columns() {
    // Header columns are matched case-insensitively; a malformed field value
    // is a hard error (exactness mandate, no silent coercion).
    let csv = "INSTRUMENT,SEGMENT,LOT_SIZE_UNITS,FRZ_QTY,ISIN,INSTRUMENT_TOKEN\n\
        SBIN,,1,1000,INE062A01020,779521\n\
        NIFTY,,50 or 25?,1800,,99926000\n";
    let err = FreezeLimitCatalog::from_csv(csv).expect_err("malformed value is a hard error");
    assert!(
        err.to_string().contains("lot size"),
        "expected the lot-size field to fail: {err}"
    );

    let good = "INSTRUMENT,SEGMENT,LOT_SIZE_UNITS,FRZ_QTY,ISIN,INSTRUMENT_TOKEN\n\
        SBIN,NSE,1,1000,INE062A01020,779521\n\
        NIFTY,NFO,50,1800,,99926000\n\
        \"BANKNIFTY\",NFO,30,900,,99972000\n";
    let catalog = FreezeLimitCatalog::from_csv(good).unwrap();
    assert_eq!(catalog.len(), 3);

    let sbin_limit = catalog.slicing_limit(&sbin()).unwrap();
    assert_eq!(sbin_limit.freeze_quantity, 1000);
    assert_eq!(sbin_limit.lot_size, Some(1));

    // NFO rows canonicalize under the NSE umbrella exchange.
    let nifty = InstrumentId::new(Exchange::Nse, "NIFTY");
    assert_eq!(catalog.slicing_limit(&nifty).unwrap().freeze_quantity, 1800);

    // Token lookup wins over a symbol miss even in mixed case.
    let via_token = InstrumentId {
        exchange: Exchange::Nse,
        symbol: "UNKNOWN-NAME".into(),
        token: Some("779521".into()),
        isin: None,
    };
    assert_eq!(
        catalog.slicing_limit(&via_token).unwrap().freeze_quantity,
        1000
    );

    // Symbol lookup is case-insensitive.
    assert_eq!(
        catalog
            .slicing_limit(&sbin_untokenized())
            .unwrap()
            .freeze_quantity,
        1000
    );
}

#[test]
fn csv_distinguishes_symbol_and_token_matches_and_conflicts() {
    let csv = concat!(
        "SYMBOL,EXCHANGE,FREEZE,LOT,TOKEN\n",
        "SBIN,NSE,1000,1,779521\n",
        "SBIN,BSE,100,1,500100\n",
        // Different instrument on the same symbol: SEBI freeze is per
        // exchange-scrip pair, so BSE SBIN differs.
    );
    let catalog = FreezeLimitCatalog::from_csv(csv).unwrap();
    assert_eq!(
        catalog
            .slicing_limit(&InstrumentId::new(Exchange::Bse, "SBIN"))
            .unwrap()
            .freeze_quantity,
        100
    );

    // Conflicting duplicates fail loudly (exactness mandate).
    let conflict = concat!(
        "SYMBOL,EXCHANGE,FRZ_QTY\n",
        "SBIN,NSE,1000\n",
        "SBIN,NSE,2000\n",
    );
    assert!(matches!(
        FreezeLimitCatalog::from_csv(conflict),
        Err(honba_te_core::BrokerError::InvalidRequest(_))
    ));

    // Idempotent re-load of the identical record is accepted.
    let duplicate = concat!(
        "SYMBOL,EXCHANGE,FRZ_QTY\n",
        "SBIN,NSE,1000\n",
        "SBIN,NSE,1000\n"
    );
    let catalog = FreezeLimitCatalog::from_csv(duplicate).unwrap();
    assert_eq!(catalog.len(), 1);
}

#[test]
fn json_loader_accepts_numbers_and_decimal_strings() {
    let json = r#"[
        {"symbol": "SBIN", "exchange": "NSE", "freeze_quantity": 1000, "lot_size": 1, "token": "779521"},
        {"trading_symbol": "TATASTEEL", "exchange": "nse", "freeze_qty": "5000.000", "lot": "1"},
        {"symbol": "GOLD", "exchange": "MCX", "freeze_qty": 500, "lot_size": 10}
    ]"#;
    let catalog = FreezeLimitCatalog::from_json(json).unwrap();
    assert_eq!(catalog.len(), 3);
    assert_eq!(
        catalog.slicing_limit(&sbin()).unwrap().freeze_quantity,
        1000
    );

    let steel = InstrumentId::new(Exchange::Nse, "tatasteel");
    let limit = catalog.slicing_limit(&steel).unwrap();
    assert_eq!(limit.freeze_quantity, 5000);

    let gold = InstrumentId::new(Exchange::Mcx, "GOLD");
    let limit = catalog.slicing_limit(&gold).unwrap();
    assert_eq!(limit.freeze_quantity, 500);
    assert_eq!(limit.lot_size, Some(10));
}

#[test]
fn bundled_index_constants_match_the_sebi_plan() {
    let mut catalog = FreezeLimitCatalog::new();
    for record in builtin_index_freezes() {
        catalog.insert(record).unwrap();
    }
    for (symbol, freeze, lot) in [
        ("NIFTY", 1800, Some(50)),
        ("BANKNIFTY", 900, Some(30)),
        ("FINNIFTY", 1800, Some(25)),
        ("MIDCPNIFTY", 1800, Some(75)),
    ] {
        let limit = catalog
            .slicing_limit(&InstrumentId::new(Exchange::Nse, symbol))
            .unwrap_or_else(|| panic!("{symbol} lookup"));
        assert_eq!(limit.freeze_quantity, freeze, "{symbol} freeze");
        assert_eq!(limit.lot_size, lot, "{symbol} lot");
    }
}

#[test]
fn default_freeze_applies_when_no_record_matches() {
    let mut catalog = FreezeLimitCatalog::new().with_default_freeze(Some(1200));
    catalog
        .insert(builtin_index_freezes().into_iter().next().unwrap())
        .unwrap();

    // Known instrument → record limit; unknown → default.
    assert_eq!(
        catalog
            .slicing_limit(&InstrumentId::new(Exchange::Nse, "NIFTY"))
            .unwrap()
            .freeze_quantity,
        1800
    );
    assert_eq!(
        catalog
            .slicing_limit(&InstrumentId::new(Exchange::Mcx, "SILVERMIC"))
            .unwrap()
            .freeze_quantity,
        1200
    );

    // Empty default config → unsliced pass-through marker (None).
    let bare = FreezeLimitCatalog::new();
    assert!(
        bare.slicing_limit(&InstrumentId::new(Exchange::Nse, "SILVERMIC"))
            .is_none()
    );
}
