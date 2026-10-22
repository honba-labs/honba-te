//! M3 §5.1 SEBI freeze-limit property verification:
//! "no child order produced by `honba-te-slicer` exceeds exchange freeze
//! quantities across 10,000 randomized order sizes."

use proptest::prelude::*;

use honba_te_slicer::plan_against_limit;

mod common;
use common::parent_request;

proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig::with_cases(10_000))]
    #[test]
    fn no_child_exceeds_freeze(
        quantity in 1u64..1_000_000u64,
        freeze in 1u64..10_000u64,
    ) {
        let parent = parent_request(quantity);
        let plan = plan_against_limit(&parent, Some(freeze)).unwrap();
        let legs = plan.legs.len() as u64;
        prop_assert_eq!(legs, quantity.div_ceil(freeze), "N = ⌈Q/Qfreeze⌉");

        let mut total = 0u64;
        for (index, leg) in plan.legs.iter().enumerate() {
            let child = leg.quantity.0.to_string().parse::<u64>().unwrap();
            prop_assert!(child <= freeze, "leg {index} {child} breaches {freeze}");
            prop_assert_eq!(leg.index as usize, index);
            total += child;
        }
        prop_assert_eq!(total, quantity, "children reconstruct the parent exactly");
    }

    #[test]
    fn no_limit_passes_through_single_leg(quantity in 1u64..1_000_000u64) {
        let parent = parent_request(quantity);
        let plan = plan_against_limit(&parent, None).unwrap();
        prop_assert_eq!(plan.legs.len(), 1);
        prop_assert_eq!(plan.legs[0].quantity.0.to_string(), quantity.to_string());
    }
}
