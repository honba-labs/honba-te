## Description
<!-- Provide a brief summary of the changes and the problem solved. -->

## Subsystem / Bounded Context
<!-- Which subsystem does this change affect? (e.g., honba-core, honba-kokoro, honba-mon, etc.) -->

## Invariant & Quality Checklist
- [ ] **Parallel Test Directory Rule**: All tests are located in `tests/` parallel to `src/` (no in-file `#[test]` blocks).
- [ ] **TDD Verified**: Tests were written and verified (Red -> Green -> Refactor).
- [ ] **Zero Floats Policy**: No floating-point numbers (`f32`/`f64`/`float`) used for prices, quantities, cash, or margin calculations (strictly `Decimal`).
- [ ] **Zero-I/O Domain Purity**: Domain logic does not perform network, disk, or un-mocked clock I/O (explicit `DateTime<Utc>`).
- [ ] **Lint & Format**: Clean `cargo clippy` / `ruff check` / `eslint` and formatting checks pass.
- [ ] **Interface-First**: Changes program to abstract traits/protocols rather than concrete implementations.
