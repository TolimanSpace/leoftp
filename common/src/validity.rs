/// A validity check on the data. Mostly useful for fuzzing, to not throw invalid data at end-to-end fuzzer.
pub trait ValidityCheck {
    fn is_valid(&self) -> bool;
}
