pub struct AssociatedType;

impl Iterator for AssociatedType {
    type Item = PgId;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}
