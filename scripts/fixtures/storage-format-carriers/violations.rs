impl
    StoredTagSet
where
    Self: Sized,
{
    pub const fn raw_bytes(&self) -> &[u8] {
        &[]
    }
}

impl TryFrom<StoredAclGrants> for String {
    type Error = ();

    fn try_from(_: StoredAclGrants) -> Result<Self, Self::Error> {
        Ok(String::new())
    }
}
