use auth::AuthContext;

/// Visibility level for bucket reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceVisibility {
    Private,
    PublicRead,
}

/// Return true when request may read a bucket.
#[must_use]
pub fn can_read_bucket(
    auth: &AuthContext,
    owner_principal: &str,
    visibility: ResourceVisibility,
) -> bool {
    if auth.principal.as_deref() == Some(owner_principal) {
        return true;
    }

    matches!(visibility, ResourceVisibility::PublicRead)
}

/// Return true when request may mutate a bucket.
#[must_use]
pub fn can_write_bucket(auth: &AuthContext, owner_principal: &str) -> bool {
    auth.principal.as_deref() == Some(owner_principal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use auth::AuthMode;

    fn owner_ctx() -> AuthContext {
        AuthContext {
            mode: AuthMode::HeaderSigV4,
            access_key_id: Some("ak".to_string()),
            principal: Some("owner".to_string()),
            request_epoch_secs: Some(1),
            streaming: None,
        }
    }

    fn other_ctx() -> AuthContext {
        AuthContext {
            mode: AuthMode::HeaderSigV4,
            access_key_id: Some("ak2".to_string()),
            principal: Some("other".to_string()),
            request_epoch_secs: Some(1),
            streaming: None,
        }
    }

    fn anon_ctx() -> AuthContext {
        AuthContext {
            mode: AuthMode::Anonymous,
            access_key_id: None,
            principal: None,
            request_epoch_secs: None,
            streaming: None,
        }
    }

    #[test]
    fn owner_can_read_private() {
        assert!(can_read_bucket(
            &owner_ctx(),
            "owner",
            ResourceVisibility::Private
        ));
    }

    #[test]
    fn non_owner_cannot_read_private() {
        assert!(!can_read_bucket(
            &other_ctx(),
            "owner",
            ResourceVisibility::Private
        ));
        assert!(!can_read_bucket(
            &anon_ctx(),
            "owner",
            ResourceVisibility::Private
        ));
    }

    #[test]
    fn public_read_allows_non_owner() {
        assert!(can_read_bucket(
            &other_ctx(),
            "owner",
            ResourceVisibility::PublicRead
        ));
        assert!(can_read_bucket(
            &anon_ctx(),
            "owner",
            ResourceVisibility::PublicRead
        ));
    }

    #[test]
    fn only_owner_can_write() {
        assert!(can_write_bucket(&owner_ctx(), "owner"));
        assert!(!can_write_bucket(&other_ctx(), "owner"));
        assert!(!can_write_bucket(&anon_ctx(), "owner"));
    }
}
