use super::test_helpers;
use super::*;
use crate::conditional::{ReadCondition, WriteCondition};
use crate::metadata_blob::MetadataBlob;
use crate::sse::ManagedWrappingKeyConfig;
use crate::system_metadata::SystemMetadata;
use ec::EcConfig;
use std::fmt;
use std::path::Path;
use std::sync::Arc;

const NO_READ: &ReadCondition = &ReadCondition {
    if_match: None,
    if_none_match: None,
    if_modified_since: None,
    if_unmodified_since: None,
};
const NO_WRITE: &WriteCondition = &WriteCondition::None;
const BUCKET_PREFIX: &str = "authz-phase1";
const KEY: &str = "key";
const TEST_SSE_S3_WRAPPING_KEY_B64: &str = "YWJjZGVmMDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODk=";

mod model {
    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum Action {
        GetObject,
        GetObjectAttributes,
    }

    impl fmt::Display for Action {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::GetObject => f.write_str("GetObject"),
                Self::GetObjectAttributes => f.write_str("GetObjectAttributes"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum ExistingTarget {
        Current,
        Versioned,
    }

    impl ExistingTarget {
        const ALL: [Self; 2] = [Self::Current, Self::Versioned];
    }

    impl fmt::Display for ExistingTarget {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Current => f.write_str("current"),
                Self::Versioned => f.write_str("versioned"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum BucketOwnerPrincipalShape {
        Root,
        NonRoot,
    }

    impl BucketOwnerPrincipalShape {
        const ALL: [Self; 2] = [Self::Root, Self::NonRoot];
    }

    impl fmt::Display for BucketOwnerPrincipalShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Root => f.write_str("root"),
                Self::NonRoot => f.write_str("non-root"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum RequesterIdentityShape {
        Anonymous,
        BucketOwnerPrincipal,
        SameAccountSharedCanonicalOtherPrincipal,
        SameAccountDistinctPrincipal,
        CrossAccountPrincipal,
    }

    impl RequesterIdentityShape {
        const ALL: [Self; 5] = [
            Self::Anonymous,
            Self::BucketOwnerPrincipal,
            Self::SameAccountSharedCanonicalOtherPrincipal,
            Self::SameAccountDistinctPrincipal,
            Self::CrossAccountPrincipal,
        ];
    }

    impl fmt::Display for RequesterIdentityShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Anonymous => f.write_str("anonymous"),
                Self::BucketOwnerPrincipal => f.write_str("bucket-owner-principal"),
                Self::SameAccountSharedCanonicalOtherPrincipal => {
                    f.write_str("same-account-shared-other")
                }
                Self::SameAccountDistinctPrincipal => f.write_str("same-account-distinct"),
                Self::CrossAccountPrincipal => f.write_str("cross-account"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum ProfileShape {
        Standard,
        OwnerAccountAdmin,
    }

    impl ProfileShape {
        const ALL: [Self; 2] = [Self::Standard, Self::OwnerAccountAdmin];
    }

    impl fmt::Display for ProfileShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Standard => f.write_str("standard"),
                Self::OwnerAccountAdmin => f.write_str("owner-account-admin"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct RequesterShape {
        pub(super) identity: RequesterIdentityShape,
        pub(super) profile: ProfileShape,
    }

    impl RequesterShape {
        fn all() -> impl Iterator<Item = Self> {
            RequesterIdentityShape::ALL
                .into_iter()
                .flat_map(|identity| {
                    ProfileShape::ALL
                        .into_iter()
                        .map(move |profile| Self { identity, profile })
                })
        }

        fn has_admin_profile(self) -> bool {
            self.profile == ProfileShape::OwnerAccountAdmin
        }

        fn is_anonymous(self) -> bool {
            self.identity == RequesterIdentityShape::Anonymous
        }

        fn is_bucket_owner_account(self) -> bool {
            matches!(
                self.identity,
                RequesterIdentityShape::BucketOwnerPrincipal
                    | RequesterIdentityShape::SameAccountSharedCanonicalOtherPrincipal
                    | RequesterIdentityShape::SameAccountDistinctPrincipal
            )
        }

        fn can_bucket_owner_account_admin(self) -> bool {
            self.identity == RequesterIdentityShape::BucketOwnerPrincipal
                || (self.has_admin_profile() && self.is_bucket_owner_account())
        }
    }

    impl fmt::Display for RequesterShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}/{}", self.identity, self.profile)
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum OwnershipShape {
        ObjectWriter,
        BucketOwnerEnforced,
    }

    impl OwnershipShape {
        const ALL: [Self; 2] = [Self::ObjectWriter, Self::BucketOwnerEnforced];
    }

    impl fmt::Display for OwnershipShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::ObjectWriter => f.write_str("object-writer"),
                Self::BucketOwnerEnforced => f.write_str("bucket-owner-enforced"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct BucketShape {
        pub(super) owner_principal: BucketOwnerPrincipalShape,
        pub(super) ownership: OwnershipShape,
        pub(super) ignore_public_acls: bool,
        pub(super) block_public_acls: bool,
        pub(super) restrict_public_buckets: bool,
        pub(super) bucket_public_read: bool,
        pub(super) bucket_public_write: bool,
    }

    impl BucketShape {
        fn all() -> impl Iterator<Item = Self> {
            BucketOwnerPrincipalShape::ALL
                .into_iter()
                .flat_map(|owner_principal| {
                    OwnershipShape::ALL.into_iter().flat_map(move |ownership| {
                        [false, true]
                            .into_iter()
                            .map(move |restrict_public_buckets| Self {
                                owner_principal,
                                ownership,
                                ignore_public_acls: false,
                                block_public_acls: false,
                                restrict_public_buckets,
                                bucket_public_read: false,
                                bucket_public_write: false,
                            })
                    })
                })
        }
    }

    impl fmt::Display for BucketShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "owner={} {}(ignore_public_acls={}, block_public_acls={}, restrict_public_buckets={}, public_read={}, public_write={})",
                self.owner_principal,
                self.ownership,
                self.ignore_public_acls,
                self.block_public_acls,
                self.restrict_public_buckets,
                self.bucket_public_read,
                self.bucket_public_write
            )
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum ObjectOwnerKind {
        BucketOwner,
        SameAccountSharedOther,
        SameAccountDistinct,
    }

    impl ObjectOwnerKind {
        const ALL: [Self; 3] = [
            Self::BucketOwner,
            Self::SameAccountSharedOther,
            Self::SameAccountDistinct,
        ];
    }

    impl fmt::Display for ObjectOwnerKind {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::BucketOwner => f.write_str("bucket-owner-principal"),
                Self::SameAccountSharedOther => f.write_str("same-account-shared-other"),
                Self::SameAccountDistinct => f.write_str("same-account-distinct"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum ObjectAclShape {
        Private,
    }

    impl ObjectAclShape {
        const ALL: [Self; 1] = [Self::Private];
    }

    impl fmt::Display for ObjectAclShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Private => f.write_str("private"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct ObjectShape {
        pub(super) owner_kind: ObjectOwnerKind,
        pub(super) acl: ObjectAclShape,
    }

    impl ObjectShape {
        fn all() -> impl Iterator<Item = Self> {
            ObjectOwnerKind::ALL.into_iter().flat_map(|owner_kind| {
                ObjectAclShape::ALL
                    .into_iter()
                    .map(move |acl| Self { owner_kind, acl })
            })
        }
    }

    impl fmt::Display for ObjectShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}+{}", self.owner_kind, self.acl)
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum PolicyDecisionShape {
        NoPolicy,
        NoMatch,
        ExplicitAllowPrivate,
        ExplicitAllowPublic,
        ExplicitDeny,
    }

    impl PolicyDecisionShape {
        const ALL: [Self; 5] = [
            Self::NoPolicy,
            Self::NoMatch,
            Self::ExplicitAllowPrivate,
            Self::ExplicitAllowPublic,
            Self::ExplicitDeny,
        ];

        fn is_public_allow(self) -> bool {
            self == Self::ExplicitAllowPublic
        }

        fn is_private_allow(self) -> bool {
            self == Self::ExplicitAllowPrivate
        }
    }

    impl fmt::Display for PolicyDecisionShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::NoPolicy => f.write_str("no-policy"),
                Self::NoMatch => f.write_str("no-match"),
                Self::ExplicitAllowPrivate => f.write_str("allow-private"),
                Self::ExplicitAllowPublic => f.write_str("allow-public"),
                Self::ExplicitDeny => f.write_str("deny"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct PolicyShape {
        pub(super) read: PolicyDecisionShape,
        pub(super) attrs: Option<PolicyDecisionShape>,
    }

    impl PolicyShape {
        fn all_for(action: Action) -> Vec<Self> {
            match action {
                Action::GetObject => PolicyDecisionShape::ALL
                    .into_iter()
                    .map(|read| Self { read, attrs: None })
                    .collect(),
                Action::GetObjectAttributes => vec![
                    Self {
                        read: PolicyDecisionShape::NoPolicy,
                        attrs: Some(PolicyDecisionShape::NoPolicy),
                    },
                    Self {
                        read: PolicyDecisionShape::NoMatch,
                        attrs: Some(PolicyDecisionShape::NoMatch),
                    },
                    Self {
                        read: PolicyDecisionShape::ExplicitAllowPrivate,
                        attrs: Some(PolicyDecisionShape::ExplicitAllowPrivate),
                    },
                    Self {
                        read: PolicyDecisionShape::ExplicitAllowPublic,
                        attrs: Some(PolicyDecisionShape::ExplicitAllowPublic),
                    },
                    Self {
                        read: PolicyDecisionShape::ExplicitDeny,
                        attrs: Some(PolicyDecisionShape::ExplicitDeny),
                    },
                    Self {
                        read: PolicyDecisionShape::ExplicitAllowPrivate,
                        attrs: Some(PolicyDecisionShape::NoPolicy),
                    },
                    Self {
                        read: PolicyDecisionShape::NoPolicy,
                        attrs: Some(PolicyDecisionShape::ExplicitAllowPrivate),
                    },
                    Self {
                        read: PolicyDecisionShape::ExplicitAllowPrivate,
                        attrs: Some(PolicyDecisionShape::ExplicitDeny),
                    },
                    Self {
                        read: PolicyDecisionShape::ExplicitDeny,
                        attrs: Some(PolicyDecisionShape::ExplicitAllowPrivate),
                    },
                    Self {
                        read: PolicyDecisionShape::ExplicitAllowPublic,
                        attrs: Some(PolicyDecisionShape::NoPolicy),
                    },
                    Self {
                        read: PolicyDecisionShape::ExplicitAllowPrivate,
                        attrs: Some(PolicyDecisionShape::ExplicitAllowPublic),
                    },
                    Self {
                        read: PolicyDecisionShape::ExplicitAllowPublic,
                        attrs: Some(PolicyDecisionShape::ExplicitAllowPrivate),
                    },
                    Self {
                        read: PolicyDecisionShape::NoPolicy,
                        attrs: Some(PolicyDecisionShape::ExplicitAllowPublic),
                    },
                    Self {
                        read: PolicyDecisionShape::NoMatch,
                        attrs: Some(PolicyDecisionShape::ExplicitAllowPrivate),
                    },
                    Self {
                        read: PolicyDecisionShape::ExplicitAllowPrivate,
                        attrs: Some(PolicyDecisionShape::NoMatch),
                    },
                ],
            }
        }

        pub(super) fn attrs_decision(self) -> PolicyDecisionShape {
            self.attrs
                .expect("GetObjectAttributes scenarios must carry an attrs policy decision")
        }

        fn has_public_allow(self) -> bool {
            self.read.is_public_allow()
                || self.attrs.is_some_and(PolicyDecisionShape::is_public_allow)
        }

        fn has_private_allow(self) -> bool {
            self.read.is_private_allow()
                || self
                    .attrs
                    .is_some_and(PolicyDecisionShape::is_private_allow)
        }
    }

    impl fmt::Display for PolicyShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "read={}", self.read)?;
            if let Some(attrs) = self.attrs {
                write!(f, " attrs={attrs}")?;
            }
            Ok(())
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum Outcome {
        Allow,
        Deny,
    }

    impl fmt::Display for Outcome {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Allow => f.write_str("Allow"),
                Self::Deny => f.write_str("Deny"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct Scenario {
        pub(super) action: Action,
        pub(super) target: ExistingTarget,
        pub(super) requester: RequesterShape,
        pub(super) bucket: BucketShape,
        pub(super) object: ObjectShape,
        pub(super) policy: PolicyShape,
    }

    impl Scenario {
        pub(super) fn phase1_scenarios(action: Action) -> Vec<Self> {
            let mut scenarios = Vec::new();
            for target in ExistingTarget::ALL {
                for requester in RequesterShape::all() {
                    for bucket in BucketShape::all() {
                        for object in ObjectShape::all() {
                            for policy in PolicyShape::all_for(action) {
                                let scenario = Self {
                                    action,
                                    target,
                                    requester,
                                    bucket,
                                    object,
                                    policy,
                                };
                                if scenario.phase1_is_possible() {
                                    scenarios.push(scenario);
                                }
                            }
                        }
                    }
                }
            }
            scenarios
        }

        pub(super) fn expected_existing_outcome(self) -> Outcome {
            let allowed = match self.action {
                Action::GetObject => {
                    self.policy_decision_allows(self.policy.read, self.base_get_object_allowed())
                }
                Action::GetObjectAttributes => {
                    // GetObjectAttributes must satisfy both the GetObject read policy and the
                    // distinct GetObjectAttributes policy; phase 1 keeps these explicit so a
                    // regression in either half of the conjunction fails the matrix.
                    let read_allowed = self
                        .policy_decision_allows(self.policy.read, self.base_get_object_allowed());
                    let attrs_allowed = self.policy_decision_allows(
                        self.policy.attrs_decision(),
                        self.base_get_object_attributes_allowed(),
                    );
                    read_allowed && attrs_allowed
                }
            };

            if allowed {
                Outcome::Allow
            } else {
                Outcome::Deny
            }
        }

        fn phase1_is_possible(self) -> bool {
            if self.requester.is_anonymous() && self.requester.has_admin_profile() {
                return false;
            }
            if self.requester.identity == RequesterIdentityShape::BucketOwnerPrincipal
                && self.requester.has_admin_profile()
            {
                return false;
            }
            if self.requester.identity == RequesterIdentityShape::CrossAccountPrincipal
                && self.requester.has_admin_profile()
            {
                return false;
            }
            if self.policy.has_private_allow() && self.requester.is_anonymous() {
                return false;
            }
            if self.bucket.restrict_public_buckets && !self.policy.has_public_allow() {
                return false;
            }

            // Phase 1 materializes BOE objects by enabling BOE before the write.
            // Pre-BOE retained-owner cases belong in the later transition phase.
            if self.bucket.ownership == OwnershipShape::BucketOwnerEnforced
                && self.object.owner_kind != ObjectOwnerKind::BucketOwner
            {
                return false;
            }

            true
        }

        fn base_get_object_allowed(self) -> bool {
            if self.bucket.ownership == OwnershipShape::BucketOwnerEnforced {
                return self.requester.can_bucket_owner_account_admin();
            }

            match self.object.acl {
                // Phase 1 keeps ACL shape minimal: private objects rely on owner identity only.
                ObjectAclShape::Private => self.requester_matches_object_owner(),
            }
        }

        fn base_get_object_attributes_allowed(self) -> bool {
            if self.bucket.ownership == OwnershipShape::BucketOwnerEnforced {
                // This is the phase-1 rule that must stay explicit: BOE GetObjectAttributes
                // follows object-owner principal identity, not bucket-owner-account admin.
                return self.requester_principal_matches_object_owner();
            }

            self.base_get_object_allowed()
        }

        fn public_policy_allow_survives(self) -> bool {
            !self.bucket.restrict_public_buckets || self.requester.is_bucket_owner_account()
        }

        fn policy_decision_allows(self, decision: PolicyDecisionShape, fallback: bool) -> bool {
            match decision {
                PolicyDecisionShape::ExplicitDeny => false,
                PolicyDecisionShape::ExplicitAllowPrivate => true,
                PolicyDecisionShape::ExplicitAllowPublic if self.public_policy_allow_survives() => {
                    true
                }
                PolicyDecisionShape::ExplicitAllowPublic
                | PolicyDecisionShape::NoPolicy
                | PolicyDecisionShape::NoMatch => fallback,
            }
        }

        fn requester_matches_object_owner(self) -> bool {
            self.requester_principal_matches_object_owner()
                || (self.requester_canonical_matches_object_owner()
                    && self.requester.has_admin_profile())
        }

        fn requester_principal_matches_object_owner(self) -> bool {
            matches!(
                (self.requester.identity, self.object.owner_kind),
                (
                    RequesterIdentityShape::BucketOwnerPrincipal,
                    ObjectOwnerKind::BucketOwner
                ) | (
                    RequesterIdentityShape::SameAccountSharedCanonicalOtherPrincipal,
                    ObjectOwnerKind::SameAccountSharedOther,
                ) | (
                    RequesterIdentityShape::SameAccountDistinctPrincipal,
                    ObjectOwnerKind::SameAccountDistinct,
                )
            )
        }

        fn requester_canonical_matches_object_owner(self) -> bool {
            self.requester_canonical_group() == self.object_canonical_group()
        }

        fn requester_canonical_group(self) -> Option<CanonicalGroup> {
            match self.requester.identity {
                RequesterIdentityShape::Anonymous => None,
                RequesterIdentityShape::BucketOwnerPrincipal
                | RequesterIdentityShape::SameAccountSharedCanonicalOtherPrincipal => {
                    Some(CanonicalGroup::Shared)
                }
                RequesterIdentityShape::SameAccountDistinctPrincipal => {
                    Some(CanonicalGroup::Distinct)
                }
                RequesterIdentityShape::CrossAccountPrincipal => Some(CanonicalGroup::CrossAccount),
            }
        }

        fn object_canonical_group(self) -> Option<CanonicalGroup> {
            match self.object.owner_kind {
                ObjectOwnerKind::BucketOwner | ObjectOwnerKind::SameAccountSharedOther => {
                    Some(CanonicalGroup::Shared)
                }
                ObjectOwnerKind::SameAccountDistinct => Some(CanonicalGroup::Distinct),
            }
        }
    }

    impl fmt::Display for Scenario {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "action={} target={} requester={} bucket={} object={} policy={}",
                self.action, self.target, self.requester, self.bucket, self.object, self.policy
            )
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum CanonicalGroup {
        Shared,
        Distinct,
        CrossAccount,
    }
}

mod harness {
    use super::model::{
        Action, BucketOwnerPrincipalShape, ExistingTarget, ObjectAclShape, ObjectOwnerKind,
        Outcome, OwnershipShape, PolicyDecisionShape, RequesterIdentityShape, RequesterShape,
        Scenario,
    };
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(super) enum ClassifiedResult {
        Allow,
        AccessDenied,
        NoSuchKey,
        VersionNotFound,
    }

    #[derive(Clone)]
    struct IdentityFixtures {
        root: AccountIdentity,
        owner_user: AccountIdentity,
        same_account_distinct: AccountIdentity,
        cross_account: AccountIdentity,
    }

    impl IdentityFixtures {
        fn new() -> Self {
            let shared_canonical_id = CanonicalUserId::from_principal("111122223333");
            let root = AccountIdentity::new(
                "arn:aws:iam::111122223333:root",
                shared_canonical_id.clone(),
                "Owner Root",
            );
            let owner_user = AccountIdentity::new(
                "arn:aws:iam::111122223333:user/owner",
                shared_canonical_id,
                "Owner User",
            );
            let same_account_distinct = AccountIdentity::new(
                "arn:aws:iam::111122223333:user/distinct",
                CanonicalUserId::from_principal("same-account-distinct-canonical"),
                "Distinct Same Account User",
            );
            let cross_account =
                AccountIdentity::from_principal("arn:aws:iam::444455556666:user/other");
            Self {
                root,
                owner_user,
                same_account_distinct,
                cross_account,
            }
        }

        fn bucket_owner_account(
            &self,
            owner_principal: BucketOwnerPrincipalShape,
        ) -> &AccountIdentity {
            match owner_principal {
                BucketOwnerPrincipalShape::Root => &self.root,
                BucketOwnerPrincipalShape::NonRoot => &self.owner_user,
            }
        }

        fn shared_other_account(
            &self,
            owner_principal: BucketOwnerPrincipalShape,
        ) -> &AccountIdentity {
            match owner_principal {
                BucketOwnerPrincipalShape::Root => &self.owner_user,
                BucketOwnerPrincipalShape::NonRoot => &self.root,
            }
        }

        fn bucket_owner_requester(&self, owner_principal: BucketOwnerPrincipalShape) -> Requester {
            Requester::authenticated(self.bucket_owner_account(owner_principal).clone())
        }

        fn requester(
            &self,
            owner_principal: BucketOwnerPrincipalShape,
            shape: RequesterShape,
        ) -> Requester {
            match shape.identity {
                RequesterIdentityShape::Anonymous => Requester::anonymous(),
                RequesterIdentityShape::BucketOwnerPrincipal => {
                    self.requester_from_account(self.bucket_owner_account(owner_principal), shape)
                }
                RequesterIdentityShape::SameAccountSharedCanonicalOtherPrincipal => {
                    self.requester_from_account(self.shared_other_account(owner_principal), shape)
                }
                RequesterIdentityShape::SameAccountDistinctPrincipal => {
                    self.requester_from_account(&self.same_account_distinct, shape)
                }
                RequesterIdentityShape::CrossAccountPrincipal => {
                    self.requester_from_account(&self.cross_account, shape)
                }
            }
        }

        fn object_writer(
            &self,
            owner_principal: BucketOwnerPrincipalShape,
            owner: ObjectOwnerKind,
        ) -> Requester {
            match owner {
                ObjectOwnerKind::BucketOwner => self.bucket_owner_requester(owner_principal),
                ObjectOwnerKind::SameAccountSharedOther => {
                    Requester::authenticated(self.shared_other_account(owner_principal).clone())
                }
                ObjectOwnerKind::SameAccountDistinct => {
                    Requester::authenticated(self.same_account_distinct.clone())
                }
            }
        }

        fn requester_principal(
            &self,
            owner_principal: BucketOwnerPrincipalShape,
            shape: RequesterShape,
        ) -> Option<&str> {
            match shape.identity {
                RequesterIdentityShape::Anonymous => None,
                RequesterIdentityShape::BucketOwnerPrincipal => {
                    Some(self.bucket_owner_account(owner_principal).principal())
                }
                RequesterIdentityShape::SameAccountSharedCanonicalOtherPrincipal => {
                    Some(self.shared_other_account(owner_principal).principal())
                }
                RequesterIdentityShape::SameAccountDistinctPrincipal => {
                    Some(self.same_account_distinct.principal())
                }
                RequesterIdentityShape::CrossAccountPrincipal => {
                    Some(self.cross_account.principal())
                }
            }
        }

        fn object_owner_principal(
            &self,
            owner_principal: BucketOwnerPrincipalShape,
            owner: ObjectOwnerKind,
        ) -> &str {
            match owner {
                ObjectOwnerKind::BucketOwner => {
                    self.bucket_owner_account(owner_principal).principal()
                }
                ObjectOwnerKind::SameAccountSharedOther => {
                    self.shared_other_account(owner_principal).principal()
                }
                ObjectOwnerKind::SameAccountDistinct => self.same_account_distinct.principal(),
            }
        }

        fn requester_from_account(
            &self,
            account: &AccountIdentity,
            shape: RequesterShape,
        ) -> Requester {
            match shape.profile {
                model::ProfileShape::Standard => Requester::authenticated(account.clone()),
                model::ProfileShape::OwnerAccountAdmin => {
                    Requester::authenticated_owner_account_admin(account.clone())
                }
            }
        }
    }

    pub(super) struct MatrixHarness {
        _tmp: test_util::TempDir,
        coord: Coordinator,
        fixtures: IdentityFixtures,
    }

    impl MatrixHarness {
        pub(super) fn new() -> Self {
            let tmp = test_util::tempdir();
            let coord = setup_coordinator(tmp.path());
            let fixtures = IdentityFixtures::new();
            Self {
                _tmp: tmp,
                coord,
                fixtures,
            }
        }

        pub(super) fn run_existing(&self, bucket: &str, scenario: Scenario) -> ClassifiedResult {
            materialize_bucket(&self.coord, &self.fixtures, bucket, scenario).unwrap_or_else(
                |err| {
                    panic!("failed to materialize bucket for {scenario}: {err:?}");
                },
            );

            let object_version = materialize_object(&self.coord, &self.fixtures, bucket, scenario)
                .unwrap_or_else(|err| {
                    panic!("failed to materialize object for {scenario}: {err:?}");
                });

            materialize_policy(&self.coord, &self.fixtures, bucket, scenario).unwrap_or_else(
                |err| {
                    panic!("failed to materialize policy for {scenario}: {err:?}");
                },
            );

            classify(run_action(
                &self.coord,
                &self.fixtures,
                bucket,
                scenario,
                object_version,
            ))
        }
    }

    pub(super) fn bucket_name_for(action: Action, index: usize) -> String {
        let action_tag = match action {
            Action::GetObject => "go",
            Action::GetObjectAttributes => "goa",
        };
        format!("{BUCKET_PREFIX}-{action_tag}-{index:05}")
    }

    pub(super) fn to_existing_outcome(result: ClassifiedResult) -> Outcome {
        match result {
            ClassifiedResult::Allow => Outcome::Allow,
            ClassifiedResult::AccessDenied => Outcome::Deny,
            ClassifiedResult::NoSuchKey | ClassifiedResult::VersionNotFound => {
                panic!(
                    "phase 1 existing-object matrix produced an impossible missing-object result"
                )
            }
        }
    }

    fn setup_coordinator(dir: &Path) -> Coordinator {
        let pg_ids: Vec<u32> = (0..4).collect();
        let storage_node = Arc::new(SharedStorageNode::open(dir, &pg_ids).unwrap());
        Coordinator::new_with_managed_key_provider(
            storage_node,
            EcConfig::default(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
        )
        .unwrap()
    }

    fn test_sse_s3_provider() -> StaticManagedKeyProvider {
        StaticManagedKeyProvider::single(
            ManagedWrappingKeyConfig::from_base64(1, TEST_SSE_S3_WRAPPING_KEY_B64).unwrap(),
        )
    }

    fn materialize_bucket(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: Scenario,
    ) -> Result<(), ServerError> {
        let owner_account = fixtures.bucket_owner_account(scenario.bucket.owner_principal);
        let owner = OwnerIdentity::new(
            owner_account.principal(),
            owner_account.canonical_user_id().clone(),
        );
        let grants = Coordinator::bucket_acl_grants_from_flags(&owner, false, false);
        coord.create_bucket_with_acl_grants(&owner, bucket, grants, false)?;

        coord.put_bucket_versioning(&PutBucketVersioningRequest {
            bucket: BucketRequest::new(
                bucket,
                fixtures.bucket_owner_requester(scenario.bucket.owner_principal),
                None,
            ),
            state: BucketVersioningState::Enabled,
        })?;

        if scenario.bucket.ownership == OwnershipShape::BucketOwnerEnforced {
            coord.put_bucket_ownership_controls(&PutBucketOwnershipControlsRequest {
                bucket: BucketRequest::new(
                    bucket,
                    fixtures.bucket_owner_requester(scenario.bucket.owner_principal),
                    None,
                ),
                config: BucketOwnershipControls {
                    object_ownership: BucketObjectOwnership::BucketOwnerEnforced,
                },
            })?;
        }

        if scenario.bucket.restrict_public_buckets {
            coord.put_bucket_public_access_block(&PutBucketPublicAccessBlockRequest {
                bucket: BucketRequest::new(
                    bucket,
                    fixtures.bucket_owner_requester(scenario.bucket.owner_principal),
                    None,
                ),
                config: PublicAccessBlockConfig {
                    block_public_acls: false,
                    ignore_public_acls: false,
                    block_public_policy: false,
                    restrict_public_buckets: true,
                },
            })?;
        }

        Ok(())
    }

    fn materialize_object(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: Scenario,
    ) -> Result<VersionId, ServerError> {
        if scenario.object.owner_kind != ObjectOwnerKind::BucketOwner {
            let principal = fixtures.object_owner_principal(
                scenario.bucket.owner_principal,
                scenario.object.owner_kind,
            );
            let policy = format!(
                r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":"s3:PutObject","Resource":"arn:aws:s3:::{bucket}/{KEY}"}}]}}"#
            );
            coord.put_bucket_policy(&PutBucketPolicyRequest {
                bucket: BucketRequest::new(
                    bucket,
                    fixtures.bucket_owner_requester(scenario.bucket.owner_principal),
                    None,
                ),
                config: &policy,
                confirm_remove_self_bucket_access: false,
            })?;
        }

        let writer =
            fixtures.object_writer(scenario.bucket.owner_principal, scenario.object.owner_kind);
        let put = test_helpers::put_object(
            coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: ObjectRequest::new(bucket, KEY, writer, None),
                data: b"phase-1-data",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,
                acl: object_write_acl(scenario.object.acl),
            },
        )?;
        Ok(put.version_id)
    }

    fn materialize_policy(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: Scenario,
    ) -> Result<(), ServerError> {
        let Some(policy) = policy_document(fixtures, bucket, scenario) else {
            if scenario.object.owner_kind != ObjectOwnerKind::BucketOwner {
                coord.delete_bucket_policy(&BucketRequest::new(
                    bucket,
                    fixtures.bucket_owner_requester(scenario.bucket.owner_principal),
                    None,
                ))?;
            }
            return Ok(());
        };

        coord.put_bucket_policy(&PutBucketPolicyRequest {
            bucket: BucketRequest::new(
                bucket,
                fixtures.bucket_owner_requester(scenario.bucket.owner_principal),
                None,
            ),
            config: &policy,
            confirm_remove_self_bucket_access: false,
        })
    }

    fn run_action(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: Scenario,
        object_version: VersionId,
    ) -> Result<(), ServerError> {
        let requester = fixtures.requester(scenario.bucket.owner_principal, scenario.requester);
        let version_id = match scenario.target {
            ExistingTarget::Current => None,
            ExistingTarget::Versioned => Some(object_version),
        };

        match scenario.action {
            Action::GetObject => coord
                .get_object(&GetObjectRequest {
                    sse_customer: None,
                    object: ObjectVersionRequest::new(bucket, KEY, version_id, requester, None),
                    cond: NO_READ,
                })
                .and_then(|result| {
                    let _ = read_all_body(result.body)?;
                    Ok(())
                }),
            Action::GetObjectAttributes => coord
                .get_object_attributes(&GetObjectAttributesRequest {
                    object: ObjectVersionRequest::new(bucket, KEY, version_id, requester, None),
                    cond: NO_READ,
                    want_parts: false,
                    part_number_marker: None,
                    max_parts: 0,
                    sse_customer: None,
                })
                .map(|_| ()),
        }
    }

    fn classify(result: Result<(), ServerError>) -> ClassifiedResult {
        match result {
            Ok(()) => ClassifiedResult::Allow,
            Err(ServerError::AccessDenied) => ClassifiedResult::AccessDenied,
            Err(ServerError::ObjectNotFound { .. }) => ClassifiedResult::NoSuchKey,
            Err(ServerError::VersionNotFound { .. }) => ClassifiedResult::VersionNotFound,
            Err(other) => panic!("unexpected classified result: {other:?}"),
        }
    }

    fn read_all_body(mut body: ReadHandle) -> Result<Vec<u8>, ServerError> {
        let mut out = Vec::new();
        while let Some(chunk) = body.next_chunk(INTERNAL_SEGMENT_SIZE)? {
            out.extend_from_slice(&chunk);
        }
        Ok(out)
    }

    fn object_write_acl(shape: ObjectAclShape) -> PutObjectWriteAcl<'static> {
        match shape {
            ObjectAclShape::Private => PutObjectWriteAcl::None,
        }
    }

    fn policy_document(
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: Scenario,
    ) -> Option<String> {
        let mut statements = Vec::new();
        push_policy_statement(
            &mut statements,
            fixtures,
            bucket,
            scenario,
            get_object_policy_action_name(scenario.target),
            scenario.policy.read,
        );
        if scenario.action == Action::GetObjectAttributes {
            push_policy_statement(
                &mut statements,
                fixtures,
                bucket,
                scenario,
                get_object_attributes_policy_action_name(scenario.target),
                scenario.policy.attrs_decision(),
            );
        }

        if statements.is_empty() {
            None
        } else {
            Some(format!(
                r#"{{"Version":"2012-10-17","Statement":[{}]}}"#,
                statements.join(",")
            ))
        }
    }

    fn push_policy_statement(
        statements: &mut Vec<String>,
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: Scenario,
        action: &str,
        decision: PolicyDecisionShape,
    ) {
        let resource = format!("arn:aws:s3:::{bucket}/{KEY}");
        match decision {
            PolicyDecisionShape::NoPolicy => {}
            PolicyDecisionShape::NoMatch => statements.push(format!(
                r#"{{"Effect":"Allow","Principal":{{"AWS":"arn:aws:iam::999988887777:user/unmatched"}},"Action":"{action}","Resource":"{resource}"}}"#
            )),
            PolicyDecisionShape::ExplicitAllowPrivate => {
                let principal = fixtures
                    .requester_principal(scenario.bucket.owner_principal, scenario.requester)
                    .expect("private allow requires an authenticated requester");
                statements.push(format!(
                    r#"{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":"{action}","Resource":"{resource}"}}"#
                ));
            }
            PolicyDecisionShape::ExplicitAllowPublic => statements.push(format!(
                r#"{{"Effect":"Allow","Principal":"*","Action":"{action}","Resource":"{resource}"}}"#
            )),
            PolicyDecisionShape::ExplicitDeny => statements.push(format!(
                r#"{{"Effect":"Deny","Principal":"*","Action":"{action}","Resource":"{resource}"}}"#
            )),
        }
    }

    fn get_object_policy_action_name(target: ExistingTarget) -> &'static str {
        match target {
            ExistingTarget::Current => "s3:GetObject",
            ExistingTarget::Versioned => "s3:GetObjectVersion",
        }
    }

    fn get_object_attributes_policy_action_name(target: ExistingTarget) -> &'static str {
        match target {
            ExistingTarget::Current => "s3:GetObjectAttributes",
            ExistingTarget::Versioned => "s3:GetObjectVersionAttributes",
        }
    }
}

use harness::{bucket_name_for, to_existing_outcome, MatrixHarness};
use model::{Action, Scenario};

#[test]
fn authz_model_phase1_get_object_existing_matrix() {
    run_phase1_existing_matrix(Action::GetObject);
}

#[test]
fn authz_model_phase1_get_object_attributes_existing_matrix() {
    run_phase1_existing_matrix(Action::GetObjectAttributes);
}

fn run_phase1_existing_matrix(action: Action) {
    let scenarios = Scenario::phase1_scenarios(action);
    assert!(
        !scenarios.is_empty(),
        "phase 1 matrix unexpectedly produced no scenarios for {action}"
    );
    let harness = MatrixHarness::new();

    for (index, scenario) in scenarios.into_iter().enumerate() {
        let bucket = bucket_name_for(action, index);
        let expected = scenario.expected_existing_outcome();
        let actual = to_existing_outcome(harness.run_existing(&bucket, scenario));
        assert_eq!(
            actual, expected,
            "phase 1 authz model mismatch\nscenario: {scenario}\nexpected: {expected}\nactual: {actual}"
        );
    }
}
