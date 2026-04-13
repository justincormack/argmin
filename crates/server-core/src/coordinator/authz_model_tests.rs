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
const MISSING_KEY: &str = "missing";
const TAGS_XML: &str =
    "<Tagging><TagSet><Tag><Key>env</Key><Value>phase2</Value></Tag></TagSet></Tagging>";
const TEST_SSE_S3_WRAPPING_KEY_B64: &str = "YWJjZGVmMDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODk=";

mod model {
    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum Action {
        GetObject,
        GetObjectAttributes,
        GetObjectAcl,
        GetObjectTagging,
        PutObjectTagging,
        DeleteObjectTagging,
    }

    impl fmt::Display for Action {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::GetObject => f.write_str("GetObject"),
                Self::GetObjectAttributes => f.write_str("GetObjectAttributes"),
                Self::GetObjectAcl => f.write_str("GetObjectAcl"),
                Self::GetObjectTagging => f.write_str("GetObjectTagging"),
                Self::PutObjectTagging => f.write_str("PutObjectTagging"),
                Self::DeleteObjectTagging => f.write_str("DeleteObjectTagging"),
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

        fn is_bucket_owner_principal(self) -> bool {
            self.identity == RequesterIdentityShape::BucketOwnerPrincipal
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
        fn all_for_existing() -> impl Iterator<Item = Self> {
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

        fn all_for_missing() -> impl Iterator<Item = Self> {
            // Phase 3 needs public bucket discovery and IgnorePublicAcls coverage without
            // inflating the existing-object matrix from phases 1/2.
            BucketOwnerPrincipalShape::ALL
                .into_iter()
                .flat_map(|owner_principal| {
                    OwnershipShape::ALL.into_iter().flat_map(move |ownership| {
                        let mut shapes = vec![
                            Self {
                                owner_principal,
                                ownership,
                                ignore_public_acls: false,
                                block_public_acls: false,
                                restrict_public_buckets: false,
                                bucket_public_read: false,
                                bucket_public_write: false,
                            },
                            Self {
                                owner_principal,
                                ownership,
                                ignore_public_acls: false,
                                block_public_acls: false,
                                restrict_public_buckets: true,
                                bucket_public_read: false,
                                bucket_public_write: false,
                            },
                        ];
                        if ownership == OwnershipShape::ObjectWriter {
                            shapes.extend([
                                Self {
                                    owner_principal,
                                    ownership,
                                    ignore_public_acls: false,
                                    block_public_acls: false,
                                    restrict_public_buckets: false,
                                    bucket_public_read: true,
                                    bucket_public_write: false,
                                },
                                Self {
                                    owner_principal,
                                    ownership,
                                    ignore_public_acls: true,
                                    block_public_acls: false,
                                    restrict_public_buckets: false,
                                    bucket_public_read: true,
                                    bucket_public_write: false,
                                },
                            ]);
                        }
                        shapes.into_iter()
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
        GrantReadAcpToRequester,
    }

    impl ObjectAclShape {
        const PRIVATE_ONLY: [Self; 1] = [Self::Private];
        const GET_OBJECT_ACL_ALL: [Self; 2] = [Self::Private, Self::GrantReadAcpToRequester];

        fn all_for(action: Action) -> &'static [Self] {
            match action {
                Action::GetObjectAcl => &Self::GET_OBJECT_ACL_ALL,
                Action::GetObject
                | Action::GetObjectAttributes
                | Action::GetObjectTagging
                | Action::PutObjectTagging
                | Action::DeleteObjectTagging => &Self::PRIVATE_ONLY,
            }
        }
    }

    impl fmt::Display for ObjectAclShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Private => f.write_str("private"),
                Self::GrantReadAcpToRequester => f.write_str("grant-read-acp-to-requester"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct ObjectShape {
        pub(super) owner_kind: ObjectOwnerKind,
        pub(super) acl: ObjectAclShape,
    }

    impl ObjectShape {
        fn all_for(action: Action) -> impl Iterator<Item = Self> {
            ObjectOwnerKind::ALL
                .into_iter()
                .flat_map(move |owner_kind| {
                    ObjectAclShape::all_for(action)
                        .iter()
                        .copied()
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
        pub(super) primary: PolicyDecisionShape,
        pub(super) attrs: Option<PolicyDecisionShape>,
    }

    impl PolicyShape {
        fn all_for(action: Action) -> Vec<Self> {
            match action {
                Action::GetObject
                | Action::GetObjectAcl
                | Action::GetObjectTagging
                | Action::PutObjectTagging
                | Action::DeleteObjectTagging => PolicyDecisionShape::ALL
                    .into_iter()
                    .map(|primary| Self {
                        primary,
                        attrs: None,
                    })
                    .collect(),
                Action::GetObjectAttributes => vec![
                    Self {
                        primary: PolicyDecisionShape::NoPolicy,
                        attrs: Some(PolicyDecisionShape::NoPolicy),
                    },
                    Self {
                        primary: PolicyDecisionShape::NoMatch,
                        attrs: Some(PolicyDecisionShape::NoMatch),
                    },
                    Self {
                        primary: PolicyDecisionShape::ExplicitAllowPrivate,
                        attrs: Some(PolicyDecisionShape::ExplicitAllowPrivate),
                    },
                    Self {
                        primary: PolicyDecisionShape::ExplicitAllowPublic,
                        attrs: Some(PolicyDecisionShape::ExplicitAllowPublic),
                    },
                    Self {
                        primary: PolicyDecisionShape::ExplicitDeny,
                        attrs: Some(PolicyDecisionShape::ExplicitDeny),
                    },
                    Self {
                        primary: PolicyDecisionShape::ExplicitAllowPrivate,
                        attrs: Some(PolicyDecisionShape::NoPolicy),
                    },
                    Self {
                        primary: PolicyDecisionShape::NoPolicy,
                        attrs: Some(PolicyDecisionShape::ExplicitAllowPrivate),
                    },
                    Self {
                        primary: PolicyDecisionShape::ExplicitAllowPrivate,
                        attrs: Some(PolicyDecisionShape::ExplicitDeny),
                    },
                    Self {
                        primary: PolicyDecisionShape::ExplicitDeny,
                        attrs: Some(PolicyDecisionShape::ExplicitAllowPrivate),
                    },
                    Self {
                        primary: PolicyDecisionShape::ExplicitAllowPublic,
                        attrs: Some(PolicyDecisionShape::NoPolicy),
                    },
                    Self {
                        primary: PolicyDecisionShape::ExplicitAllowPrivate,
                        attrs: Some(PolicyDecisionShape::ExplicitAllowPublic),
                    },
                    Self {
                        primary: PolicyDecisionShape::ExplicitAllowPublic,
                        attrs: Some(PolicyDecisionShape::ExplicitAllowPrivate),
                    },
                    Self {
                        primary: PolicyDecisionShape::NoPolicy,
                        attrs: Some(PolicyDecisionShape::ExplicitAllowPublic),
                    },
                    Self {
                        primary: PolicyDecisionShape::NoMatch,
                        attrs: Some(PolicyDecisionShape::ExplicitAllowPrivate),
                    },
                    Self {
                        primary: PolicyDecisionShape::ExplicitAllowPrivate,
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
            self.primary.is_public_allow()
                || self.attrs.is_some_and(PolicyDecisionShape::is_public_allow)
        }

        fn has_private_allow(self) -> bool {
            self.primary.is_private_allow()
                || self
                    .attrs
                    .is_some_and(PolicyDecisionShape::is_private_allow)
        }
    }

    impl fmt::Display for PolicyShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "primary={}", self.primary)?;
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
        pub(super) fn existing_scenarios(action: Action) -> Vec<Self> {
            let mut scenarios = Vec::new();
            for target in ExistingTarget::ALL {
                for requester in RequesterShape::all() {
                    for bucket in BucketShape::all_for_existing() {
                        for object in ObjectShape::all_for(action) {
                            for policy in PolicyShape::all_for(action) {
                                let scenario = Self {
                                    action,
                                    target,
                                    requester,
                                    bucket,
                                    object,
                                    policy,
                                };
                                if scenario.existing_is_possible() {
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
                    self.policy_decision_allows(self.policy.primary, self.base_get_object_allowed())
                }
                Action::GetObjectAttributes => {
                    // GetObjectAttributes must satisfy both the GetObject read policy and the
                    // distinct GetObjectAttributes policy; phase 1 keeps these explicit so a
                    // regression in either half of the conjunction fails the matrix.
                    let read_allowed = self.policy_decision_allows(
                        self.policy.primary,
                        self.base_get_object_allowed(),
                    );
                    let attrs_allowed = self.policy_decision_allows(
                        self.policy.attrs_decision(),
                        self.base_get_object_attributes_allowed(),
                    );
                    read_allowed && attrs_allowed
                }
                Action::GetObjectAcl => self.policy_decision_allows(
                    self.policy.primary,
                    self.base_get_object_acl_allowed(),
                ),
                Action::GetObjectTagging
                | Action::PutObjectTagging
                | Action::DeleteObjectTagging => self.policy_decision_allows(
                    self.policy.primary,
                    self.base_object_tagging_allowed(),
                ),
            };

            if allowed {
                Outcome::Allow
            } else {
                Outcome::Deny
            }
        }

        fn existing_is_possible(self) -> bool {
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
            if self.object.acl == ObjectAclShape::GrantReadAcpToRequester
                && self.requester.is_anonymous()
            {
                return false;
            }

            // Phase 1 materializes BOE objects by enabling BOE before the write.
            // Pre-BOE retained-owner cases belong in the later transition phase.
            if self.bucket.ownership == OwnershipShape::BucketOwnerEnforced
                && self.object.owner_kind != ObjectOwnerKind::BucketOwner
            {
                return false;
            }
            if self.bucket.ownership == OwnershipShape::BucketOwnerEnforced
                && self.object.acl != ObjectAclShape::Private
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
                ObjectAclShape::Private | ObjectAclShape::GrantReadAcpToRequester => {
                    self.requester_matches_object_owner()
                }
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

        fn base_get_object_acl_allowed(self) -> bool {
            (self.bucket.ownership == OwnershipShape::BucketOwnerEnforced
                && self.requester.can_bucket_owner_account_admin())
                || self.requester_matches_object_owner()
                || self.requester_has_read_acp_grant()
        }

        fn base_object_tagging_allowed(self) -> bool {
            self.requester.can_bucket_owner_account_admin()
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

        fn requester_has_read_acp_grant(self) -> bool {
            match self.object.acl {
                ObjectAclShape::Private => false,
                ObjectAclShape::GrantReadAcpToRequester => {
                    !self.requester.is_anonymous()
                        && (!self.requester_canonical_matches_object_owner()
                            || self.requester.has_admin_profile())
                }
            }
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
    pub(super) enum MissingTarget {
        Key,
        Version,
    }

    impl MissingTarget {
        const ALL: [Self; 2] = [Self::Key, Self::Version];

        pub(super) fn policy_target(self) -> ExistingTarget {
            match self {
                Self::Key => ExistingTarget::Current,
                Self::Version => ExistingTarget::Versioned,
            }
        }
    }

    impl fmt::Display for MissingTarget {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Key => f.write_str("missing-key"),
                Self::Version => f.write_str("missing-version"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum MissingOutcome {
        RevealMissing,
        HideMissing,
    }

    impl fmt::Display for MissingOutcome {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::RevealMissing => f.write_str("RevealMissing"),
                Self::HideMissing => f.write_str("HideMissing"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct MissingPolicyShape {
        pub(super) read: Option<PolicyDecisionShape>,
        pub(super) attrs: Option<PolicyDecisionShape>,
        pub(super) list: Option<PolicyDecisionShape>,
    }

    impl MissingPolicyShape {
        fn all_for(action: Action) -> Vec<Self> {
            match action {
                Action::GetObject => PolicyDecisionShape::ALL
                    .into_iter()
                    .map(|list| Self {
                        read: None,
                        attrs: None,
                        list: Some(list),
                    })
                    .collect(),
                Action::GetObjectAttributes => PolicyShape::all_for(Action::GetObjectAttributes)
                    .into_iter()
                    .flat_map(|object_policy| {
                        PolicyDecisionShape::ALL.into_iter().map(move |list| Self {
                            read: Some(object_policy.primary),
                            attrs: object_policy.attrs,
                            list: Some(list),
                        })
                    })
                    .collect(),
                Action::GetObjectAcl
                | Action::GetObjectTagging
                | Action::PutObjectTagging
                | Action::DeleteObjectTagging => vec![Self {
                    read: None,
                    attrs: None,
                    list: None,
                }],
            }
        }

        pub(super) fn read_decision(self) -> PolicyDecisionShape {
            self.read
                .expect("missing-object read policy decision must be present for this action")
        }

        pub(super) fn attrs_decision(self) -> PolicyDecisionShape {
            self.attrs
                .expect("missing-object attrs policy decision must be present for this action")
        }

        pub(super) fn list_decision(self) -> PolicyDecisionShape {
            self.list
                .expect("missing-object list policy decision must be present for this action")
        }

        fn has_public_allow(self) -> bool {
            self.read.is_some_and(PolicyDecisionShape::is_public_allow)
                || self.attrs.is_some_and(PolicyDecisionShape::is_public_allow)
                || self.list.is_some_and(PolicyDecisionShape::is_public_allow)
        }

        fn has_private_allow(self) -> bool {
            self.read.is_some_and(PolicyDecisionShape::is_private_allow)
                || self
                    .attrs
                    .is_some_and(PolicyDecisionShape::is_private_allow)
                || self.list.is_some_and(PolicyDecisionShape::is_private_allow)
        }
    }

    impl fmt::Display for MissingPolicyShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            let mut wrote = false;
            if let Some(read) = self.read {
                write!(f, "read={read}")?;
                wrote = true;
            }
            if let Some(attrs) = self.attrs {
                if wrote {
                    f.write_str(" ")?;
                }
                write!(f, "attrs={attrs}")?;
                wrote = true;
            }
            if let Some(list) = self.list {
                if wrote {
                    f.write_str(" ")?;
                }
                write!(f, "list={list}")?;
                wrote = true;
            }
            if !wrote {
                f.write_str("no-policy")
            } else {
                Ok(())
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct MissingScenario {
        pub(super) action: Action,
        pub(super) target: MissingTarget,
        pub(super) requester: RequesterShape,
        pub(super) bucket: BucketShape,
        pub(super) policy: MissingPolicyShape,
    }

    impl MissingScenario {
        pub(super) fn missing_scenarios(action: Action) -> Vec<Self> {
            let mut scenarios = Vec::new();
            for target in MissingTarget::ALL {
                for requester in RequesterShape::all() {
                    for bucket in BucketShape::all_for_missing() {
                        for policy in MissingPolicyShape::all_for(action) {
                            let scenario = Self {
                                action,
                                target,
                                requester,
                                bucket,
                                policy,
                            };
                            if scenario.is_possible() {
                                scenarios.push(scenario);
                            }
                        }
                    }
                }
            }
            scenarios
        }

        pub(super) fn expected_outcome(self) -> MissingOutcome {
            let reveal = match self.action {
                Action::GetObject => {
                    self.base_get_object_discovery_allowed() || self.list_bucket_allowed()
                }
                Action::GetObjectAttributes => {
                    let read_allowed = self.policy_decision_allows(
                        self.policy.read_decision(),
                        self.base_get_object_discovery_allowed(),
                    );
                    let attrs_allowed = self.policy_decision_allows(
                        self.policy.attrs_decision(),
                        self.base_get_object_attributes_discovery_allowed(),
                    );
                    read_allowed && attrs_allowed && self.list_bucket_allowed()
                }
                Action::GetObjectAcl => self.base_get_object_acl_discovery_allowed(),
                Action::GetObjectTagging
                | Action::PutObjectTagging
                | Action::DeleteObjectTagging => self.requester.can_bucket_owner_account_admin(),
            };

            if reveal {
                MissingOutcome::RevealMissing
            } else {
                MissingOutcome::HideMissing
            }
        }

        fn is_possible(self) -> bool {
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

            true
        }

        fn base_get_object_discovery_allowed(self) -> bool {
            self.bucket_read_allowed()
                || (self.bucket.ownership == OwnershipShape::BucketOwnerEnforced
                    && self.requester.can_bucket_owner_account_admin())
        }

        fn base_get_object_attributes_discovery_allowed(self) -> bool {
            if self.bucket.ownership == OwnershipShape::BucketOwnerEnforced {
                return self.requester.is_bucket_owner_principal();
            }

            self.bucket_read_allowed()
        }

        fn base_get_object_acl_discovery_allowed(self) -> bool {
            self.requester.is_bucket_owner_principal()
                || (self.bucket.ownership == OwnershipShape::BucketOwnerEnforced
                    && self.requester.can_bucket_owner_account_admin())
        }

        fn bucket_read_allowed(self) -> bool {
            self.requester.is_bucket_owner_principal()
                || self.requester_has_bucket_acl_read()
                || (self.bucket.bucket_public_read && !self.bucket.ignore_public_acls)
        }

        fn requester_has_bucket_acl_read(self) -> bool {
            self.requester.identity
                == RequesterIdentityShape::SameAccountSharedCanonicalOtherPrincipal
                && self.requester.has_admin_profile()
        }

        fn list_bucket_allowed(self) -> bool {
            self.policy_decision_allows(self.policy.list_decision(), self.bucket_read_allowed())
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
    }

    impl fmt::Display for MissingScenario {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "action={} target={} requester={} bucket={} policy={}",
                self.action, self.target, self.requester, self.bucket, self.policy
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
        Action, BucketOwnerPrincipalShape, BucketShape, ExistingTarget, MissingOutcome,
        MissingScenario, MissingTarget, ObjectAclShape, ObjectOwnerKind, Outcome, OwnershipShape,
        PolicyDecisionShape, RequesterIdentityShape, RequesterShape, Scenario,
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

        fn object_owner_account(
            &self,
            owner_principal: BucketOwnerPrincipalShape,
            owner: ObjectOwnerKind,
        ) -> &AccountIdentity {
            match owner {
                ObjectOwnerKind::BucketOwner => self.bucket_owner_account(owner_principal),
                ObjectOwnerKind::SameAccountSharedOther => {
                    self.shared_other_account(owner_principal)
                }
                ObjectOwnerKind::SameAccountDistinct => &self.same_account_distinct,
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

        fn requester_account(
            &self,
            owner_principal: BucketOwnerPrincipalShape,
            shape: RequesterShape,
        ) -> Option<&AccountIdentity> {
            match shape.identity {
                RequesterIdentityShape::Anonymous => None,
                RequesterIdentityShape::BucketOwnerPrincipal => {
                    Some(self.bucket_owner_account(owner_principal))
                }
                RequesterIdentityShape::SameAccountSharedCanonicalOtherPrincipal => {
                    Some(self.shared_other_account(owner_principal))
                }
                RequesterIdentityShape::SameAccountDistinctPrincipal => {
                    Some(&self.same_account_distinct)
                }
                RequesterIdentityShape::CrossAccountPrincipal => Some(&self.cross_account),
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

        pub(super) fn run_missing(
            &self,
            bucket: &str,
            scenario: MissingScenario,
        ) -> ClassifiedResult {
            materialize_missing_bucket(&self.coord, &self.fixtures, bucket, scenario)
                .unwrap_or_else(|err| {
                    panic!("failed to materialize bucket for {scenario}: {err:?}");
                });

            let (key, version_id) =
                materialize_missing_target(&self.coord, &self.fixtures, bucket, scenario)
                    .unwrap_or_else(|err| {
                        panic!("failed to materialize missing target for {scenario}: {err:?}");
                    });

            materialize_missing_policy(&self.coord, &self.fixtures, bucket, scenario, key)
                .unwrap_or_else(|err| {
                    panic!("failed to materialize policy for {scenario}: {err:?}");
                });

            classify(run_missing_action(
                &self.coord,
                &self.fixtures,
                bucket,
                scenario,
                key,
                version_id,
            ))
        }
    }

    pub(super) fn bucket_name_for(action: Action, index: usize) -> String {
        let action_tag = match action {
            Action::GetObject => "go",
            Action::GetObjectAttributes => "goa",
            Action::GetObjectAcl => "goacl",
            Action::GetObjectTagging => "gotag",
            Action::PutObjectTagging => "potag",
            Action::DeleteObjectTagging => "dotag",
        };
        format!("{BUCKET_PREFIX}-{action_tag}-{index:05}")
    }

    pub(super) fn to_existing_outcome(result: ClassifiedResult) -> Outcome {
        match result {
            ClassifiedResult::Allow => Outcome::Allow,
            ClassifiedResult::AccessDenied => Outcome::Deny,
            ClassifiedResult::NoSuchKey | ClassifiedResult::VersionNotFound => {
                panic!("existing-object matrix produced an impossible missing-object result")
            }
        }
    }

    pub(super) fn to_missing_outcome(
        result: ClassifiedResult,
        target: MissingTarget,
    ) -> MissingOutcome {
        match (result, target) {
            (ClassifiedResult::AccessDenied, _) => MissingOutcome::HideMissing,
            (ClassifiedResult::NoSuchKey, MissingTarget::Key) => MissingOutcome::RevealMissing,
            (ClassifiedResult::VersionNotFound, MissingTarget::Version) => {
                MissingOutcome::RevealMissing
            }
            (ClassifiedResult::Allow, _) => {
                panic!("missing-object matrix produced an impossible allow result")
            }
            (ClassifiedResult::NoSuchKey, MissingTarget::Version) => {
                panic!("missing-version matrix produced NoSuchKey instead of VersionNotFound")
            }
            (ClassifiedResult::VersionNotFound, MissingTarget::Key) => {
                panic!("missing-key matrix produced VersionNotFound instead of NoSuchKey")
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
        materialize_bucket_shape(coord, fixtures, bucket, scenario.bucket)
    }

    fn materialize_missing_bucket(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: MissingScenario,
    ) -> Result<(), ServerError> {
        materialize_bucket_shape(coord, fixtures, bucket, scenario.bucket)
    }

    fn materialize_bucket_shape(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        shape: BucketShape,
    ) -> Result<(), ServerError> {
        let owner_account = fixtures.bucket_owner_account(shape.owner_principal);
        let owner = OwnerIdentity::new(
            owner_account.principal(),
            owner_account.canonical_user_id().clone(),
        );
        let grants = Coordinator::bucket_acl_grants_from_flags(
            &owner,
            shape.bucket_public_read,
            shape.bucket_public_write,
        );
        coord.create_bucket_with_acl_grants(&owner, bucket, grants, false)?;

        coord.put_bucket_versioning(&PutBucketVersioningRequest {
            bucket: BucketRequest::new(
                bucket,
                fixtures.bucket_owner_requester(shape.owner_principal),
                None,
            ),
            state: BucketVersioningState::Enabled,
        })?;

        if shape.ownership == OwnershipShape::BucketOwnerEnforced {
            coord.put_bucket_ownership_controls(&PutBucketOwnershipControlsRequest {
                bucket: BucketRequest::new(
                    bucket,
                    fixtures.bucket_owner_requester(shape.owner_principal),
                    None,
                ),
                config: BucketOwnershipControls {
                    object_ownership: BucketObjectOwnership::BucketOwnerEnforced,
                },
            })?;
        }

        if shape.block_public_acls || shape.ignore_public_acls || shape.restrict_public_buckets {
            coord.put_bucket_public_access_block(&PutBucketPublicAccessBlockRequest {
                bucket: BucketRequest::new(
                    bucket,
                    fixtures.bucket_owner_requester(shape.owner_principal),
                    None,
                ),
                config: PublicAccessBlockConfig {
                    block_public_acls: shape.block_public_acls,
                    ignore_public_acls: shape.ignore_public_acls,
                    block_public_policy: false,
                    restrict_public_buckets: shape.restrict_public_buckets,
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
                acl: object_write_acl(fixtures, scenario),
            },
        )?;
        Ok(put.version_id)
    }

    fn materialize_missing_target(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: MissingScenario,
    ) -> Result<(&'static str, Option<VersionId>), ServerError> {
        match scenario.target {
            MissingTarget::Key => Ok((MISSING_KEY, None)),
            MissingTarget::Version => {
                let existing_version = materialize_bucket_owner_object(
                    coord,
                    fixtures,
                    bucket,
                    scenario.bucket.owner_principal,
                )?;
                Ok((
                    KEY,
                    Some(VersionId::from_u64(existing_version.to_u64() + 1000)),
                ))
            }
        }
    }

    fn materialize_bucket_owner_object(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        owner_principal: BucketOwnerPrincipalShape,
    ) -> Result<VersionId, ServerError> {
        let put = test_helpers::put_object(
            coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: ObjectRequest::new(
                    bucket,
                    KEY,
                    fixtures.bucket_owner_requester(owner_principal),
                    None,
                ),
                data: b"phase-3-data",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,
                acl: PutObjectWriteAcl::None,
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

    fn materialize_missing_policy(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: MissingScenario,
        key: &str,
    ) -> Result<(), ServerError> {
        let Some(policy) = missing_policy_document(fixtures, bucket, scenario, key) else {
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
        let object = ObjectVersionRequest::new(bucket, KEY, version_id, requester, None);

        run_action_for_object(coord, scenario.action, object)
    }

    fn run_missing_action(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: MissingScenario,
        key: &str,
        version_id: Option<VersionId>,
    ) -> Result<(), ServerError> {
        let requester = fixtures.requester(scenario.bucket.owner_principal, scenario.requester);
        let object = ObjectVersionRequest::new(bucket, key, version_id, requester, None);

        run_action_for_object(coord, scenario.action, object)
    }

    fn run_action_for_object(
        coord: &Coordinator,
        action: Action,
        object: ObjectVersionRequest<'_>,
    ) -> Result<(), ServerError> {
        match action {
            Action::GetObject => coord
                .get_object(&GetObjectRequest {
                    sse_customer: None,
                    object,
                    cond: NO_READ,
                })
                .and_then(|result| {
                    let _ = read_all_body(result.body)?;
                    Ok(())
                }),
            Action::GetObjectAttributes => coord
                .get_object_attributes(&GetObjectAttributesRequest {
                    object,
                    cond: NO_READ,
                    want_parts: false,
                    part_number_marker: None,
                    max_parts: 0,
                    sse_customer: None,
                })
                .map(|_| ()),
            Action::GetObjectAcl => coord.get_object_acl(&object).map(|_| ()),
            Action::GetObjectTagging => coord.get_object_tags(&object).map(|_| ()),
            Action::PutObjectTagging => coord
                .put_object_tags(&PutObjectTagsRequest {
                    object,
                    tags: TAGS_XML,
                })
                .map(|_| ()),
            Action::DeleteObjectTagging => coord.delete_object_tags(&object).map(|_| ()),
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

    fn object_write_acl(
        fixtures: &IdentityFixtures,
        scenario: Scenario,
    ) -> PutObjectWriteAcl<'static> {
        match scenario.object.acl {
            ObjectAclShape::Private => PutObjectWriteAcl::None,
            ObjectAclShape::GrantReadAcpToRequester => {
                let owner = fixtures.object_owner_account(
                    scenario.bucket.owner_principal,
                    scenario.object.owner_kind,
                );
                let requester = fixtures
                    .requester_account(scenario.bucket.owner_principal, scenario.requester)
                    .expect("explicit requester ACL grants require an authenticated requester");
                PutObjectWriteAcl::Grants(AclGrants::new(vec![
                    AclGrant::new(
                        AclGrantee::CanonicalUser(owner.canonical_user_id().clone()),
                        AclPermission::FullControl,
                    ),
                    AclGrant::new(
                        AclGrantee::CanonicalUser(requester.canonical_user_id().clone()),
                        AclPermission::ReadAcp,
                    ),
                ]))
            }
        }
    }

    fn policy_document(
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: Scenario,
    ) -> Option<String> {
        let mut statements = Vec::new();
        let requester_principal =
            fixtures.requester_principal(scenario.bucket.owner_principal, scenario.requester);
        let object_resource = format!("arn:aws:s3:::{bucket}/{KEY}");
        push_policy_statement(
            &mut statements,
            requester_principal,
            policy_action_name(scenario.action, scenario.target),
            &object_resource,
            scenario.policy.primary,
        );
        if scenario.action == Action::GetObjectAttributes {
            push_policy_statement(
                &mut statements,
                requester_principal,
                get_object_attributes_policy_action_name(scenario.target),
                &object_resource,
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

    fn missing_policy_document(
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: MissingScenario,
        key: &str,
    ) -> Option<String> {
        let mut statements = Vec::new();
        let requester_principal =
            fixtures.requester_principal(scenario.bucket.owner_principal, scenario.requester);
        let object_resource = format!("arn:aws:s3:::{bucket}/{key}");
        let bucket_resource = format!("arn:aws:s3:::{bucket}");

        match scenario.action {
            Action::GetObject => push_policy_statement(
                &mut statements,
                requester_principal,
                "s3:ListBucket",
                &bucket_resource,
                scenario.policy.list_decision(),
            ),
            Action::GetObjectAttributes => {
                let target = scenario.target.policy_target();
                push_policy_statement(
                    &mut statements,
                    requester_principal,
                    get_object_policy_action_name(target),
                    &object_resource,
                    scenario.policy.read_decision(),
                );
                push_policy_statement(
                    &mut statements,
                    requester_principal,
                    get_object_attributes_policy_action_name(target),
                    &object_resource,
                    scenario.policy.attrs_decision(),
                );
                push_policy_statement(
                    &mut statements,
                    requester_principal,
                    "s3:ListBucket",
                    &bucket_resource,
                    scenario.policy.list_decision(),
                );
            }
            Action::GetObjectAcl
            | Action::GetObjectTagging
            | Action::PutObjectTagging
            | Action::DeleteObjectTagging => {}
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
        requester_principal: Option<&str>,
        action: &str,
        resource: &str,
        decision: PolicyDecisionShape,
    ) {
        match decision {
            PolicyDecisionShape::NoPolicy => {}
            PolicyDecisionShape::NoMatch => statements.push(format!(
                r#"{{"Effect":"Allow","Principal":{{"AWS":"arn:aws:iam::999988887777:user/unmatched"}},"Action":"{action}","Resource":"{resource}"}}"#
            )),
            PolicyDecisionShape::ExplicitAllowPrivate => {
                let principal = requester_principal
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

    fn get_object_acl_policy_action_name(target: ExistingTarget) -> &'static str {
        match target {
            ExistingTarget::Current => "s3:GetObjectAcl",
            ExistingTarget::Versioned => "s3:GetObjectVersionAcl",
        }
    }

    fn get_object_tagging_policy_action_name(target: ExistingTarget) -> &'static str {
        match target {
            ExistingTarget::Current => "s3:GetObjectTagging",
            ExistingTarget::Versioned => "s3:GetObjectVersionTagging",
        }
    }

    fn put_object_tagging_policy_action_name(target: ExistingTarget) -> &'static str {
        match target {
            ExistingTarget::Current => "s3:PutObjectTagging",
            ExistingTarget::Versioned => "s3:PutObjectVersionTagging",
        }
    }

    fn delete_object_tagging_policy_action_name(target: ExistingTarget) -> &'static str {
        match target {
            ExistingTarget::Current => "s3:DeleteObjectTagging",
            ExistingTarget::Versioned => "s3:DeleteObjectVersionTagging",
        }
    }

    fn policy_action_name(action: Action, target: ExistingTarget) -> &'static str {
        match action {
            Action::GetObject => get_object_policy_action_name(target),
            Action::GetObjectAttributes => get_object_policy_action_name(target),
            Action::GetObjectAcl => get_object_acl_policy_action_name(target),
            Action::GetObjectTagging => get_object_tagging_policy_action_name(target),
            Action::PutObjectTagging => put_object_tagging_policy_action_name(target),
            Action::DeleteObjectTagging => delete_object_tagging_policy_action_name(target),
        }
    }
}

use harness::{bucket_name_for, to_existing_outcome, to_missing_outcome, MatrixHarness};
use model::{Action, MissingScenario, Scenario};

#[test]
fn authz_model_phase1_get_object_existing_matrix() {
    run_existing_matrix("phase 1", Action::GetObject);
}

#[test]
fn authz_model_phase1_get_object_attributes_existing_matrix() {
    run_existing_matrix("phase 1", Action::GetObjectAttributes);
}

#[test]
fn authz_model_phase2_get_object_acl_existing_matrix() {
    run_existing_matrix("phase 2", Action::GetObjectAcl);
}

#[test]
fn authz_model_phase2_get_object_tagging_existing_matrix() {
    run_existing_matrix("phase 2", Action::GetObjectTagging);
}

#[test]
fn authz_model_phase2_put_object_tagging_existing_matrix() {
    run_existing_matrix("phase 2", Action::PutObjectTagging);
}

#[test]
fn authz_model_phase2_delete_object_tagging_existing_matrix() {
    run_existing_matrix("phase 2", Action::DeleteObjectTagging);
}

#[test]
fn authz_model_phase3_get_object_missing_matrix() {
    run_missing_matrix("phase 3", Action::GetObject);
}

#[test]
fn authz_model_phase3_get_object_attributes_missing_matrix() {
    run_missing_matrix("phase 3", Action::GetObjectAttributes);
}

#[test]
fn authz_model_phase3_get_object_acl_missing_matrix() {
    run_missing_matrix("phase 3", Action::GetObjectAcl);
}

#[test]
fn authz_model_phase3_get_object_tagging_missing_matrix() {
    run_missing_matrix("phase 3", Action::GetObjectTagging);
}

#[test]
fn authz_model_phase3_put_object_tagging_missing_matrix() {
    run_missing_matrix("phase 3", Action::PutObjectTagging);
}

#[test]
fn authz_model_phase3_delete_object_tagging_missing_matrix() {
    run_missing_matrix("phase 3", Action::DeleteObjectTagging);
}

fn run_existing_matrix(phase: &str, action: Action) {
    let scenarios = Scenario::existing_scenarios(action);
    assert!(
        !scenarios.is_empty(),
        "{phase} matrix unexpectedly produced no scenarios for {action}"
    );
    let harness = MatrixHarness::new();

    for (index, scenario) in scenarios.into_iter().enumerate() {
        let bucket = bucket_name_for(action, index);
        let expected = scenario.expected_existing_outcome();
        let actual = to_existing_outcome(harness.run_existing(&bucket, scenario));
        assert_eq!(
            actual, expected,
            "{phase} authz model mismatch\nscenario: {scenario}\nexpected: {expected}\nactual: {actual}"
        );
    }
}

fn run_missing_matrix(phase: &str, action: Action) {
    let scenarios = MissingScenario::missing_scenarios(action);
    assert!(
        !scenarios.is_empty(),
        "{phase} matrix unexpectedly produced no scenarios for {action}"
    );
    let harness = MatrixHarness::new();

    for (index, scenario) in scenarios.into_iter().enumerate() {
        let bucket = bucket_name_for(action, index);
        let expected = scenario.expected_outcome();
        let actual = to_missing_outcome(harness.run_missing(&bucket, scenario), scenario.target);
        assert_eq!(
            actual, expected,
            "{phase} authz model mismatch\nscenario: {scenario}\nexpected: {expected}\nactual: {actual}"
        );
    }
}
