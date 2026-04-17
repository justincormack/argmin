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
        pub(super) const ALL: [Self; 5] = [
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
    pub(super) struct IdentityFixtures {
        pub(super) root: AccountIdentity,
        pub(super) owner_user: AccountIdentity,
        pub(super) same_account_distinct: AccountIdentity,
        pub(super) cross_account: AccountIdentity,
    }

    impl IdentityFixtures {
        pub(super) fn new() -> Self {
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

    pub(super) fn setup_coordinator(dir: &Path) -> Coordinator {
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
                trusted_bucket_name(bucket),
                fixtures.bucket_owner_requester(shape.owner_principal),
                None,
            ),
            state: BucketVersioningState::Enabled,
        })?;

        if shape.ownership == OwnershipShape::BucketOwnerEnforced {
            coord.put_bucket_ownership_controls(&PutBucketOwnershipControlsRequest {
                bucket: BucketRequest::new(
                    trusted_bucket_name(bucket),
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
                    trusted_bucket_name(bucket),
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
                    trusted_bucket_name(bucket),
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
                object: ObjectRequest::new(
                    trusted_bucket_name(bucket),
                    trusted_object_key(KEY),
                    writer,
                    None,
                ),
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
                    trusted_bucket_name(bucket),
                    trusted_object_key(KEY),
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
                    trusted_bucket_name(bucket),
                    fixtures.bucket_owner_requester(scenario.bucket.owner_principal),
                    None,
                ))?;
            }
            return Ok(());
        };

        coord.put_bucket_policy(&PutBucketPolicyRequest {
            bucket: BucketRequest::new(
                trusted_bucket_name(bucket),
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
                trusted_bucket_name(bucket),
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
        let object = ObjectVersionRequest::new(
            trusted_bucket_name(bucket),
            trusted_object_key(KEY),
            version_id,
            requester,
            None,
        );

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
        let object = ObjectVersionRequest::new(
            trusted_bucket_name(bucket),
            trusted_object_key(key),
            version_id,
            requester,
            None,
        );

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

mod phase4_model {
    use super::model::{OwnershipShape, PolicyDecisionShape};
    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum WriteAction {
        PutObject,
        CreateMultipartUpload,
        BeginStreamPut,
    }

    impl fmt::Display for WriteAction {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::PutObject => f.write_str("PutObject"),
                Self::CreateMultipartUpload => f.write_str("CreateMultipartUpload"),
                Self::BeginStreamPut => f.write_str("BeginStreamPut"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum WriteRequesterShape {
        Anonymous,
        BucketOwnerPrincipal,
        SameAccountOtherPrincipal,
        SameAccountDistinctAdmin,
        CrossAccountPrincipal,
    }

    impl WriteRequesterShape {
        const ALL: [Self; 5] = [
            Self::Anonymous,
            Self::BucketOwnerPrincipal,
            Self::SameAccountOtherPrincipal,
            Self::SameAccountDistinctAdmin,
            Self::CrossAccountPrincipal,
        ];

        fn is_anonymous(self) -> bool {
            self == Self::Anonymous
        }

        fn is_bucket_owner_account(self) -> bool {
            matches!(
                self,
                Self::BucketOwnerPrincipal
                    | Self::SameAccountOtherPrincipal
                    | Self::SameAccountDistinctAdmin
            )
        }

        fn is_bucket_owner_account_admin(self) -> bool {
            matches!(
                self,
                Self::BucketOwnerPrincipal | Self::SameAccountDistinctAdmin
            )
        }
    }

    impl fmt::Display for WriteRequesterShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Anonymous => f.write_str("anonymous"),
                Self::BucketOwnerPrincipal => f.write_str("bucket-owner-principal"),
                Self::SameAccountOtherPrincipal => f.write_str("same-account-other"),
                Self::SameAccountDistinctAdmin => f.write_str("same-account-admin"),
                Self::CrossAccountPrincipal => f.write_str("cross-account"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct WriteBucketShape {
        pub(super) ownership: OwnershipShape,
        pub(super) block_public_acls: bool,
        pub(super) ignore_public_acls: bool,
        pub(super) restrict_public_buckets: bool,
        pub(super) bucket_public_write: bool,
    }

    impl WriteBucketShape {
        pub(super) fn all() -> impl Iterator<Item = Self> {
            [
                Self {
                    ownership: OwnershipShape::ObjectWriter,
                    block_public_acls: false,
                    ignore_public_acls: false,
                    restrict_public_buckets: false,
                    bucket_public_write: false,
                },
                Self {
                    ownership: OwnershipShape::ObjectWriter,
                    block_public_acls: false,
                    ignore_public_acls: false,
                    restrict_public_buckets: false,
                    bucket_public_write: true,
                },
                Self {
                    ownership: OwnershipShape::ObjectWriter,
                    block_public_acls: false,
                    ignore_public_acls: true,
                    restrict_public_buckets: false,
                    bucket_public_write: true,
                },
                Self {
                    ownership: OwnershipShape::ObjectWriter,
                    block_public_acls: true,
                    ignore_public_acls: false,
                    restrict_public_buckets: false,
                    bucket_public_write: false,
                },
                Self {
                    ownership: OwnershipShape::ObjectWriter,
                    block_public_acls: true,
                    ignore_public_acls: false,
                    restrict_public_buckets: false,
                    bucket_public_write: true,
                },
                Self {
                    ownership: OwnershipShape::ObjectWriter,
                    block_public_acls: false,
                    ignore_public_acls: false,
                    restrict_public_buckets: true,
                    bucket_public_write: false,
                },
                Self {
                    ownership: OwnershipShape::BucketOwnerEnforced,
                    block_public_acls: false,
                    ignore_public_acls: false,
                    restrict_public_buckets: false,
                    bucket_public_write: false,
                },
            ]
            .into_iter()
        }
    }

    impl fmt::Display for WriteBucketShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "{}(block_public_acls={}, ignore_public_acls={}, restrict_public_buckets={}, public_write={})",
                self.ownership,
                self.block_public_acls,
                self.ignore_public_acls,
                self.restrict_public_buckets,
                self.bucket_public_write
            )
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum RequestedAclShape {
        None,
        CannedPrivate,
        CannedPublicRead,
        CannedAuthenticatedRead,
        CannedBucketOwnerFullControl,
        GrantsOwnerFullControlOnly,
        GrantsRequesterRead,
        GrantsAuthenticatedUsersRead,
    }

    impl RequestedAclShape {
        const WRITE_ALL: [Self; 8] = [
            Self::None,
            Self::CannedPrivate,
            Self::CannedPublicRead,
            Self::CannedAuthenticatedRead,
            Self::CannedBucketOwnerFullControl,
            Self::GrantsOwnerFullControlOnly,
            Self::GrantsRequesterRead,
            Self::GrantsAuthenticatedUsersRead,
        ];

        const ACL_UPDATE_ALL: [Self; 4] = [
            Self::CannedPrivate,
            Self::CannedPublicRead,
            Self::GrantsRequesterRead,
            Self::GrantsAuthenticatedUsersRead,
        ];

        fn is_public_acl(self) -> bool {
            matches!(
                self,
                Self::CannedPublicRead
                    | Self::CannedAuthenticatedRead
                    | Self::GrantsAuthenticatedUsersRead
            )
        }

        fn is_public_canned_acl(self) -> bool {
            self == Self::CannedPublicRead
        }

        fn is_canned(self) -> bool {
            matches!(
                self,
                Self::CannedPrivate
                    | Self::CannedPublicRead
                    | Self::CannedAuthenticatedRead
                    | Self::CannedBucketOwnerFullControl
            )
        }

        fn is_supported_under_boe(self) -> bool {
            matches!(
                self,
                Self::None
                    | Self::CannedPrivate
                    | Self::CannedBucketOwnerFullControl
                    | Self::GrantsOwnerFullControlOnly
            )
        }

        pub(super) fn policy_condition_value(self) -> Option<&'static str> {
            match self {
                Self::None
                | Self::GrantsOwnerFullControlOnly
                | Self::GrantsRequesterRead
                | Self::GrantsAuthenticatedUsersRead => None,
                Self::CannedPrivate => Some("private"),
                Self::CannedPublicRead => Some("public-read"),
                Self::CannedAuthenticatedRead => Some("authenticated-read"),
                Self::CannedBucketOwnerFullControl => Some("bucket-owner-full-control"),
            }
        }
    }

    impl fmt::Display for RequestedAclShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::None => f.write_str("none"),
                Self::CannedPrivate => f.write_str("canned-private"),
                Self::CannedPublicRead => f.write_str("canned-public-read"),
                Self::CannedAuthenticatedRead => f.write_str("canned-authenticated-read"),
                Self::CannedBucketOwnerFullControl => {
                    f.write_str("canned-bucket-owner-full-control")
                }
                Self::GrantsOwnerFullControlOnly => f.write_str("grants-owner-full-control"),
                Self::GrantsRequesterRead => f.write_str("grants-requester-read"),
                Self::GrantsAuthenticatedUsersRead => {
                    f.write_str("grants-authenticated-users-read")
                }
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum WriteOutcome {
        Allow,
        Deny,
        AclNotSupported,
    }

    impl fmt::Display for WriteOutcome {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Allow => f.write_str("Allow"),
                Self::Deny => f.write_str("Deny"),
                Self::AclNotSupported => f.write_str("AclNotSupported"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct WriteScenario {
        pub(super) action: WriteAction,
        pub(super) requester: WriteRequesterShape,
        pub(super) bucket: WriteBucketShape,
        pub(super) acl: RequestedAclShape,
        pub(super) policy: PolicyDecisionShape,
    }

    impl WriteScenario {
        pub(super) fn scenarios(action: WriteAction) -> Vec<Self> {
            let mut scenarios = Vec::new();
            for requester in WriteRequesterShape::ALL {
                for bucket in WriteBucketShape::all() {
                    for acl in RequestedAclShape::WRITE_ALL {
                        for policy in PolicyDecisionShape::ALL {
                            let scenario = Self {
                                action,
                                requester,
                                bucket,
                                acl,
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

        pub(super) fn expected_outcome(self) -> WriteOutcome {
            if self.action == WriteAction::CreateMultipartUpload && self.requester.is_anonymous() {
                return WriteOutcome::Deny;
            }
            let fallback = self.requester.is_bucket_owner_account_admin()
                || (self.bucket.bucket_public_write && !self.bucket.ignore_public_acls);
            let allowed = match self.policy {
                PolicyDecisionShape::ExplicitDeny => false,
                PolicyDecisionShape::ExplicitAllowPrivate => true,
                PolicyDecisionShape::ExplicitAllowPublic => {
                    if !self.bucket.restrict_public_buckets
                        || self.requester.is_bucket_owner_account()
                    {
                        true
                    } else {
                        fallback
                    }
                }
                PolicyDecisionShape::NoPolicy | PolicyDecisionShape::NoMatch => fallback,
            };
            if !allowed {
                return WriteOutcome::Deny;
            }
            if self.bucket.ownership == OwnershipShape::BucketOwnerEnforced
                && !self.acl.is_supported_under_boe()
            {
                return WriteOutcome::AclNotSupported;
            }
            if self.bucket.block_public_acls && self.acl.is_public_acl() {
                return WriteOutcome::Deny;
            }
            WriteOutcome::Allow
        }

        fn is_possible(self) -> bool {
            if self.requester.is_anonymous()
                && self.policy == PolicyDecisionShape::ExplicitAllowPrivate
            {
                return false;
            }
            if self.bucket.restrict_public_buckets
                && self.policy != PolicyDecisionShape::ExplicitAllowPublic
            {
                return false;
            }
            if self.bucket.ignore_public_acls && !self.bucket.bucket_public_write {
                return false;
            }
            true
        }
    }

    impl fmt::Display for WriteScenario {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "action={} requester={} bucket={} acl={} policy={}",
                self.action, self.requester, self.bucket, self.acl, self.policy
            )
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum AclUpdateAction {
        PutObjectAcl,
        PutObjectVersionAcl,
    }

    impl fmt::Display for AclUpdateAction {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::PutObjectAcl => f.write_str("PutObjectAcl"),
                Self::PutObjectVersionAcl => f.write_str("PutObjectVersionAcl"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum AclRequesterShape {
        Anonymous,
        BucketOwnerPrincipal,
        SameAccountOtherPrincipal,
        SameAccountOtherAdmin,
        SameAccountDistinctAdmin,
        CrossAccountPrincipal,
    }

    impl AclRequesterShape {
        const ALL: [Self; 6] = [
            Self::Anonymous,
            Self::BucketOwnerPrincipal,
            Self::SameAccountOtherPrincipal,
            Self::SameAccountOtherAdmin,
            Self::SameAccountDistinctAdmin,
            Self::CrossAccountPrincipal,
        ];

        fn is_anonymous(self) -> bool {
            self == Self::Anonymous
        }

        fn is_bucket_owner_account(self) -> bool {
            matches!(
                self,
                Self::BucketOwnerPrincipal
                    | Self::SameAccountOtherPrincipal
                    | Self::SameAccountOtherAdmin
                    | Self::SameAccountDistinctAdmin
            )
        }

        fn is_bucket_owner_account_admin(self) -> bool {
            matches!(
                self,
                Self::BucketOwnerPrincipal
                    | Self::SameAccountOtherAdmin
                    | Self::SameAccountDistinctAdmin
            )
        }

        fn has_owner_account_admin_profile(self) -> bool {
            matches!(
                self,
                Self::SameAccountOtherAdmin | Self::SameAccountDistinctAdmin
            )
        }
    }

    impl fmt::Display for AclRequesterShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Anonymous => f.write_str("anonymous"),
                Self::BucketOwnerPrincipal => f.write_str("bucket-owner-principal"),
                Self::SameAccountOtherPrincipal => f.write_str("same-account-other"),
                Self::SameAccountOtherAdmin => f.write_str("same-account-other-admin"),
                Self::SameAccountDistinctAdmin => f.write_str("same-account-distinct-admin"),
                Self::CrossAccountPrincipal => f.write_str("cross-account"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum AclOwnerKind {
        BucketOwner,
        SameAccountOther,
    }

    impl AclOwnerKind {
        const ALL: [Self; 2] = [Self::BucketOwner, Self::SameAccountOther];
    }

    impl fmt::Display for AclOwnerKind {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::BucketOwner => f.write_str("bucket-owner"),
                Self::SameAccountOther => f.write_str("same-account-other"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum ExistingAclShape {
        Private,
        GrantWriteAcpToRequester,
    }

    impl ExistingAclShape {
        const ALL: [Self; 2] = [Self::Private, Self::GrantWriteAcpToRequester];
    }

    impl fmt::Display for ExistingAclShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Private => f.write_str("private"),
                Self::GrantWriteAcpToRequester => f.write_str("grant-write-acp-to-requester"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct AclBucketShape {
        pub(super) ownership: OwnershipShape,
        pub(super) block_public_acls: bool,
        pub(super) restrict_public_buckets: bool,
    }

    impl AclBucketShape {
        fn all() -> impl Iterator<Item = Self> {
            [
                Self {
                    ownership: OwnershipShape::ObjectWriter,
                    block_public_acls: false,
                    restrict_public_buckets: false,
                },
                Self {
                    ownership: OwnershipShape::ObjectWriter,
                    block_public_acls: true,
                    restrict_public_buckets: false,
                },
                Self {
                    ownership: OwnershipShape::ObjectWriter,
                    block_public_acls: false,
                    restrict_public_buckets: true,
                },
                Self {
                    ownership: OwnershipShape::BucketOwnerEnforced,
                    block_public_acls: false,
                    restrict_public_buckets: false,
                },
            ]
            .into_iter()
        }
    }

    impl fmt::Display for AclBucketShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "{}(block_public_acls={}, restrict_public_buckets={})",
                self.ownership, self.block_public_acls, self.restrict_public_buckets
            )
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum AclPolicyShape {
        NoPolicy,
        NoMatch,
        AllowPrivate,
        AllowPublic,
        AllowWithPublicCannedDeny,
        AllowWithGrantReadCondition,
    }

    impl AclPolicyShape {
        const ALL: [Self; 6] = [
            Self::NoPolicy,
            Self::NoMatch,
            Self::AllowPrivate,
            Self::AllowPublic,
            Self::AllowWithPublicCannedDeny,
            Self::AllowWithGrantReadCondition,
        ];
    }

    impl fmt::Display for AclPolicyShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::NoPolicy => f.write_str("no-policy"),
                Self::NoMatch => f.write_str("no-match"),
                Self::AllowPrivate => f.write_str("allow-private"),
                Self::AllowPublic => f.write_str("allow-public"),
                Self::AllowWithPublicCannedDeny => f.write_str("allow-with-public-canned-deny"),
                Self::AllowWithGrantReadCondition => f.write_str("allow-with-grant-read-condition"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum AclContextShape {
        Exact,
        CannedAclMismatch,
        CannedAclWithGrantHeader,
        GrantReadMismatch,
        GrantInputWithCannedAcl,
    }

    impl AclContextShape {
        const ALL: [Self; 5] = [
            Self::Exact,
            Self::CannedAclMismatch,
            Self::CannedAclWithGrantHeader,
            Self::GrantReadMismatch,
            Self::GrantInputWithCannedAcl,
        ];
    }

    impl fmt::Display for AclContextShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Exact => f.write_str("exact"),
                Self::CannedAclMismatch => f.write_str("canned-acl-mismatch"),
                Self::CannedAclWithGrantHeader => f.write_str("canned-acl-with-grant-header"),
                Self::GrantReadMismatch => f.write_str("grant-read-mismatch"),
                Self::GrantInputWithCannedAcl => f.write_str("grant-input-with-canned-acl"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum AclOutcome {
        Allow,
        Deny,
        AclNotSupported,
        InvalidArgument,
    }

    impl fmt::Display for AclOutcome {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Allow => f.write_str("Allow"),
                Self::Deny => f.write_str("Deny"),
                Self::AclNotSupported => f.write_str("AclNotSupported"),
                Self::InvalidArgument => f.write_str("InvalidArgument"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct AclUpdateScenario {
        pub(super) action: AclUpdateAction,
        pub(super) requester: AclRequesterShape,
        pub(super) bucket: AclBucketShape,
        pub(super) owner: AclOwnerKind,
        pub(super) existing_acl: ExistingAclShape,
        pub(super) requested_acl: RequestedAclShape,
        pub(super) policy: AclPolicyShape,
        pub(super) context: AclContextShape,
    }

    impl AclUpdateScenario {
        pub(super) fn scenarios(action: AclUpdateAction) -> Vec<Self> {
            let mut scenarios = Vec::new();
            for requester in AclRequesterShape::ALL {
                for bucket in AclBucketShape::all() {
                    for owner in AclOwnerKind::ALL {
                        for existing_acl in ExistingAclShape::ALL {
                            for requested_acl in RequestedAclShape::ACL_UPDATE_ALL {
                                for policy in AclPolicyShape::ALL {
                                    for context in AclContextShape::ALL {
                                        let scenario = Self {
                                            action,
                                            requester,
                                            bucket,
                                            owner,
                                            existing_acl,
                                            requested_acl,
                                            policy,
                                            context,
                                        };
                                        if scenario.is_possible() {
                                            scenarios.push(scenario);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            scenarios
        }

        pub(super) fn expected_outcome(self) -> AclOutcome {
            if self.requester.is_anonymous() {
                return AclOutcome::Deny;
            }
            if self.context != AclContextShape::Exact {
                return AclOutcome::InvalidArgument;
            }
            if !self.authorization_allowed() {
                return AclOutcome::Deny;
            }
            if self.bucket.ownership == OwnershipShape::BucketOwnerEnforced {
                return AclOutcome::AclNotSupported;
            }
            if self.bucket.block_public_acls && self.requested_acl.is_public_acl() {
                return AclOutcome::Deny;
            }
            AclOutcome::Allow
        }

        fn is_possible(self) -> bool {
            if self.requester.is_anonymous() && self.context != AclContextShape::Exact {
                return false;
            }
            if self.bucket.ownership == OwnershipShape::BucketOwnerEnforced
                && self.owner != AclOwnerKind::BucketOwner
            {
                return false;
            }
            if self.bucket.ownership == OwnershipShape::BucketOwnerEnforced
                && self.existing_acl != ExistingAclShape::Private
            {
                return false;
            }
            if self.existing_acl == ExistingAclShape::GrantWriteAcpToRequester
                && (self.requester.is_anonymous() || self.requester_is_exact_owner())
            {
                return false;
            }
            if self.requested_acl == RequestedAclShape::GrantsRequesterRead
                && self.requester.is_anonymous()
            {
                return false;
            }
            if self.bucket.block_public_acls && !self.requested_acl.is_public_acl() {
                return false;
            }
            if self.bucket.restrict_public_buckets && self.policy != AclPolicyShape::AllowPublic {
                return false;
            }
            match self.context {
                AclContextShape::Exact => {}
                AclContextShape::CannedAclMismatch | AclContextShape::CannedAclWithGrantHeader => {
                    if !self.requested_acl.is_canned() {
                        return false;
                    }
                }
                AclContextShape::GrantReadMismatch => {
                    if self.requested_acl != RequestedAclShape::GrantsRequesterRead {
                        return false;
                    }
                }
                AclContextShape::GrantInputWithCannedAcl => {
                    if self.requested_acl.is_canned() {
                        return false;
                    }
                }
            }
            match self.policy {
                AclPolicyShape::AllowPrivate => !self.requester.is_anonymous(),
                AclPolicyShape::AllowPublic => true,
                AclPolicyShape::AllowWithPublicCannedDeny => {
                    !self.requester.is_anonymous() && self.requested_acl.is_canned()
                }
                AclPolicyShape::AllowWithGrantReadCondition => {
                    !self.requester.is_anonymous()
                        && self.requested_acl == RequestedAclShape::GrantsRequesterRead
                }
                AclPolicyShape::NoPolicy | AclPolicyShape::NoMatch => true,
            }
        }

        fn authorization_allowed(self) -> bool {
            match self.policy {
                AclPolicyShape::NoPolicy | AclPolicyShape::NoMatch => self.fallback_allowed(),
                AclPolicyShape::AllowPrivate => true,
                AclPolicyShape::AllowPublic => {
                    if !self.bucket.restrict_public_buckets
                        || self.requester.is_bucket_owner_account()
                    {
                        true
                    } else {
                        self.fallback_allowed()
                    }
                }
                AclPolicyShape::AllowWithPublicCannedDeny => {
                    !self.requested_acl.is_public_canned_acl()
                }
                AclPolicyShape::AllowWithGrantReadCondition => true,
            }
        }

        fn fallback_allowed(self) -> bool {
            if self.bucket.ownership == OwnershipShape::BucketOwnerEnforced {
                return self.requester.is_bucket_owner_account_admin();
            }

            self.requester_is_exact_owner()
                || self.requester_matches_owner_via_admin_canonical()
                || self.requester_has_granted_write_acp()
        }

        fn requester_is_exact_owner(self) -> bool {
            matches!(
                (self.requester, self.owner),
                (
                    AclRequesterShape::BucketOwnerPrincipal,
                    AclOwnerKind::BucketOwner
                ) | (
                    AclRequesterShape::SameAccountOtherPrincipal,
                    AclOwnerKind::SameAccountOther,
                ) | (
                    AclRequesterShape::SameAccountOtherAdmin,
                    AclOwnerKind::SameAccountOther,
                )
            )
        }

        fn requester_matches_owner_via_admin_canonical(self) -> bool {
            self.requester.has_owner_account_admin_profile()
                && !self.requester_is_exact_owner()
                && self.requester_canonical_matches_owner()
        }

        fn requester_has_granted_write_acp(self) -> bool {
            self.existing_acl == ExistingAclShape::GrantWriteAcpToRequester
                && !self.requester.is_anonymous()
                && (!self.requester_canonical_matches_owner()
                    || self.requester.has_owner_account_admin_profile())
        }

        fn requester_canonical_matches_owner(self) -> bool {
            matches!(
                self.requester,
                AclRequesterShape::BucketOwnerPrincipal
                    | AclRequesterShape::SameAccountOtherPrincipal
                    | AclRequesterShape::SameAccountOtherAdmin
            )
        }
    }

    impl fmt::Display for AclUpdateScenario {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "action={} requester={} bucket={} owner={} existing_acl={} requested_acl={} policy={} context={}",
                self.action,
                self.requester,
                self.bucket,
                self.owner,
                self.existing_acl,
                self.requested_acl,
                self.policy,
                self.context
            )
        }
    }
}

mod phase4_harness {
    use super::harness::{setup_coordinator, IdentityFixtures};
    use super::model::{OwnershipShape, PolicyDecisionShape};
    use super::phase4_model::{
        AclBucketShape, AclContextShape, AclOutcome, AclOwnerKind, AclPolicyShape,
        AclRequesterShape, AclUpdateAction, AclUpdateScenario, ExistingAclShape, RequestedAclShape,
        WriteAction, WriteBucketShape, WriteOutcome, WriteRequesterShape, WriteScenario,
    };
    use super::*;

    const PHASE4_KEY: &str = "key";

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum ClassifiedWriteResult {
        Allow,
        Deny,
        AclNotSupported,
        InvalidArgument,
    }

    pub(super) struct Phase4Harness {
        _tmp: test_util::TempDir,
        coord: Coordinator,
        fixtures: IdentityFixtures,
    }

    impl Phase4Harness {
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

        pub(super) fn run_write(
            &self,
            bucket: &str,
            scenario: WriteScenario,
        ) -> ClassifiedWriteResult {
            materialize_write_bucket(&self.coord, &self.fixtures, bucket, scenario.bucket)
                .unwrap_or_else(|err| {
                    panic!("failed to materialize phase 4 write bucket for {scenario}: {err:?}");
                });
            materialize_write_policy(&self.coord, &self.fixtures, bucket, scenario).unwrap_or_else(
                |err| {
                    panic!("failed to materialize phase 4 write policy for {scenario}: {err:?}");
                },
            );
            classify(run_write_action(
                &self.coord,
                &self.fixtures,
                bucket,
                scenario,
            ))
        }

        pub(super) fn run_acl_update(
            &self,
            bucket: &str,
            scenario: AclUpdateScenario,
        ) -> ClassifiedWriteResult {
            materialize_acl_bucket(&self.coord, &self.fixtures, bucket, scenario.bucket)
                .unwrap_or_else(|err| {
                    panic!("failed to materialize phase 4 acl bucket for {scenario}: {err:?}");
                });
            let version_id = materialize_acl_target(&self.coord, &self.fixtures, bucket, scenario)
                .unwrap_or_else(|err| {
                    panic!("failed to materialize phase 4 acl target for {scenario}: {err:?}");
                });
            materialize_acl_policy(&self.coord, &self.fixtures, bucket, scenario).unwrap_or_else(
                |err| {
                    panic!("failed to materialize phase 4 acl policy for {scenario}: {err:?}");
                },
            );
            classify(run_acl_update_action(
                &self.coord,
                &self.fixtures,
                bucket,
                scenario,
                version_id,
            ))
        }
    }

    pub(super) fn write_bucket_name_for(action: WriteAction, index: usize) -> String {
        let action_tag = match action {
            WriteAction::PutObject => "putobj",
            WriteAction::CreateMultipartUpload => "mpu",
            WriteAction::BeginStreamPut => "stream",
        };
        format!("authz-phase4-{action_tag}-{index:05}")
    }

    pub(super) fn acl_bucket_name_for(action: AclUpdateAction, index: usize) -> String {
        let action_tag = match action {
            AclUpdateAction::PutObjectAcl => "putacl",
            AclUpdateAction::PutObjectVersionAcl => "putvacl",
        };
        format!("authz-phase4-{action_tag}-{index:05}")
    }

    pub(super) fn to_write_outcome(result: ClassifiedWriteResult) -> WriteOutcome {
        match result {
            ClassifiedWriteResult::Allow => WriteOutcome::Allow,
            ClassifiedWriteResult::Deny => WriteOutcome::Deny,
            ClassifiedWriteResult::AclNotSupported => WriteOutcome::AclNotSupported,
            ClassifiedWriteResult::InvalidArgument => {
                panic!("write-entry matrix produced an unexpected InvalidArgument")
            }
        }
    }

    pub(super) fn to_acl_outcome(result: ClassifiedWriteResult) -> AclOutcome {
        match result {
            ClassifiedWriteResult::Allow => AclOutcome::Allow,
            ClassifiedWriteResult::Deny => AclOutcome::Deny,
            ClassifiedWriteResult::AclNotSupported => AclOutcome::AclNotSupported,
            ClassifiedWriteResult::InvalidArgument => AclOutcome::InvalidArgument,
        }
    }

    fn materialize_write_bucket(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        shape: WriteBucketShape,
    ) -> Result<(), ServerError> {
        let owner = OwnerIdentity::new(
            fixtures.owner_user.principal(),
            fixtures.owner_user.canonical_user_id().clone(),
        );
        let grants =
            Coordinator::bucket_acl_grants_from_flags(&owner, false, shape.bucket_public_write);
        coord.create_bucket_with_acl_grants(&owner, bucket, grants, false)?;

        if shape.ownership == OwnershipShape::BucketOwnerEnforced {
            coord.put_bucket_ownership_controls(&PutBucketOwnershipControlsRequest {
                bucket: BucketRequest::new(
                    trusted_bucket_name(bucket),
                    Requester::authenticated(fixtures.owner_user.clone()),
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
                    trusted_bucket_name(bucket),
                    Requester::authenticated(fixtures.owner_user.clone()),
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

    fn materialize_write_policy(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: WriteScenario,
    ) -> Result<(), ServerError> {
        let Some(policy) = write_policy_document(fixtures, bucket, scenario) else {
            return Ok(());
        };
        coord.put_bucket_policy(&PutBucketPolicyRequest {
            bucket: BucketRequest::new(
                trusted_bucket_name(bucket),
                Requester::authenticated(fixtures.owner_user.clone()),
                None,
            ),
            config: &policy,
            confirm_remove_self_bucket_access: false,
        })
    }

    fn write_policy_document(
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: WriteScenario,
    ) -> Option<String> {
        let mut statements = Vec::new();
        let requester_principal = write_requester_principal(fixtures, scenario.requester);
        let object_resource = format!("arn:aws:s3:::{bucket}/{PHASE4_KEY}");
        push_policy_statement(
            &mut statements,
            requester_principal,
            "s3:PutObject",
            &object_resource,
            scenario.policy,
        );

        if statements.is_empty() {
            None
        } else {
            Some(format!(
                r#"{{"Version":"2012-10-17","Statement":[{}]}}"#,
                statements.join(",")
            ))
        }
    }

    fn run_write_action(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: WriteScenario,
    ) -> Result<(), ServerError> {
        let requester = write_requester(fixtures, scenario.requester);
        match scenario.action {
            WriteAction::PutObject => test_helpers::put_object(
                coord,
                &PutObjectRequest {
                    encryption: WriteEncryptionRequest::none(),
                    policy_context: PutObjectPolicyContext::default(),
                    object_lock: ObjectLockState::default(),
                    object: ObjectRequest::new(
                        trusted_bucket_name(bucket),
                        trusted_object_key(PHASE4_KEY),
                        requester,
                        None,
                    ),
                    data: b"phase-4-write",
                    metadata: &MetadataBlob::new(),
                    system_metadata: &SystemMetadata::EMPTY,
                    tags: None,
                    cond: NO_WRITE,
                    acl: put_object_write_acl(fixtures, scenario.acl),
                },
            )
            .map(|_| ()),
            WriteAction::CreateMultipartUpload => {
                let upload = coord.create_multipart_upload(&CreateMultipartUploadRequest {
                    object: ObjectRequest::new(
                        trusted_bucket_name(bucket),
                        trusted_object_key(PHASE4_KEY),
                        requester.clone(),
                        None,
                    ),
                    metadata: &MetadataBlob::new(),
                    system_metadata: &SystemMetadata::EMPTY,
                    tags: None,
                    checksum: None,
                    acl: put_object_write_acl(fixtures, scenario.acl),
                    policy_context: PutObjectPolicyContext::default(),
                    object_lock: ObjectLockState::default(),
                    encryption: WriteEncryptionRequest::none(),
                })?;
                coord.abort_multipart_upload(&MultipartObjectRequest::new(
                    trusted_bucket_name(bucket),
                    trusted_object_key(PHASE4_KEY),
                    UploadId::try_from(upload.upload_id.as_str())
                        .expect("test-created upload IDs should be valid"),
                    requester,
                    None,
                ))
            }
            WriteAction::BeginStreamPut => {
                let prepared = coord.begin_stream_put(&AuthorizePutObjectRequest {
                    object: ObjectRequest::new(
                        trusted_bucket_name(bucket),
                        trusted_object_key(PHASE4_KEY),
                        requester,
                        None,
                    ),
                    acl: put_object_write_acl(fixtures, scenario.acl),
                    policy_context: WriteEncryptionRequest::none().with_policy_context(
                        PutObjectPolicyContext::default()
                            .with_default_canned_acl(scenario.acl.policy_condition_value()),
                    ),
                    object_lock: ObjectLockState::default(),
                    tags: None,
                    encryption: WriteEncryptionRequest::none(),
                })?;
                coord.abort_stream_put(bucket, PHASE4_KEY, &prepared.session_id)
            }
        }
    }

    fn materialize_acl_bucket(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        shape: AclBucketShape,
    ) -> Result<(), ServerError> {
        let owner = OwnerIdentity::new(
            fixtures.owner_user.principal(),
            fixtures.owner_user.canonical_user_id().clone(),
        );
        let grants = Coordinator::bucket_acl_grants_from_flags(&owner, false, false);
        coord.create_bucket_with_acl_grants(&owner, bucket, grants, false)?;
        coord.put_bucket_versioning(&PutBucketVersioningRequest {
            bucket: BucketRequest::new(
                trusted_bucket_name(bucket),
                Requester::authenticated(fixtures.owner_user.clone()),
                None,
            ),
            state: BucketVersioningState::Enabled,
        })?;

        if shape.ownership == OwnershipShape::BucketOwnerEnforced {
            coord.put_bucket_ownership_controls(&PutBucketOwnershipControlsRequest {
                bucket: BucketRequest::new(
                    trusted_bucket_name(bucket),
                    Requester::authenticated(fixtures.owner_user.clone()),
                    None,
                ),
                config: BucketOwnershipControls {
                    object_ownership: BucketObjectOwnership::BucketOwnerEnforced,
                },
            })?;
        }

        if shape.block_public_acls || shape.restrict_public_buckets {
            coord.put_bucket_public_access_block(&PutBucketPublicAccessBlockRequest {
                bucket: BucketRequest::new(
                    trusted_bucket_name(bucket),
                    Requester::authenticated(fixtures.owner_user.clone()),
                    None,
                ),
                config: PublicAccessBlockConfig {
                    block_public_acls: shape.block_public_acls,
                    ignore_public_acls: false,
                    block_public_policy: false,
                    restrict_public_buckets: shape.restrict_public_buckets,
                },
            })?;
        }

        Ok(())
    }

    fn materialize_acl_target(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: AclUpdateScenario,
    ) -> Result<VersionId, ServerError> {
        if scenario.owner == AclOwnerKind::SameAccountOther {
            let allow_policy = format!(
                r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"{}"}},"Action":"s3:PutObject","Resource":"arn:aws:s3:::{bucket}/{PHASE4_KEY}"}}]}}"#,
                fixtures.root.principal()
            );
            coord.put_bucket_policy(&PutBucketPolicyRequest {
                bucket: BucketRequest::new(
                    trusted_bucket_name(bucket),
                    Requester::authenticated(fixtures.owner_user.clone()),
                    None,
                ),
                config: &allow_policy,
                confirm_remove_self_bucket_access: false,
            })?;
        }

        let writer = acl_owner_requester(fixtures, scenario.owner);
        let put = test_helpers::put_object(
            coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: ObjectRequest::new(
                    trusted_bucket_name(bucket),
                    trusted_object_key(PHASE4_KEY),
                    writer,
                    None,
                ),
                data: b"phase-4-acl",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,
                acl: existing_object_write_acl(fixtures, scenario),
            },
        )?;
        Ok(put.version_id)
    }

    fn materialize_acl_policy(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: AclUpdateScenario,
    ) -> Result<(), ServerError> {
        let Some(policy) = acl_policy_document(fixtures, bucket, scenario) else {
            if scenario.owner == AclOwnerKind::SameAccountOther {
                coord.delete_bucket_policy(&BucketRequest::new(
                    trusted_bucket_name(bucket),
                    Requester::authenticated(fixtures.owner_user.clone()),
                    None,
                ))?;
            }
            return Ok(());
        };
        coord.put_bucket_policy(&PutBucketPolicyRequest {
            bucket: BucketRequest::new(
                trusted_bucket_name(bucket),
                Requester::authenticated(fixtures.owner_user.clone()),
                None,
            ),
            config: &policy,
            confirm_remove_self_bucket_access: false,
        })
    }

    fn acl_policy_document(
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: AclUpdateScenario,
    ) -> Option<String> {
        let mut statements = Vec::new();
        let requester_principal = acl_requester_principal(fixtures, scenario.requester);
        let action = acl_policy_action_name(scenario.action);
        let object_resource = format!("arn:aws:s3:::{bucket}/{PHASE4_KEY}");
        match scenario.policy {
            AclPolicyShape::NoPolicy => {}
            AclPolicyShape::NoMatch => statements.push(format!(
                r#"{{"Effect":"Allow","Principal":{{"AWS":"arn:aws:iam::999988887777:user/unmatched"}},"Action":"{action}","Resource":"{object_resource}"}}"#
            )),
            AclPolicyShape::AllowPrivate => {
                let principal = requester_principal
                    .expect("private acl-update allow requires an authenticated requester");
                statements.push(format!(
                    r#"{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":"{action}","Resource":"{object_resource}"}}"#
                ));
            }
            AclPolicyShape::AllowPublic => statements.push(format!(
                r#"{{"Effect":"Allow","Principal":"*","Action":"{action}","Resource":"{object_resource}"}}"#
            )),
            AclPolicyShape::AllowWithPublicCannedDeny => {
                let principal = requester_principal.expect(
                    "conditional canned-acl deny policy requires an authenticated requester",
                );
                statements.push(format!(
                    r#"{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":"{action}","Resource":"{object_resource}"}}"#
                ));
                statements.push(format!(
                    r#"{{"Effect":"Deny","Principal":{{"AWS":"{principal}"}},"Action":"{action}","Resource":"{object_resource}","Condition":{{"StringLike":{{"s3:x-amz-acl":"public*"}}}}}}"#
                ));
            }
            AclPolicyShape::AllowWithGrantReadCondition => {
                let principal = requester_principal.expect(
                    "grant-read condition policy requires an authenticated requester",
                );
                let grant_read_header = acl_requested_grant_read_header(fixtures, scenario)
                    .expect("grant-read condition policy requires a requester-read ACL shape");
                let grant_read_json = json_string(grant_read_header);
                statements.push(format!(
                    r#"{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":"{action}","Resource":"{object_resource}","Condition":{{"StringEquals":{{"s3:x-amz-grant-read":{grant_read_json}}}}}}}"#
                ));
            }
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

    fn run_acl_update_action(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: AclUpdateScenario,
        version_id: VersionId,
    ) -> Result<(), ServerError> {
        let requester = acl_requester(fixtures, scenario.requester);
        let request_version_id = match scenario.action {
            AclUpdateAction::PutObjectAcl => None,
            AclUpdateAction::PutObjectVersionAcl => Some(version_id),
        };
        coord
            .put_object_acl(&PutObjectAclRequest {
                object: ObjectVersionRequest::new(
                    trusted_bucket_name(bucket),
                    trusted_object_key(PHASE4_KEY),
                    request_version_id,
                    requester,
                    None,
                ),
                acl: put_object_acl_input(fixtures, scenario),
                policy_context: acl_policy_context(fixtures, scenario),
            })
            .map(|_| ())
    }

    fn classify(result: Result<(), ServerError>) -> ClassifiedWriteResult {
        match result {
            Ok(()) => ClassifiedWriteResult::Allow,
            Err(ServerError::AccessDenied | ServerError::AnonymousApiAccessDenied) => {
                ClassifiedWriteResult::Deny
            }
            Err(ServerError::AccessControlListNotSupported) => {
                ClassifiedWriteResult::AclNotSupported
            }
            Err(ServerError::InvalidArgument { .. }) => ClassifiedWriteResult::InvalidArgument,
            Err(other) => panic!("unexpected phase 4 classified result: {other:?}"),
        }
    }

    fn write_requester(fixtures: &IdentityFixtures, shape: WriteRequesterShape) -> Requester {
        match shape {
            WriteRequesterShape::Anonymous => Requester::anonymous(),
            WriteRequesterShape::BucketOwnerPrincipal => {
                Requester::authenticated(fixtures.owner_user.clone())
            }
            WriteRequesterShape::SameAccountOtherPrincipal => {
                Requester::authenticated(fixtures.root.clone())
            }
            WriteRequesterShape::SameAccountDistinctAdmin => {
                Requester::authenticated_owner_account_admin(fixtures.same_account_distinct.clone())
            }
            WriteRequesterShape::CrossAccountPrincipal => {
                Requester::authenticated(fixtures.cross_account.clone())
            }
        }
    }

    fn write_requester_principal(
        fixtures: &IdentityFixtures,
        shape: WriteRequesterShape,
    ) -> Option<&str> {
        match shape {
            WriteRequesterShape::Anonymous => None,
            WriteRequesterShape::BucketOwnerPrincipal => Some(fixtures.owner_user.principal()),
            WriteRequesterShape::SameAccountOtherPrincipal => Some(fixtures.root.principal()),
            WriteRequesterShape::SameAccountDistinctAdmin => {
                Some(fixtures.same_account_distinct.principal())
            }
            WriteRequesterShape::CrossAccountPrincipal => Some(fixtures.cross_account.principal()),
        }
    }

    fn put_object_write_acl(
        fixtures: &IdentityFixtures,
        acl: RequestedAclShape,
    ) -> PutObjectWriteAcl<'static> {
        match acl {
            RequestedAclShape::None => PutObjectWriteAcl::None,
            RequestedAclShape::CannedPrivate => PutObjectAcl::Private.into(),
            RequestedAclShape::CannedPublicRead => PutObjectAcl::PublicRead.into(),
            RequestedAclShape::CannedAuthenticatedRead => PutObjectAcl::AuthenticatedRead.into(),
            RequestedAclShape::CannedBucketOwnerFullControl => {
                PutObjectAcl::BucketOwnerFullControl.into()
            }
            RequestedAclShape::GrantsOwnerFullControlOnly => {
                PutObjectWriteAcl::Grants(AclGrants::new(vec![AclGrant::new(
                    AclGrantee::CanonicalUser(fixtures.owner_user.canonical_user_id().clone()),
                    AclPermission::FullControl,
                )]))
            }
            RequestedAclShape::GrantsRequesterRead => {
                PutObjectWriteAcl::Grants(AclGrants::new(vec![
                    AclGrant::new(
                        AclGrantee::CanonicalUser(fixtures.owner_user.canonical_user_id().clone()),
                        AclPermission::FullControl,
                    ),
                    AclGrant::new(
                        AclGrantee::CanonicalUser(
                            fixtures.cross_account.canonical_user_id().clone(),
                        ),
                        AclPermission::Read,
                    ),
                ]))
            }
            RequestedAclShape::GrantsAuthenticatedUsersRead => {
                PutObjectWriteAcl::Grants(AclGrants::new(vec![
                    AclGrant::new(
                        AclGrantee::CanonicalUser(fixtures.owner_user.canonical_user_id().clone()),
                        AclPermission::FullControl,
                    ),
                    AclGrant::new(AclGrantee::AuthenticatedUsers, AclPermission::Read),
                ]))
            }
        }
    }

    fn acl_requester(fixtures: &IdentityFixtures, shape: AclRequesterShape) -> Requester {
        match shape {
            AclRequesterShape::Anonymous => Requester::anonymous(),
            AclRequesterShape::BucketOwnerPrincipal => {
                Requester::authenticated(fixtures.owner_user.clone())
            }
            AclRequesterShape::SameAccountOtherPrincipal => {
                Requester::authenticated(fixtures.root.clone())
            }
            AclRequesterShape::SameAccountOtherAdmin => {
                Requester::authenticated_owner_account_admin(fixtures.root.clone())
            }
            AclRequesterShape::SameAccountDistinctAdmin => {
                Requester::authenticated_owner_account_admin(fixtures.same_account_distinct.clone())
            }
            AclRequesterShape::CrossAccountPrincipal => {
                Requester::authenticated(fixtures.cross_account.clone())
            }
        }
    }

    fn acl_requester_principal(
        fixtures: &IdentityFixtures,
        shape: AclRequesterShape,
    ) -> Option<&str> {
        match shape {
            AclRequesterShape::Anonymous => None,
            AclRequesterShape::BucketOwnerPrincipal => Some(fixtures.owner_user.principal()),
            AclRequesterShape::SameAccountOtherPrincipal => Some(fixtures.root.principal()),
            AclRequesterShape::SameAccountOtherAdmin => Some(fixtures.root.principal()),
            AclRequesterShape::SameAccountDistinctAdmin => {
                Some(fixtures.same_account_distinct.principal())
            }
            AclRequesterShape::CrossAccountPrincipal => Some(fixtures.cross_account.principal()),
        }
    }

    fn acl_requester_account(
        fixtures: &IdentityFixtures,
        shape: AclRequesterShape,
    ) -> Option<&AccountIdentity> {
        match shape {
            AclRequesterShape::Anonymous => None,
            AclRequesterShape::BucketOwnerPrincipal => Some(&fixtures.owner_user),
            AclRequesterShape::SameAccountOtherPrincipal => Some(&fixtures.root),
            AclRequesterShape::SameAccountOtherAdmin => Some(&fixtures.root),
            AclRequesterShape::SameAccountDistinctAdmin => Some(&fixtures.same_account_distinct),
            AclRequesterShape::CrossAccountPrincipal => Some(&fixtures.cross_account),
        }
    }

    fn acl_owner_requester(fixtures: &IdentityFixtures, owner: AclOwnerKind) -> Requester {
        match owner {
            AclOwnerKind::BucketOwner => Requester::authenticated(fixtures.owner_user.clone()),
            AclOwnerKind::SameAccountOther => Requester::authenticated(fixtures.root.clone()),
        }
    }

    fn existing_object_write_acl(
        fixtures: &IdentityFixtures,
        scenario: AclUpdateScenario,
    ) -> PutObjectWriteAcl<'static> {
        match scenario.existing_acl {
            ExistingAclShape::Private => PutObjectWriteAcl::None,
            ExistingAclShape::GrantWriteAcpToRequester => {
                let owner = match scenario.owner {
                    AclOwnerKind::BucketOwner => &fixtures.owner_user,
                    AclOwnerKind::SameAccountOther => &fixtures.root,
                };
                let requester = acl_requester_account(fixtures, scenario.requester)
                    .expect("WriteAcp grants require an authenticated requester");
                PutObjectWriteAcl::Grants(AclGrants::new(vec![
                    AclGrant::new(
                        AclGrantee::CanonicalUser(owner.canonical_user_id().clone()),
                        AclPermission::FullControl,
                    ),
                    AclGrant::new(
                        AclGrantee::CanonicalUser(requester.canonical_user_id().clone()),
                        AclPermission::WriteAcp,
                    ),
                ]))
            }
        }
    }

    fn put_object_acl_input(
        fixtures: &IdentityFixtures,
        scenario: AclUpdateScenario,
    ) -> PutObjectAclInput<'static> {
        match scenario.requested_acl {
            RequestedAclShape::CannedPrivate => PutObjectAclInput::Canned(PutObjectAcl::Private),
            RequestedAclShape::CannedPublicRead => {
                PutObjectAclInput::Canned(PutObjectAcl::PublicRead)
            }
            RequestedAclShape::GrantsRequesterRead => {
                let requester = acl_requester_account(fixtures, scenario.requester)
                    .expect("requester-read grants require an authenticated requester");
                PutObjectAclInput::Grants(AclGrants::new(vec![AclGrant::new(
                    AclGrantee::CanonicalUser(requester.canonical_user_id().clone()),
                    AclPermission::Read,
                )]))
            }
            RequestedAclShape::GrantsAuthenticatedUsersRead => {
                PutObjectAclInput::Grants(AclGrants::new(vec![AclGrant::new(
                    AclGrantee::AuthenticatedUsers,
                    AclPermission::Read,
                )]))
            }
            RequestedAclShape::None
            | RequestedAclShape::CannedAuthenticatedRead
            | RequestedAclShape::CannedBucketOwnerFullControl
            | RequestedAclShape::GrantsOwnerFullControlOnly => {
                panic!(
                    "unsupported phase 4 PutObjectAcl request shape: {}",
                    scenario.requested_acl
                )
            }
        }
    }

    fn acl_policy_context(
        fixtures: &IdentityFixtures,
        scenario: AclUpdateScenario,
    ) -> PutObjectPolicyContext<'static> {
        match scenario.context {
            AclContextShape::Exact => match scenario.requested_acl {
                RequestedAclShape::CannedPrivate | RequestedAclShape::CannedPublicRead => {
                    PutObjectPolicyContext::default()
                        .with_default_canned_acl(scenario.requested_acl.policy_condition_value())
                }
                RequestedAclShape::GrantsRequesterRead => PutObjectPolicyContext::default()
                    .with_acl_grant_headers(
                        acl_requested_grant_read_header(fixtures, scenario),
                        None,
                        None,
                        None,
                        None,
                    ),
                RequestedAclShape::GrantsAuthenticatedUsersRead => {
                    PutObjectPolicyContext::default()
                }
                RequestedAclShape::None
                | RequestedAclShape::CannedAuthenticatedRead
                | RequestedAclShape::CannedBucketOwnerFullControl
                | RequestedAclShape::GrantsOwnerFullControlOnly => {
                    panic!(
                        "unsupported phase 4 PutObjectAcl context shape: {}",
                        scenario.requested_acl
                    )
                }
            },
            AclContextShape::CannedAclMismatch => PutObjectPolicyContext::default()
                .with_default_canned_acl(Some(match scenario.requested_acl {
                    RequestedAclShape::CannedPrivate => "public-read",
                    RequestedAclShape::CannedPublicRead => "private",
                    other => {
                        panic!("canned ACL mismatch requires canned request shape, got {other}")
                    }
                })),
            AclContextShape::CannedAclWithGrantHeader => PutObjectPolicyContext::default()
                .with_default_canned_acl(scenario.requested_acl.policy_condition_value())
                .with_acl_grant_headers(Some(r#"id="different-grantee""#), None, None, None, None),
            AclContextShape::GrantReadMismatch => PutObjectPolicyContext::default()
                .with_acl_grant_headers(Some(r#"id="different-grantee""#), None, None, None, None),
            AclContextShape::GrantInputWithCannedAcl => {
                PutObjectPolicyContext::default().with_default_canned_acl(Some("public-read"))
            }
        }
    }

    fn acl_requested_grant_read_header(
        fixtures: &IdentityFixtures,
        scenario: AclUpdateScenario,
    ) -> Option<&'static str> {
        let requester = acl_requester_account(fixtures, scenario.requester)?;
        let header = format!(r#"id="{}""#, requester.canonical_user_id());
        Some(Box::leak(header.into_boxed_str()))
    }

    fn acl_policy_action_name(action: AclUpdateAction) -> &'static str {
        match action {
            AclUpdateAction::PutObjectAcl => "s3:PutObjectAcl",
            AclUpdateAction::PutObjectVersionAcl => "s3:PutObjectVersionAcl",
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

    fn json_string(value: &str) -> String {
        format!(r#""{}""#, value.replace('\\', "\\\\").replace('"', "\\\""))
    }
}

mod phase5_model {
    use super::model::{Outcome, OwnershipShape};
    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum TransitionSeed {
        PrivateObject,
        PublicReadAcl,
        ExplicitGrantReadToCrossAccount,
    }

    impl fmt::Display for TransitionSeed {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::PrivateObject => f.write_str("private-object"),
                Self::PublicReadAcl => f.write_str("public-read-acl"),
                Self::ExplicitGrantReadToCrossAccount => {
                    f.write_str("explicit-grant-read-cross-account")
                }
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum TransitionPolicyState {
        None,
        AllowCrossAccountRead,
        DenyCrossAccountRead,
    }

    impl fmt::Display for TransitionPolicyState {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::None => f.write_str("no-policy"),
                Self::AllowCrossAccountRead => f.write_str("allow-cross-account-read"),
                Self::DenyCrossAccountRead => f.write_str("deny-cross-account-read"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum TransitionProbe {
        AnonymousGetObject,
        CrossAccountGetObject,
    }

    impl fmt::Display for TransitionProbe {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::AnonymousGetObject => f.write_str("anonymous-get-object"),
                Self::CrossAccountGetObject => f.write_str("cross-account-get-object"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum TransitionMutation {
        EnableBucketOwnerEnforced,
        DeleteOwnershipControls,
        EnableIgnorePublicAcls,
        DeletePublicAccessBlock,
        SetPolicy(TransitionPolicyState),
    }

    impl fmt::Display for TransitionMutation {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::EnableBucketOwnerEnforced => f.write_str("enable-boe"),
                Self::DeleteOwnershipControls => f.write_str("delete-ownership-controls"),
                Self::EnableIgnorePublicAcls => f.write_str("enable-ignore-public-acls"),
                Self::DeletePublicAccessBlock => f.write_str("delete-public-access-block"),
                Self::SetPolicy(policy) => write!(f, "set-policy={policy}"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum TransitionStep {
        Probe(TransitionProbe),
        Mutate(TransitionMutation),
    }

    impl fmt::Display for TransitionStep {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Probe(probe) => write!(f, "probe {probe}"),
                Self::Mutate(mutation) => write!(f, "mutate {mutation}"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct TransitionState {
        ownership: OwnershipShape,
        ignore_public_acls: bool,
        policy: TransitionPolicyState,
    }

    impl TransitionState {
        pub(super) fn new(policy: TransitionPolicyState) -> Self {
            Self {
                ownership: OwnershipShape::ObjectWriter,
                ignore_public_acls: false,
                policy,
            }
        }

        pub(super) fn apply(&mut self, mutation: TransitionMutation) {
            match mutation {
                TransitionMutation::EnableBucketOwnerEnforced => {
                    self.ownership = OwnershipShape::BucketOwnerEnforced;
                }
                TransitionMutation::DeleteOwnershipControls => {
                    self.ownership = OwnershipShape::ObjectWriter;
                }
                TransitionMutation::EnableIgnorePublicAcls => {
                    self.ignore_public_acls = true;
                }
                TransitionMutation::DeletePublicAccessBlock => {
                    self.ignore_public_acls = false;
                }
                TransitionMutation::SetPolicy(policy) => {
                    self.policy = policy;
                }
            }
        }

        pub(super) fn expected_outcome(
            self,
            seed: TransitionSeed,
            probe: TransitionProbe,
        ) -> Outcome {
            if probe == TransitionProbe::CrossAccountGetObject {
                match self.policy {
                    TransitionPolicyState::AllowCrossAccountRead => return Outcome::Allow,
                    TransitionPolicyState::DenyCrossAccountRead => return Outcome::Deny,
                    TransitionPolicyState::None => {}
                }
            }

            let legacy_acl_allows = match (seed, probe) {
                (TransitionSeed::PublicReadAcl, TransitionProbe::AnonymousGetObject)
                | (TransitionSeed::PublicReadAcl, TransitionProbe::CrossAccountGetObject)
                | (
                    TransitionSeed::ExplicitGrantReadToCrossAccount,
                    TransitionProbe::CrossAccountGetObject,
                ) => true,
                (TransitionSeed::PrivateObject, _) => false,
                (
                    TransitionSeed::ExplicitGrantReadToCrossAccount,
                    TransitionProbe::AnonymousGetObject,
                ) => false,
            };
            if !legacy_acl_allows {
                return Outcome::Deny;
            }
            if self.ownership == OwnershipShape::BucketOwnerEnforced {
                return Outcome::Deny;
            }
            if seed == TransitionSeed::PublicReadAcl && self.ignore_public_acls {
                return Outcome::Deny;
            }
            Outcome::Allow
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct TransitionScenario {
        pub(super) name: &'static str,
        pub(super) seed: TransitionSeed,
        pub(super) initial_policy: TransitionPolicyState,
        pub(super) probe_via_reader: bool,
        pub(super) steps: &'static [TransitionStep],
    }

    impl TransitionScenario {
        pub(super) fn scenarios() -> Vec<Self> {
            const PUBLIC_READ_BOE_TRACE: [TransitionStep; 5] = [
                TransitionStep::Probe(TransitionProbe::AnonymousGetObject),
                TransitionStep::Mutate(TransitionMutation::EnableBucketOwnerEnforced),
                TransitionStep::Probe(TransitionProbe::AnonymousGetObject),
                TransitionStep::Mutate(TransitionMutation::DeleteOwnershipControls),
                TransitionStep::Probe(TransitionProbe::AnonymousGetObject),
            ];
            const EXPLICIT_GRANT_BOE_TRACE: [TransitionStep; 5] = [
                TransitionStep::Probe(TransitionProbe::CrossAccountGetObject),
                TransitionStep::Mutate(TransitionMutation::EnableBucketOwnerEnforced),
                TransitionStep::Probe(TransitionProbe::CrossAccountGetObject),
                TransitionStep::Mutate(TransitionMutation::DeleteOwnershipControls),
                TransitionStep::Probe(TransitionProbe::CrossAccountGetObject),
            ];
            const IGNORE_PUBLIC_ACLS_TRACE: [TransitionStep; 5] = [
                TransitionStep::Probe(TransitionProbe::AnonymousGetObject),
                TransitionStep::Mutate(TransitionMutation::EnableIgnorePublicAcls),
                TransitionStep::Probe(TransitionProbe::AnonymousGetObject),
                TransitionStep::Mutate(TransitionMutation::DeletePublicAccessBlock),
                TransitionStep::Probe(TransitionProbe::AnonymousGetObject),
            ];
            const POLICY_REPLACE_REMOVE_TRACE: [TransitionStep; 5] = [
                TransitionStep::Probe(TransitionProbe::CrossAccountGetObject),
                TransitionStep::Mutate(TransitionMutation::SetPolicy(
                    TransitionPolicyState::DenyCrossAccountRead,
                )),
                TransitionStep::Probe(TransitionProbe::CrossAccountGetObject),
                TransitionStep::Mutate(TransitionMutation::SetPolicy(TransitionPolicyState::None)),
                TransitionStep::Probe(TransitionProbe::CrossAccountGetObject),
            ];
            const POLICY_BOE_TRACE: [TransitionStep; 5] = [
                TransitionStep::Probe(TransitionProbe::CrossAccountGetObject),
                TransitionStep::Mutate(TransitionMutation::EnableBucketOwnerEnforced),
                TransitionStep::Probe(TransitionProbe::CrossAccountGetObject),
                TransitionStep::Mutate(TransitionMutation::DeleteOwnershipControls),
                TransitionStep::Probe(TransitionProbe::CrossAccountGetObject),
            ];

            vec![
                Self {
                    name: "legacy-public-read-acl-boe-transition",
                    seed: TransitionSeed::PublicReadAcl,
                    initial_policy: TransitionPolicyState::None,
                    probe_via_reader: false,
                    steps: &PUBLIC_READ_BOE_TRACE,
                },
                Self {
                    name: "legacy-explicit-grant-read-boe-transition",
                    seed: TransitionSeed::ExplicitGrantReadToCrossAccount,
                    initial_policy: TransitionPolicyState::None,
                    probe_via_reader: false,
                    steps: &EXPLICIT_GRANT_BOE_TRACE,
                },
                Self {
                    name: "ignore-public-acls-toggle-restores-legacy-public-read",
                    seed: TransitionSeed::PublicReadAcl,
                    initial_policy: TransitionPolicyState::None,
                    probe_via_reader: false,
                    steps: &IGNORE_PUBLIC_ACLS_TRACE,
                },
                Self {
                    name: "bucket-policy-replacement-and-removal-transition",
                    seed: TransitionSeed::PrivateObject,
                    initial_policy: TransitionPolicyState::AllowCrossAccountRead,
                    probe_via_reader: false,
                    steps: &POLICY_REPLACE_REMOVE_TRACE,
                },
                Self {
                    name: "bucket-policy-read-survives-boe-transition",
                    seed: TransitionSeed::PrivateObject,
                    initial_policy: TransitionPolicyState::AllowCrossAccountRead,
                    probe_via_reader: false,
                    steps: &POLICY_BOE_TRACE,
                },
            ]
        }

        pub(super) fn initial_state(self) -> TransitionState {
            TransitionState::new(self.initial_policy)
        }
    }

    impl fmt::Display for TransitionScenario {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "name={} seed={}", self.name, self.seed)
        }
    }
}

mod phase5_harness {
    use super::harness::{setup_coordinator, IdentityFixtures};
    use super::model::Outcome;
    use super::phase5_model::{
        TransitionMutation, TransitionPolicyState, TransitionProbe, TransitionScenario,
        TransitionSeed,
    };
    use super::*;

    const PHASE5_KEY: &str = "key";

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum ClassifiedTransitionResult {
        Allow,
        Deny,
    }

    pub(super) struct Phase5Harness {
        _tmp: test_util::TempDir,
        admin: Coordinator,
        reader: Coordinator,
        fixtures: IdentityFixtures,
    }

    impl Phase5Harness {
        pub(super) fn new() -> Self {
            let tmp = test_util::tempdir();
            let admin = setup_coordinator(tmp.path());
            let reader = setup_coordinator(tmp.path());
            let fixtures = IdentityFixtures::new();
            Self {
                _tmp: tmp,
                admin,
                reader,
                fixtures,
            }
        }

        pub(super) fn prepare(&self, bucket: &str, scenario: TransitionScenario) {
            materialize_transition_bucket(&self.admin, &self.fixtures, bucket).unwrap_or_else(
                |err| {
                    panic!("failed to materialize phase 5 bucket for {scenario}: {err:?}");
                },
            );
            materialize_transition_seed(&self.admin, &self.fixtures, bucket, scenario.seed)
                .unwrap_or_else(|err| {
                    panic!("failed to materialize phase 5 seed for {scenario}: {err:?}");
                });
            if scenario.initial_policy != TransitionPolicyState::None {
                apply_transition_mutation(
                    &self.admin,
                    &self.fixtures,
                    bucket,
                    TransitionMutation::SetPolicy(scenario.initial_policy),
                )
                .unwrap_or_else(|err| {
                    panic!("failed to materialize phase 5 initial policy for {scenario}: {err:?}");
                });
            }
        }

        pub(super) fn apply_mutation(
            &self,
            bucket: &str,
            scenario: TransitionScenario,
            mutation: TransitionMutation,
        ) {
            apply_transition_mutation(&self.admin, &self.fixtures, bucket, mutation)
                .unwrap_or_else(|err| {
                    panic!("failed to apply phase 5 mutation {mutation} for {scenario}: {err:?}");
                });
        }

        pub(super) fn probe(
            &self,
            bucket: &str,
            scenario: TransitionScenario,
            probe: TransitionProbe,
        ) -> ClassifiedTransitionResult {
            let coord = if scenario.probe_via_reader {
                &self.reader
            } else {
                &self.admin
            };
            classify(run_transition_probe(coord, &self.fixtures, bucket, probe))
        }
    }

    pub(super) fn phase5_bucket_name_for(index: usize) -> String {
        format!("authz-phase5-{index:05}")
    }

    pub(super) fn to_transition_outcome(result: ClassifiedTransitionResult) -> Outcome {
        match result {
            ClassifiedTransitionResult::Allow => Outcome::Allow,
            ClassifiedTransitionResult::Deny => Outcome::Deny,
        }
    }

    fn materialize_transition_bucket(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
    ) -> Result<(), ServerError> {
        let owner = OwnerIdentity::new(
            fixtures.owner_user.principal(),
            fixtures.owner_user.canonical_user_id().clone(),
        );
        let grants = Coordinator::bucket_acl_grants_from_flags(&owner, false, false);
        coord.create_bucket_with_acl_grants(&owner, bucket, grants, false)?;
        Ok(())
    }

    fn materialize_transition_seed(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        seed: TransitionSeed,
    ) -> Result<(), ServerError> {
        let acl = match seed {
            TransitionSeed::PrivateObject => PutObjectWriteAcl::None,
            TransitionSeed::PublicReadAcl => PutObjectAcl::PublicRead.into(),
            TransitionSeed::ExplicitGrantReadToCrossAccount => {
                PutObjectWriteAcl::Grants(AclGrants::new(vec![
                    AclGrant::new(
                        AclGrantee::CanonicalUser(fixtures.owner_user.canonical_user_id().clone()),
                        AclPermission::FullControl,
                    ),
                    AclGrant::new(
                        AclGrantee::CanonicalUser(
                            fixtures.cross_account.canonical_user_id().clone(),
                        ),
                        AclPermission::Read,
                    ),
                ]))
            }
        };

        test_helpers::put_object(
            coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: ObjectRequest::new(
                    trusted_bucket_name(bucket),
                    trusted_object_key(PHASE5_KEY),
                    Requester::authenticated(fixtures.owner_user.clone()),
                    None,
                ),
                data: b"phase-5-object",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,
                acl,
            },
        )
        .map(|_| ())
    }

    fn apply_transition_mutation(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        mutation: TransitionMutation,
    ) -> Result<(), ServerError> {
        let owner_requester = Requester::authenticated(fixtures.owner_user.clone());
        match mutation {
            TransitionMutation::EnableBucketOwnerEnforced => {
                coord.put_bucket_ownership_controls(&PutBucketOwnershipControlsRequest {
                    bucket: BucketRequest::new(trusted_bucket_name(bucket), owner_requester, None),
                    config: BucketOwnershipControls {
                        object_ownership: BucketObjectOwnership::BucketOwnerEnforced,
                    },
                })
            }
            TransitionMutation::DeleteOwnershipControls => coord.delete_bucket_ownership_controls(
                &BucketRequest::new(trusted_bucket_name(bucket), owner_requester, None),
            ),
            TransitionMutation::EnableIgnorePublicAcls => {
                coord.put_bucket_public_access_block(&PutBucketPublicAccessBlockRequest {
                    bucket: BucketRequest::new(trusted_bucket_name(bucket), owner_requester, None),
                    config: PublicAccessBlockConfig {
                        block_public_acls: false,
                        ignore_public_acls: true,
                        block_public_policy: false,
                        restrict_public_buckets: false,
                    },
                })
            }
            TransitionMutation::DeletePublicAccessBlock => coord.delete_bucket_public_access_block(
                &BucketRequest::new(trusted_bucket_name(bucket), owner_requester, None),
            ),
            TransitionMutation::SetPolicy(policy) => match policy {
                TransitionPolicyState::None => coord.delete_bucket_policy(&BucketRequest::new(
                    trusted_bucket_name(bucket),
                    owner_requester,
                    None,
                )),
                TransitionPolicyState::AllowCrossAccountRead
                | TransitionPolicyState::DenyCrossAccountRead => {
                    let policy_document = transition_policy_document(fixtures, bucket, policy);
                    coord.put_bucket_policy(&PutBucketPolicyRequest {
                        bucket: BucketRequest::new(
                            trusted_bucket_name(bucket),
                            owner_requester,
                            None,
                        ),
                        config: &policy_document,
                        confirm_remove_self_bucket_access: false,
                    })
                }
            },
        }
    }

    fn transition_policy_document(
        fixtures: &IdentityFixtures,
        bucket: &str,
        policy: TransitionPolicyState,
    ) -> String {
        let effect = match policy {
            TransitionPolicyState::AllowCrossAccountRead => "Allow",
            TransitionPolicyState::DenyCrossAccountRead => "Deny",
            TransitionPolicyState::None => panic!("no policy document for no-policy state"),
        };
        let principal = fixtures.cross_account.principal();
        format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"{effect}","Principal":{{"AWS":"{principal}"}},"Action":"s3:GetObject","Resource":"arn:aws:s3:::{bucket}/{PHASE5_KEY}"}}]}}"#
        )
    }

    fn run_transition_probe(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        probe: TransitionProbe,
    ) -> Result<(), ServerError> {
        let requester = match probe {
            TransitionProbe::AnonymousGetObject => Requester::anonymous(),
            TransitionProbe::CrossAccountGetObject => {
                Requester::authenticated(fixtures.cross_account.clone())
            }
        };
        coord
            .get_object(&GetObjectRequest {
                sse_customer: None,
                object: ObjectVersionRequest::new(
                    trusted_bucket_name(bucket),
                    trusted_object_key(PHASE5_KEY),
                    None,
                    requester,
                    None,
                ),
                cond: NO_READ,
            })
            .and_then(|result| {
                let _ = read_all_transition_body(result.body)?;
                Ok(())
            })
    }

    fn classify(result: Result<(), ServerError>) -> ClassifiedTransitionResult {
        match result {
            Ok(()) => ClassifiedTransitionResult::Allow,
            Err(ServerError::AccessDenied | ServerError::AnonymousApiAccessDenied) => {
                ClassifiedTransitionResult::Deny
            }
            Err(other) => panic!("unexpected phase 5 classified result: {other:?}"),
        }
    }

    fn read_all_transition_body(mut body: ReadHandle) -> Result<Vec<u8>, ServerError> {
        let mut out = Vec::new();
        while let Some(chunk) = body.next_chunk(INTERNAL_SEGMENT_SIZE)? {
            out.extend_from_slice(&chunk);
        }
        Ok(out)
    }
}

mod phase6_model {
    use super::model::OwnershipShape;
    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum CopyRequesterShape {
        BucketOwnerPrincipal,
        CrossAccountPrincipal,
    }

    impl CopyRequesterShape {
        fn is_bucket_owner_account(self) -> bool {
            self == Self::BucketOwnerPrincipal
        }
    }

    impl fmt::Display for CopyRequesterShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::BucketOwnerPrincipal => f.write_str("bucket-owner-principal"),
                Self::CrossAccountPrincipal => f.write_str("cross-account"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct CopyBucketShape {
        pub(super) ownership: OwnershipShape,
        pub(super) block_public_acls: bool,
        pub(super) restrict_public_buckets: bool,
        pub(super) bucket_public_write: bool,
    }

    impl CopyBucketShape {
        const PRIVATE: Self = Self {
            ownership: OwnershipShape::ObjectWriter,
            block_public_acls: false,
            restrict_public_buckets: false,
            bucket_public_write: false,
        };

        const BLOCK_PUBLIC_ACLS: Self = Self {
            ownership: OwnershipShape::ObjectWriter,
            block_public_acls: true,
            restrict_public_buckets: false,
            bucket_public_write: false,
        };

        const RESTRICT_PUBLIC_BUCKETS: Self = Self {
            ownership: OwnershipShape::ObjectWriter,
            block_public_acls: false,
            restrict_public_buckets: true,
            bucket_public_write: false,
        };

        const BOE: Self = Self {
            ownership: OwnershipShape::BucketOwnerEnforced,
            block_public_acls: false,
            restrict_public_buckets: false,
            bucket_public_write: false,
        };
    }

    impl fmt::Display for CopyBucketShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "{}(block_public_acls={}, restrict_public_buckets={}, public_write={})",
                self.ownership,
                self.block_public_acls,
                self.restrict_public_buckets,
                self.bucket_public_write
            )
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum CopySourceAccess {
        Readable,
        Unreadable,
    }

    impl fmt::Display for CopySourceAccess {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Readable => f.write_str("readable"),
                Self::Unreadable => f.write_str("unreadable"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum CopyDirectiveShape {
        CopyImplicit,
        CopyExplicit,
    }

    impl fmt::Display for CopyDirectiveShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::CopyImplicit => f.write_str("copy-implicit"),
                Self::CopyExplicit => f.write_str("copy-explicit"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum CopyTaggingShape {
        Copy,
        ReplaceMatching,
    }

    impl fmt::Display for CopyTaggingShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Copy => f.write_str("copy"),
                Self::ReplaceMatching => f.write_str("replace-matching"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum CopyAclShape {
        None,
        CannedPrivate,
        CannedPublicRead,
        GrantRead,
        GrantReadAcp,
        GrantWrite,
        GrantWriteAcp,
    }

    impl fmt::Display for CopyAclShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::None => f.write_str("none"),
                Self::CannedPrivate => f.write_str("canned-private"),
                Self::CannedPublicRead => f.write_str("canned-public-read"),
                Self::GrantRead => f.write_str("grant-read"),
                Self::GrantReadAcp => f.write_str("grant-read-acp"),
                Self::GrantWrite => f.write_str("grant-write"),
                Self::GrantWriteAcp => f.write_str("grant-write-acp"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum CopyPolicyShape {
        NoPolicy,
        AllowPrivate,
        AllowPublic,
        AllowWithCopySourceCondition,
        AllowWithMetadataDirectiveCopy,
        AllowPrivateWithPublicAclDeny,
        AllowWithGrantReadCondition,
        AllowWithGrantReadAcpCondition,
        AllowWithGrantWriteCondition,
        AllowWithGrantWriteAcpCondition,
        AllowWithRequestTagsAndTaggingPermission,
        AllowWithRequestTagsWithoutTaggingPermission,
    }

    impl fmt::Display for CopyPolicyShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::NoPolicy => f.write_str("no-policy"),
                Self::AllowPrivate => f.write_str("allow-private"),
                Self::AllowPublic => f.write_str("allow-public"),
                Self::AllowWithCopySourceCondition => f.write_str("allow-copy-source-condition"),
                Self::AllowWithMetadataDirectiveCopy => {
                    f.write_str("allow-metadata-directive-copy")
                }
                Self::AllowPrivateWithPublicAclDeny => {
                    f.write_str("allow-private-with-public-acl-deny")
                }
                Self::AllowWithGrantReadCondition => f.write_str("allow-grant-read-condition"),
                Self::AllowWithGrantReadAcpCondition => {
                    f.write_str("allow-grant-read-acp-condition")
                }
                Self::AllowWithGrantWriteCondition => f.write_str("allow-grant-write-condition"),
                Self::AllowWithGrantWriteAcpCondition => {
                    f.write_str("allow-grant-write-acp-condition")
                }
                Self::AllowWithRequestTagsAndTaggingPermission => {
                    f.write_str("allow-request-tags-with-tagging-permission")
                }
                Self::AllowWithRequestTagsWithoutTaggingPermission => {
                    f.write_str("allow-request-tags-without-tagging-permission")
                }
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum CopyContextShape {
        Exact,
        CopySourceMismatch,
        GrantReadMismatch,
        GrantReadAcpMismatch,
        GrantWriteMismatch,
        GrantWriteAcpMismatch,
    }

    impl fmt::Display for CopyContextShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Exact => f.write_str("exact"),
                Self::CopySourceMismatch => f.write_str("copy-source-mismatch"),
                Self::GrantReadMismatch => f.write_str("grant-read-mismatch"),
                Self::GrantReadAcpMismatch => f.write_str("grant-read-acp-mismatch"),
                Self::GrantWriteMismatch => f.write_str("grant-write-mismatch"),
                Self::GrantWriteAcpMismatch => f.write_str("grant-write-acp-mismatch"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum CopyOutcome {
        Allow,
        Deny,
        AclNotSupported,
    }

    impl fmt::Display for CopyOutcome {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Allow => f.write_str("Allow"),
                Self::Deny => f.write_str("Deny"),
                Self::AclNotSupported => f.write_str("AclNotSupported"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct CopyObjectScenario {
        pub(super) name: &'static str,
        pub(super) requester: CopyRequesterShape,
        pub(super) bucket: CopyBucketShape,
        pub(super) source_access: CopySourceAccess,
        pub(super) acl: CopyAclShape,
        pub(super) directive: CopyDirectiveShape,
        pub(super) tagging: CopyTaggingShape,
        pub(super) policy: CopyPolicyShape,
        pub(super) context: CopyContextShape,
    }

    impl CopyObjectScenario {
        pub(super) fn scenarios() -> Vec<Self> {
            vec![
                Self {
                    name: "owner-default-copy-allowed",
                    requester: CopyRequesterShape::BucketOwnerPrincipal,
                    bucket: CopyBucketShape::PRIVATE,
                    source_access: CopySourceAccess::Readable,
                    acl: CopyAclShape::None,
                    directive: CopyDirectiveShape::CopyImplicit,
                    tagging: CopyTaggingShape::Copy,
                    policy: CopyPolicyShape::NoPolicy,
                    context: CopyContextShape::Exact,
                },
                Self {
                    name: "cross-account-copy-source-condition-allowed",
                    requester: CopyRequesterShape::CrossAccountPrincipal,
                    bucket: CopyBucketShape::PRIVATE,
                    source_access: CopySourceAccess::Readable,
                    acl: CopyAclShape::None,
                    directive: CopyDirectiveShape::CopyImplicit,
                    tagging: CopyTaggingShape::Copy,
                    policy: CopyPolicyShape::AllowWithCopySourceCondition,
                    context: CopyContextShape::Exact,
                },
                Self {
                    name: "cross-account-copy-source-condition-mismatch-denied",
                    requester: CopyRequesterShape::CrossAccountPrincipal,
                    bucket: CopyBucketShape::PRIVATE,
                    source_access: CopySourceAccess::Readable,
                    acl: CopyAclShape::None,
                    directive: CopyDirectiveShape::CopyImplicit,
                    tagging: CopyTaggingShape::Copy,
                    policy: CopyPolicyShape::AllowWithCopySourceCondition,
                    context: CopyContextShape::CopySourceMismatch,
                },
                Self {
                    name: "cross-account-metadata-directive-copy-explicit-allowed",
                    requester: CopyRequesterShape::CrossAccountPrincipal,
                    bucket: CopyBucketShape::PRIVATE,
                    source_access: CopySourceAccess::Readable,
                    acl: CopyAclShape::None,
                    directive: CopyDirectiveShape::CopyExplicit,
                    tagging: CopyTaggingShape::Copy,
                    policy: CopyPolicyShape::AllowWithMetadataDirectiveCopy,
                    context: CopyContextShape::Exact,
                },
                Self {
                    name: "cross-account-metadata-directive-copy-implicit-denied",
                    requester: CopyRequesterShape::CrossAccountPrincipal,
                    bucket: CopyBucketShape::PRIVATE,
                    source_access: CopySourceAccess::Readable,
                    acl: CopyAclShape::None,
                    directive: CopyDirectiveShape::CopyImplicit,
                    tagging: CopyTaggingShape::Copy,
                    policy: CopyPolicyShape::AllowWithMetadataDirectiveCopy,
                    context: CopyContextShape::Exact,
                },
                Self {
                    name: "cross-account-request-tags-need-tagging-permission",
                    requester: CopyRequesterShape::CrossAccountPrincipal,
                    bucket: CopyBucketShape::PRIVATE,
                    source_access: CopySourceAccess::Readable,
                    acl: CopyAclShape::None,
                    directive: CopyDirectiveShape::CopyImplicit,
                    tagging: CopyTaggingShape::ReplaceMatching,
                    policy: CopyPolicyShape::AllowWithRequestTagsWithoutTaggingPermission,
                    context: CopyContextShape::Exact,
                },
                Self {
                    name: "cross-account-request-tags-with-tagging-permission-allowed",
                    requester: CopyRequesterShape::CrossAccountPrincipal,
                    bucket: CopyBucketShape::PRIVATE,
                    source_access: CopySourceAccess::Readable,
                    acl: CopyAclShape::None,
                    directive: CopyDirectiveShape::CopyImplicit,
                    tagging: CopyTaggingShape::ReplaceMatching,
                    policy: CopyPolicyShape::AllowWithRequestTagsAndTaggingPermission,
                    context: CopyContextShape::Exact,
                },
                Self {
                    name: "cross-account-private-acl-allowed-with-public-acl-deny-policy",
                    requester: CopyRequesterShape::CrossAccountPrincipal,
                    bucket: CopyBucketShape::PRIVATE,
                    source_access: CopySourceAccess::Readable,
                    acl: CopyAclShape::CannedPrivate,
                    directive: CopyDirectiveShape::CopyImplicit,
                    tagging: CopyTaggingShape::Copy,
                    policy: CopyPolicyShape::AllowPrivateWithPublicAclDeny,
                    context: CopyContextShape::Exact,
                },
                Self {
                    name: "cross-account-public-acl-denied-by-policy-condition",
                    requester: CopyRequesterShape::CrossAccountPrincipal,
                    bucket: CopyBucketShape::PRIVATE,
                    source_access: CopySourceAccess::Readable,
                    acl: CopyAclShape::CannedPublicRead,
                    directive: CopyDirectiveShape::CopyImplicit,
                    tagging: CopyTaggingShape::Copy,
                    policy: CopyPolicyShape::AllowPrivateWithPublicAclDeny,
                    context: CopyContextShape::Exact,
                },
                Self {
                    name: "cross-account-grant-read-condition-allowed",
                    requester: CopyRequesterShape::CrossAccountPrincipal,
                    bucket: CopyBucketShape::PRIVATE,
                    source_access: CopySourceAccess::Readable,
                    acl: CopyAclShape::GrantRead,
                    directive: CopyDirectiveShape::CopyImplicit,
                    tagging: CopyTaggingShape::Copy,
                    policy: CopyPolicyShape::AllowWithGrantReadCondition,
                    context: CopyContextShape::Exact,
                },
                Self {
                    name: "cross-account-grant-read-condition-mismatch-denied",
                    requester: CopyRequesterShape::CrossAccountPrincipal,
                    bucket: CopyBucketShape::PRIVATE,
                    source_access: CopySourceAccess::Readable,
                    acl: CopyAclShape::GrantRead,
                    directive: CopyDirectiveShape::CopyImplicit,
                    tagging: CopyTaggingShape::Copy,
                    policy: CopyPolicyShape::AllowWithGrantReadCondition,
                    context: CopyContextShape::GrantReadMismatch,
                },
                Self {
                    name: "cross-account-grant-read-acp-condition-allowed",
                    requester: CopyRequesterShape::CrossAccountPrincipal,
                    bucket: CopyBucketShape::PRIVATE,
                    source_access: CopySourceAccess::Readable,
                    acl: CopyAclShape::GrantReadAcp,
                    directive: CopyDirectiveShape::CopyImplicit,
                    tagging: CopyTaggingShape::Copy,
                    policy: CopyPolicyShape::AllowWithGrantReadAcpCondition,
                    context: CopyContextShape::Exact,
                },
                Self {
                    name: "cross-account-grant-read-acp-condition-mismatch-denied",
                    requester: CopyRequesterShape::CrossAccountPrincipal,
                    bucket: CopyBucketShape::PRIVATE,
                    source_access: CopySourceAccess::Readable,
                    acl: CopyAclShape::GrantReadAcp,
                    directive: CopyDirectiveShape::CopyImplicit,
                    tagging: CopyTaggingShape::Copy,
                    policy: CopyPolicyShape::AllowWithGrantReadAcpCondition,
                    context: CopyContextShape::GrantReadAcpMismatch,
                },
                Self {
                    name: "cross-account-grant-write-condition-allowed",
                    requester: CopyRequesterShape::CrossAccountPrincipal,
                    bucket: CopyBucketShape::PRIVATE,
                    source_access: CopySourceAccess::Readable,
                    acl: CopyAclShape::GrantWrite,
                    directive: CopyDirectiveShape::CopyImplicit,
                    tagging: CopyTaggingShape::Copy,
                    policy: CopyPolicyShape::AllowWithGrantWriteCondition,
                    context: CopyContextShape::Exact,
                },
                Self {
                    name: "cross-account-grant-write-condition-mismatch-denied",
                    requester: CopyRequesterShape::CrossAccountPrincipal,
                    bucket: CopyBucketShape::PRIVATE,
                    source_access: CopySourceAccess::Readable,
                    acl: CopyAclShape::GrantWrite,
                    directive: CopyDirectiveShape::CopyImplicit,
                    tagging: CopyTaggingShape::Copy,
                    policy: CopyPolicyShape::AllowWithGrantWriteCondition,
                    context: CopyContextShape::GrantWriteMismatch,
                },
                Self {
                    name: "cross-account-grant-write-acp-condition-allowed",
                    requester: CopyRequesterShape::CrossAccountPrincipal,
                    bucket: CopyBucketShape::PRIVATE,
                    source_access: CopySourceAccess::Readable,
                    acl: CopyAclShape::GrantWriteAcp,
                    directive: CopyDirectiveShape::CopyImplicit,
                    tagging: CopyTaggingShape::Copy,
                    policy: CopyPolicyShape::AllowWithGrantWriteAcpCondition,
                    context: CopyContextShape::Exact,
                },
                Self {
                    name: "cross-account-grant-write-acp-condition-mismatch-denied",
                    requester: CopyRequesterShape::CrossAccountPrincipal,
                    bucket: CopyBucketShape::PRIVATE,
                    source_access: CopySourceAccess::Readable,
                    acl: CopyAclShape::GrantWriteAcp,
                    directive: CopyDirectiveShape::CopyImplicit,
                    tagging: CopyTaggingShape::Copy,
                    policy: CopyPolicyShape::AllowWithGrantWriteAcpCondition,
                    context: CopyContextShape::GrantWriteAcpMismatch,
                },
                Self {
                    name: "cross-account-public-acl-denied-by-block-public-acls",
                    requester: CopyRequesterShape::CrossAccountPrincipal,
                    bucket: CopyBucketShape::BLOCK_PUBLIC_ACLS,
                    source_access: CopySourceAccess::Readable,
                    acl: CopyAclShape::CannedPublicRead,
                    directive: CopyDirectiveShape::CopyImplicit,
                    tagging: CopyTaggingShape::Copy,
                    policy: CopyPolicyShape::AllowPrivate,
                    context: CopyContextShape::Exact,
                },
                Self {
                    name: "cross-account-public-policy-restricted-by-public-access-block",
                    requester: CopyRequesterShape::CrossAccountPrincipal,
                    bucket: CopyBucketShape::RESTRICT_PUBLIC_BUCKETS,
                    source_access: CopySourceAccess::Readable,
                    acl: CopyAclShape::None,
                    directive: CopyDirectiveShape::CopyImplicit,
                    tagging: CopyTaggingShape::Copy,
                    policy: CopyPolicyShape::AllowPublic,
                    context: CopyContextShape::Exact,
                },
                Self {
                    name: "owner-copy-public-read-acl-not-supported-under-boe",
                    requester: CopyRequesterShape::BucketOwnerPrincipal,
                    bucket: CopyBucketShape::BOE,
                    source_access: CopySourceAccess::Readable,
                    acl: CopyAclShape::CannedPublicRead,
                    directive: CopyDirectiveShape::CopyImplicit,
                    tagging: CopyTaggingShape::Copy,
                    policy: CopyPolicyShape::NoPolicy,
                    context: CopyContextShape::Exact,
                },
                Self {
                    name: "cross-account-unreadable-source-denied",
                    requester: CopyRequesterShape::CrossAccountPrincipal,
                    bucket: CopyBucketShape::PRIVATE,
                    source_access: CopySourceAccess::Unreadable,
                    acl: CopyAclShape::None,
                    directive: CopyDirectiveShape::CopyImplicit,
                    tagging: CopyTaggingShape::Copy,
                    policy: CopyPolicyShape::AllowPrivate,
                    context: CopyContextShape::Exact,
                },
            ]
        }

        pub(super) fn expected_outcome(self) -> CopyOutcome {
            if self.source_access == CopySourceAccess::Unreadable {
                return CopyOutcome::Deny;
            }

            let fallback =
                self.requester.is_bucket_owner_account() || self.bucket.bucket_public_write;
            let allowed = match self.policy {
                CopyPolicyShape::NoPolicy => fallback,
                CopyPolicyShape::AllowPrivate => true,
                CopyPolicyShape::AllowPublic => {
                    if !self.bucket.restrict_public_buckets
                        || self.requester.is_bucket_owner_account()
                    {
                        true
                    } else {
                        fallback
                    }
                }
                CopyPolicyShape::AllowWithCopySourceCondition => {
                    self.context == CopyContextShape::Exact
                }
                CopyPolicyShape::AllowWithMetadataDirectiveCopy => {
                    self.directive == CopyDirectiveShape::CopyExplicit
                }
                CopyPolicyShape::AllowPrivateWithPublicAclDeny => !self.acl.is_public_acl(),
                CopyPolicyShape::AllowWithGrantReadCondition => {
                    self.acl == CopyAclShape::GrantRead && self.context == CopyContextShape::Exact
                }
                CopyPolicyShape::AllowWithGrantReadAcpCondition => {
                    self.acl == CopyAclShape::GrantReadAcp
                        && self.context == CopyContextShape::Exact
                }
                CopyPolicyShape::AllowWithGrantWriteCondition => {
                    self.acl == CopyAclShape::GrantWrite && self.context == CopyContextShape::Exact
                }
                CopyPolicyShape::AllowWithGrantWriteAcpCondition => {
                    self.acl == CopyAclShape::GrantWriteAcp
                        && self.context == CopyContextShape::Exact
                }
                CopyPolicyShape::AllowWithRequestTagsAndTaggingPermission => {
                    self.tagging == CopyTaggingShape::ReplaceMatching
                }
                CopyPolicyShape::AllowWithRequestTagsWithoutTaggingPermission => false,
            };
            if !allowed {
                return CopyOutcome::Deny;
            }
            if self.bucket.ownership == OwnershipShape::BucketOwnerEnforced
                && !self.acl.is_supported_under_boe()
            {
                return CopyOutcome::AclNotSupported;
            }
            if self.bucket.block_public_acls && self.acl.is_public_acl() {
                return CopyOutcome::Deny;
            }
            CopyOutcome::Allow
        }
    }

    impl fmt::Display for CopyObjectScenario {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "name={} requester={} bucket={} source={} acl={} directive={} tagging={} policy={} context={}",
                self.name,
                self.requester,
                self.bucket,
                self.source_access,
                self.acl,
                self.directive,
                self.tagging,
                self.policy,
                self.context
            )
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum UploadOwnerShape {
        Requester,
        BucketOwner,
    }

    impl fmt::Display for UploadOwnerShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Requester => f.write_str("requester"),
                Self::BucketOwner => f.write_str("bucket-owner"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum UploadStateShape {
        InProgress,
        Completed,
    }

    impl fmt::Display for UploadStateShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::InProgress => f.write_str("in-progress"),
                Self::Completed => f.write_str("completed"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum UploadPartCopyPolicyShape {
        NoPolicy,
        AllowPrivate,
        AllowWithCopySourceCondition,
        AllowWithMetadataDirectiveCopy,
    }

    impl fmt::Display for UploadPartCopyPolicyShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::NoPolicy => f.write_str("no-policy"),
                Self::AllowPrivate => f.write_str("allow-private"),
                Self::AllowWithCopySourceCondition => f.write_str("allow-copy-source-condition"),
                Self::AllowWithMetadataDirectiveCopy => {
                    f.write_str("allow-metadata-directive-copy")
                }
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum UploadPartCopyOutcome {
        Allow,
        Deny,
        NoSuchUpload,
    }

    impl fmt::Display for UploadPartCopyOutcome {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Allow => f.write_str("Allow"),
                Self::Deny => f.write_str("Deny"),
                Self::NoSuchUpload => f.write_str("NoSuchUpload"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct UploadPartCopyScenario {
        pub(super) name: &'static str,
        pub(super) requester: CopyRequesterShape,
        pub(super) bucket: CopyBucketShape,
        pub(super) source_access: CopySourceAccess,
        pub(super) upload_owner: UploadOwnerShape,
        pub(super) upload_state: UploadStateShape,
        pub(super) policy: UploadPartCopyPolicyShape,
        pub(super) context: CopyContextShape,
    }

    impl UploadPartCopyScenario {
        pub(super) fn scenarios() -> Vec<Self> {
            vec![
                Self {
                    name: "owner-in-progress-default-allowed",
                    requester: CopyRequesterShape::BucketOwnerPrincipal,
                    bucket: CopyBucketShape::PRIVATE,
                    source_access: CopySourceAccess::Readable,
                    upload_owner: UploadOwnerShape::BucketOwner,
                    upload_state: UploadStateShape::InProgress,
                    policy: UploadPartCopyPolicyShape::NoPolicy,
                    context: CopyContextShape::Exact,
                },
                Self {
                    name: "owner-completed-upload-no-such-upload",
                    requester: CopyRequesterShape::BucketOwnerPrincipal,
                    bucket: CopyBucketShape::PRIVATE,
                    source_access: CopySourceAccess::Readable,
                    upload_owner: UploadOwnerShape::BucketOwner,
                    upload_state: UploadStateShape::Completed,
                    policy: UploadPartCopyPolicyShape::NoPolicy,
                    context: CopyContextShape::Exact,
                },
                Self {
                    name: "cross-account-non-owner-no-policy-denied",
                    requester: CopyRequesterShape::CrossAccountPrincipal,
                    bucket: CopyBucketShape::PRIVATE,
                    source_access: CopySourceAccess::Readable,
                    upload_owner: UploadOwnerShape::BucketOwner,
                    upload_state: UploadStateShape::InProgress,
                    policy: UploadPartCopyPolicyShape::NoPolicy,
                    context: CopyContextShape::Exact,
                },
                Self {
                    name: "cross-account-own-upload-copy-source-condition-allowed",
                    requester: CopyRequesterShape::CrossAccountPrincipal,
                    bucket: CopyBucketShape::PRIVATE,
                    source_access: CopySourceAccess::Readable,
                    upload_owner: UploadOwnerShape::Requester,
                    upload_state: UploadStateShape::InProgress,
                    policy: UploadPartCopyPolicyShape::AllowWithCopySourceCondition,
                    context: CopyContextShape::Exact,
                },
                Self {
                    name: "cross-account-own-upload-copy-source-condition-mismatch-denied",
                    requester: CopyRequesterShape::CrossAccountPrincipal,
                    bucket: CopyBucketShape::PRIVATE,
                    source_access: CopySourceAccess::Readable,
                    upload_owner: UploadOwnerShape::Requester,
                    upload_state: UploadStateShape::InProgress,
                    policy: UploadPartCopyPolicyShape::AllowWithCopySourceCondition,
                    context: CopyContextShape::CopySourceMismatch,
                },
                Self {
                    name: "cross-account-own-upload-metadata-directive-policy-denied",
                    requester: CopyRequesterShape::CrossAccountPrincipal,
                    bucket: CopyBucketShape::PRIVATE,
                    source_access: CopySourceAccess::Readable,
                    upload_owner: UploadOwnerShape::Requester,
                    upload_state: UploadStateShape::InProgress,
                    policy: UploadPartCopyPolicyShape::AllowWithMetadataDirectiveCopy,
                    context: CopyContextShape::Exact,
                },
                Self {
                    name: "cross-account-own-upload-unreadable-source-denied",
                    requester: CopyRequesterShape::CrossAccountPrincipal,
                    bucket: CopyBucketShape::PRIVATE,
                    source_access: CopySourceAccess::Unreadable,
                    upload_owner: UploadOwnerShape::Requester,
                    upload_state: UploadStateShape::InProgress,
                    policy: UploadPartCopyPolicyShape::AllowPrivate,
                    context: CopyContextShape::Exact,
                },
            ]
        }

        pub(super) fn expected_outcome(self) -> UploadPartCopyOutcome {
            if self.upload_state == UploadStateShape::Completed {
                return UploadPartCopyOutcome::NoSuchUpload;
            }
            if self.source_access == CopySourceAccess::Unreadable {
                return UploadPartCopyOutcome::Deny;
            }

            let can_manage = match self.upload_owner {
                UploadOwnerShape::Requester => true,
                UploadOwnerShape::BucketOwner => {
                    self.requester == CopyRequesterShape::BucketOwnerPrincipal
                }
            };
            let can_write_bucket = self.requester == CopyRequesterShape::BucketOwnerPrincipal
                || self.bucket.bucket_public_write;
            let fallback = can_manage && can_write_bucket;
            let allowed = match self.policy {
                UploadPartCopyPolicyShape::NoPolicy => fallback,
                UploadPartCopyPolicyShape::AllowPrivate => true,
                UploadPartCopyPolicyShape::AllowWithCopySourceCondition => {
                    self.context == CopyContextShape::Exact
                }
                UploadPartCopyPolicyShape::AllowWithMetadataDirectiveCopy => false,
            };

            if allowed {
                UploadPartCopyOutcome::Allow
            } else {
                UploadPartCopyOutcome::Deny
            }
        }
    }

    impl fmt::Display for UploadPartCopyScenario {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "name={} requester={} bucket={} source={} upload_owner={} upload_state={} policy={} context={}",
                self.name,
                self.requester,
                self.bucket,
                self.source_access,
                self.upload_owner,
                self.upload_state,
                self.policy,
                self.context
            )
        }
    }

    impl CopyAclShape {
        fn is_public_acl(self) -> bool {
            matches!(self, Self::CannedPublicRead)
        }

        fn is_supported_under_boe(self) -> bool {
            matches!(self, Self::None | Self::CannedPrivate)
        }

        pub(super) fn canned_acl_condition_value(self) -> Option<&'static str> {
            match self {
                Self::None
                | Self::GrantRead
                | Self::GrantReadAcp
                | Self::GrantWrite
                | Self::GrantWriteAcp => None,
                Self::CannedPrivate => Some("private"),
                Self::CannedPublicRead => Some("public-read"),
            }
        }
    }
}

mod phase6_harness {
    use super::harness::{setup_coordinator, IdentityFixtures};
    use super::model::OwnershipShape;
    use super::phase6_model::{
        CopyAclShape, CopyBucketShape, CopyContextShape, CopyDirectiveShape, CopyObjectScenario,
        CopyOutcome, CopyPolicyShape, CopyRequesterShape, CopySourceAccess, CopyTaggingShape,
        UploadOwnerShape, UploadPartCopyOutcome, UploadPartCopyPolicyShape, UploadPartCopyScenario,
        UploadStateShape,
    };
    use super::*;

    const SRC_KEY: &str = "source-key";
    const DST_KEY: &str = "dest-key";
    const REPLACEMENT_TAGS_XML: &str =
        "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>";

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum ClassifiedPhase6Result {
        Allow,
        Deny,
        AclNotSupported,
        NoSuchUpload,
    }

    pub(super) struct Phase6Harness {
        _tmp: test_util::TempDir,
        coord: Coordinator,
        fixtures: IdentityFixtures,
    }

    impl Phase6Harness {
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

        pub(super) fn run_copy_object(
            &self,
            bucket: &str,
            scenario: CopyObjectScenario,
        ) -> ClassifiedPhase6Result {
            let src_bucket = format!("{bucket}-src");
            let dst_bucket = format!("{bucket}-dst");
            materialize_source_bucket(
                &self.coord,
                &self.fixtures,
                &src_bucket,
                scenario.requester,
                scenario.source_access,
            )
            .unwrap_or_else(|err| {
                panic!("failed to materialize phase 6 source for {scenario}: {err:?}");
            });
            materialize_copy_bucket(&self.coord, &self.fixtures, &dst_bucket, scenario.bucket)
                .unwrap_or_else(|err| {
                    panic!("failed to materialize phase 6 bucket for {scenario}: {err:?}");
                });
            materialize_copy_policy(&self.coord, &self.fixtures, &dst_bucket, scenario)
                .unwrap_or_else(|err| {
                    panic!("failed to materialize phase 6 copy policy for {scenario}: {err:?}");
                });
            classify(run_copy_object_action(
                &self.coord,
                &self.fixtures,
                &src_bucket,
                &dst_bucket,
                scenario,
            ))
        }

        pub(super) fn run_upload_part_copy(
            &self,
            bucket: &str,
            scenario: UploadPartCopyScenario,
        ) -> ClassifiedPhase6Result {
            let src_bucket = format!("{bucket}-src");
            let dst_bucket = format!("{bucket}-dst");
            materialize_source_bucket(
                &self.coord,
                &self.fixtures,
                &src_bucket,
                scenario.requester,
                scenario.source_access,
            )
            .unwrap_or_else(|err| {
                panic!("failed to materialize phase 6 source for {scenario}: {err:?}");
            });
            materialize_copy_bucket(&self.coord, &self.fixtures, &dst_bucket, scenario.bucket)
                .unwrap_or_else(|err| {
                    panic!("failed to materialize phase 6 bucket for {scenario}: {err:?}");
                });
            let upload_id =
                materialize_upload_target(&self.coord, &self.fixtures, &dst_bucket, scenario)
                    .unwrap_or_else(|err| {
                        panic!(
                            "failed to materialize phase 6 upload target for {scenario}: {err:?}"
                        );
                    });
            materialize_upload_part_copy_policy(
                &self.coord,
                &self.fixtures,
                &dst_bucket,
                scenario,
            )
            .unwrap_or_else(|err| {
                panic!("failed to materialize phase 6 upload-part-copy policy for {scenario}: {err:?}");
            });
            classify(run_upload_part_copy_action(
                &self.coord,
                &self.fixtures,
                &src_bucket,
                &dst_bucket,
                &upload_id,
                scenario,
            ))
        }
    }

    pub(super) fn copy_bucket_name_for(index: usize) -> String {
        format!("authz-phase6-copy-{index:05}")
    }

    pub(super) fn upload_part_copy_bucket_name_for(index: usize) -> String {
        format!("authz-phase6-upc-{index:05}")
    }

    pub(super) fn to_copy_outcome(result: ClassifiedPhase6Result) -> CopyOutcome {
        match result {
            ClassifiedPhase6Result::Allow => CopyOutcome::Allow,
            ClassifiedPhase6Result::Deny => CopyOutcome::Deny,
            ClassifiedPhase6Result::AclNotSupported => CopyOutcome::AclNotSupported,
            ClassifiedPhase6Result::NoSuchUpload => {
                panic!("copy-object matrix produced unexpected NoSuchUpload")
            }
        }
    }

    pub(super) fn to_upload_part_copy_outcome(
        result: ClassifiedPhase6Result,
    ) -> UploadPartCopyOutcome {
        match result {
            ClassifiedPhase6Result::Allow => UploadPartCopyOutcome::Allow,
            ClassifiedPhase6Result::Deny => UploadPartCopyOutcome::Deny,
            ClassifiedPhase6Result::NoSuchUpload => UploadPartCopyOutcome::NoSuchUpload,
            ClassifiedPhase6Result::AclNotSupported => {
                panic!("upload-part-copy matrix produced unexpected AclNotSupported")
            }
        }
    }

    fn materialize_source_bucket(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        requester: CopyRequesterShape,
        source_access: CopySourceAccess,
    ) -> Result<(), ServerError> {
        let owner = OwnerIdentity::new(
            fixtures.owner_user.principal(),
            fixtures.owner_user.canonical_user_id().clone(),
        );
        let grants = Coordinator::bucket_acl_grants_from_flags(&owner, false, false);
        coord.create_bucket_with_acl_grants(&owner, bucket, grants, false)?;
        test_helpers::put_object(
            coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: phase6_object_request(
                    bucket,
                    SRC_KEY,
                    Requester::authenticated(fixtures.owner_user.clone()),
                ),
                data: b"phase-6-source",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: Some(REPLACEMENT_TAGS_XML),
                cond: NO_WRITE,
                acl: PutObjectAcl::None.into(),
            },
        )?;

        if requester == CopyRequesterShape::CrossAccountPrincipal
            && source_access == CopySourceAccess::Readable
        {
            let policy = format!(
                r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"{}"}},"Action":"s3:GetObject","Resource":"arn:aws:s3:::{bucket}/{SRC_KEY}"}}]}}"#,
                fixtures.cross_account.principal()
            );
            coord.put_bucket_policy(&PutBucketPolicyRequest {
                bucket: phase6_bucket_request(
                    bucket,
                    Requester::authenticated(fixtures.owner_user.clone()),
                ),
                config: &policy,
                confirm_remove_self_bucket_access: false,
            })?;
        }

        Ok(())
    }

    fn materialize_copy_bucket(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        shape: CopyBucketShape,
    ) -> Result<(), ServerError> {
        let owner = OwnerIdentity::new(
            fixtures.owner_user.principal(),
            fixtures.owner_user.canonical_user_id().clone(),
        );
        let grants =
            Coordinator::bucket_acl_grants_from_flags(&owner, false, shape.bucket_public_write);
        coord.create_bucket_with_acl_grants(&owner, bucket, grants, false)?;

        if shape.ownership == OwnershipShape::BucketOwnerEnforced {
            coord.put_bucket_ownership_controls(&PutBucketOwnershipControlsRequest {
                bucket: BucketRequest::new(
                    trusted_bucket_name(bucket),
                    Requester::authenticated(fixtures.owner_user.clone()),
                    None,
                ),
                config: BucketOwnershipControls {
                    object_ownership: BucketObjectOwnership::BucketOwnerEnforced,
                },
            })?;
        }

        if shape.block_public_acls || shape.restrict_public_buckets {
            coord.put_bucket_public_access_block(&PutBucketPublicAccessBlockRequest {
                bucket: BucketRequest::new(
                    trusted_bucket_name(bucket),
                    Requester::authenticated(fixtures.owner_user.clone()),
                    None,
                ),
                config: PublicAccessBlockConfig {
                    block_public_acls: shape.block_public_acls,
                    ignore_public_acls: false,
                    block_public_policy: false,
                    restrict_public_buckets: shape.restrict_public_buckets,
                },
            })?;
        }

        Ok(())
    }

    fn materialize_copy_policy(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: CopyObjectScenario,
    ) -> Result<(), ServerError> {
        let Some(policy) = copy_policy_document(fixtures, bucket, scenario) else {
            return Ok(());
        };
        coord.put_bucket_policy(&PutBucketPolicyRequest {
            bucket: BucketRequest::new(
                trusted_bucket_name(bucket),
                Requester::authenticated(fixtures.owner_user.clone()),
                None,
            ),
            config: &policy,
            confirm_remove_self_bucket_access: false,
        })
    }

    fn copy_policy_document(
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: CopyObjectScenario,
    ) -> Option<String> {
        let principal = copy_requester_principal(fixtures, scenario.requester)?;
        let object_resource = format!("arn:aws:s3:::{bucket}/{DST_KEY}");
        let mut statements = Vec::new();
        match scenario.policy {
            CopyPolicyShape::NoPolicy => {}
            CopyPolicyShape::AllowPrivate => statements.push(format!(
                r#"{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":"s3:PutObject","Resource":"{object_resource}"}}"#
            )),
            CopyPolicyShape::AllowPublic => statements.push(format!(
                r#"{{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"{object_resource}"}}"#
            )),
            CopyPolicyShape::AllowWithCopySourceCondition => statements.push(format!(
                r#"{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":"s3:PutObject","Resource":"{object_resource}","Condition":{{"StringLike":{{"s3:x-amz-copy-source":"{}"}}}}}}"#,
                copy_source_policy_value(scenario.context)
            )),
            CopyPolicyShape::AllowWithMetadataDirectiveCopy => statements.push(format!(
                r#"{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":"s3:PutObject","Resource":"{object_resource}","Condition":{{"StringEquals":{{"s3:x-amz-metadata-directive":"COPY"}}}}}}"#
            )),
            CopyPolicyShape::AllowPrivateWithPublicAclDeny => {
                statements.push(format!(
                    r#"{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":"s3:PutObject","Resource":"{object_resource}"}}"#
                ));
                statements.push(format!(
                    r#"{{"Effect":"Deny","Principal":{{"AWS":"{principal}"}},"Action":"s3:PutObject","Resource":"{object_resource}","Condition":{{"StringLike":{{"s3:x-amz-acl":"public*"}}}}}}"#
                ));
            }
            CopyPolicyShape::AllowWithGrantReadCondition => {
                let grant_read = copy_grant_read_header(fixtures, scenario.context);
                let grant_read_json = phase6_json_string(grant_read);
                statements.push(format!(
                    r#"{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":"s3:PutObject","Resource":"{object_resource}","Condition":{{"StringEquals":{{"s3:x-amz-grant-read":{grant_read_json}}}}}}}"#
                ));
            }
            CopyPolicyShape::AllowWithGrantReadAcpCondition => {
                let grant_read_acp = copy_grant_read_acp_header(fixtures, scenario.context);
                let grant_read_acp_json = phase6_json_string(grant_read_acp);
                statements.push(format!(
                    r#"{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":"s3:PutObject","Resource":"{object_resource}","Condition":{{"StringEquals":{{"s3:x-amz-grant-read-acp":{grant_read_acp_json}}}}}}}"#
                ));
            }
            CopyPolicyShape::AllowWithGrantWriteCondition => {
                let grant_write = copy_grant_write_header(fixtures, scenario.context);
                let grant_write_json = phase6_json_string(grant_write);
                statements.push(format!(
                    r#"{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":"s3:PutObject","Resource":"{object_resource}","Condition":{{"StringEquals":{{"s3:x-amz-grant-write":{grant_write_json}}}}}}}"#
                ));
            }
            CopyPolicyShape::AllowWithGrantWriteAcpCondition => {
                let grant_write_acp = copy_grant_write_acp_header(fixtures, scenario.context);
                let grant_write_acp_json = phase6_json_string(grant_write_acp);
                statements.push(format!(
                    r#"{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":"s3:PutObject","Resource":"{object_resource}","Condition":{{"StringEquals":{{"s3:x-amz-grant-write-acp":{grant_write_acp_json}}}}}}}"#
                ));
            }
            CopyPolicyShape::AllowWithRequestTagsAndTaggingPermission => {
                let condition = r#"{"StringEquals":{"s3:RequestObjectTag/security":"public"}}"#;
                statements.push(format!(
                    r#"{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":"s3:PutObject","Resource":"{object_resource}","Condition":{condition}}}"#
                ));
                statements.push(format!(
                    r#"{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":"s3:PutObjectTagging","Resource":"{object_resource}","Condition":{condition}}}"#
                ));
            }
            CopyPolicyShape::AllowWithRequestTagsWithoutTaggingPermission => statements.push(format!(
                r#"{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":"s3:PutObject","Resource":"{object_resource}","Condition":{{"StringEquals":{{"s3:RequestObjectTag/security":"public"}}}}}}"#
            )),
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

    fn run_copy_object_action(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        src_bucket: &str,
        dst_bucket: &str,
        scenario: CopyObjectScenario,
    ) -> Result<(), ServerError> {
        let requester = copy_requester(fixtures, scenario.requester);
        let tagging = match scenario.tagging {
            CopyTaggingShape::Copy => TaggingDirective::Copy,
            CopyTaggingShape::ReplaceMatching => {
                TaggingDirective::Replace(Some(REPLACEMENT_TAGS_XML))
            }
        };
        let directive = match scenario.directive {
            CopyDirectiveShape::CopyImplicit => MetadataDirective::Copy,
            CopyDirectiveShape::CopyExplicit => MetadataDirective::CopyExplicit,
        };
        coord
            .copy_object(&CopyObjectRequest {
                source: phase6_copy_source(src_bucket, SRC_KEY),
                destination: phase6_object_request(dst_bucket, DST_KEY, requester),
                dst_condition: NO_WRITE,
                directive,
                tagging,
                acl: copy_acl(fixtures, scenario.acl),
                policy_context: copy_policy_context(fixtures, scenario),
                source_sse_customer: None,
                destination_encryption: WriteEncryptionRequest::none(),
                object_lock: ObjectLockState::default(),
            })
            .map(|_| ())
    }

    fn materialize_upload_target(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: UploadPartCopyScenario,
    ) -> Result<UploadId, ServerError> {
        let owner_requester = match scenario.upload_owner {
            UploadOwnerShape::Requester => copy_requester(fixtures, scenario.requester),
            UploadOwnerShape::BucketOwner => Requester::authenticated(fixtures.owner_user.clone()),
        };

        let temporary_policy = match (scenario.requester, scenario.upload_owner) {
            (CopyRequesterShape::CrossAccountPrincipal, UploadOwnerShape::Requester) => {
                Some(format!(
                    r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"{}"}},"Action":"s3:PutObject","Resource":"arn:aws:s3:::{bucket}/{DST_KEY}"}}]}}"#,
                    fixtures.cross_account.principal()
                ))
            }
            _ => None,
        };

        if let Some(policy) = temporary_policy.as_deref() {
            coord.put_bucket_policy(&PutBucketPolicyRequest {
                bucket: BucketRequest::new(
                    trusted_bucket_name(bucket),
                    Requester::authenticated(fixtures.owner_user.clone()),
                    None,
                ),
                config: policy,
                confirm_remove_self_bucket_access: false,
            })?;
        }

        let upload = coord.create_multipart_upload(&CreateMultipartUploadRequest {
            object: phase6_object_request(bucket, DST_KEY, owner_requester.clone()),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: PutObjectAcl::None.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })?;

        if scenario.upload_state == UploadStateShape::Completed {
            let part = test_helpers::upload_part(
                coord,
                &test_helpers::UploadPartRequest {
                    upload: phase6_multipart_object_request(
                        bucket,
                        DST_KEY,
                        &upload.upload_id,
                        owner_requester,
                    ),
                    part_number: 1,
                    data: b"phase-6-part",
                    claimed_checksum: None,
                    sse_customer: None,
                },
            )?;
            coord.complete_multipart_upload(&CompleteMultipartUploadRequest {
                upload: phase6_multipart_object_request(
                    bucket,
                    DST_KEY,
                    &upload.upload_id,
                    match scenario.upload_owner {
                        UploadOwnerShape::Requester => copy_requester(fixtures, scenario.requester),
                        UploadOwnerShape::BucketOwner => {
                            Requester::authenticated(fixtures.owner_user.clone())
                        }
                    },
                ),
                parts: &[CompletePart {
                    part_number: 1,
                    etag: part.etag,
                    checksum: None,
                }],
                claimed_checksum: None,
                expected_object_size: None,
                cond: NO_WRITE,
                sse_customer: None,
            })?;
        }

        Ok(upload.upload_id)
    }

    fn materialize_upload_part_copy_policy(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: UploadPartCopyScenario,
    ) -> Result<(), ServerError> {
        let Some(policy) = upload_part_copy_policy_document(fixtures, bucket, scenario) else {
            return Ok(());
        };
        coord.put_bucket_policy(&PutBucketPolicyRequest {
            bucket: BucketRequest::new(
                trusted_bucket_name(bucket),
                Requester::authenticated(fixtures.owner_user.clone()),
                None,
            ),
            config: &policy,
            confirm_remove_self_bucket_access: false,
        })
    }

    fn upload_part_copy_policy_document(
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: UploadPartCopyScenario,
    ) -> Option<String> {
        let principal = copy_requester_principal(fixtures, scenario.requester)?;
        let object_resource = format!("arn:aws:s3:::{bucket}/{DST_KEY}");
        let statement = match scenario.policy {
            UploadPartCopyPolicyShape::NoPolicy => return None,
            UploadPartCopyPolicyShape::AllowPrivate => format!(
                r#"{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":"s3:PutObject","Resource":"{object_resource}"}}"#
            ),
            UploadPartCopyPolicyShape::AllowWithCopySourceCondition => format!(
                r#"{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":"s3:PutObject","Resource":"{object_resource}","Condition":{{"StringLike":{{"s3:x-amz-copy-source":"{}"}}}}}}"#,
                copy_source_policy_value(scenario.context)
            ),
            UploadPartCopyPolicyShape::AllowWithMetadataDirectiveCopy => format!(
                r#"{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":"s3:PutObject","Resource":"{object_resource}","Condition":{{"StringEquals":{{"s3:x-amz-metadata-directive":"COPY"}}}}}}"#
            ),
        };
        Some(format!(
            r#"{{"Version":"2012-10-17","Statement":[{statement}]}}"#
        ))
    }

    fn run_upload_part_copy_action(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        src_bucket: &str,
        dst_bucket: &str,
        upload_id: &UploadId,
        scenario: UploadPartCopyScenario,
    ) -> Result<(), ServerError> {
        let requester = copy_requester(fixtures, scenario.requester);
        coord
            .upload_part_copy(&UploadPartCopyRequest {
                source: phase6_copy_source(src_bucket, SRC_KEY),
                upload: phase6_multipart_object_request(dst_bucket, DST_KEY, upload_id, requester),
                part_number: 1,
                copy_source_range: None,
                source_sse_customer: None,
                sse_customer: None,
            })
            .map(|_| ())
    }

    fn classify(result: Result<(), ServerError>) -> ClassifiedPhase6Result {
        match result {
            Ok(()) => ClassifiedPhase6Result::Allow,
            Err(ServerError::AccessDenied | ServerError::AnonymousApiAccessDenied) => {
                ClassifiedPhase6Result::Deny
            }
            Err(ServerError::AccessControlListNotSupported) => {
                ClassifiedPhase6Result::AclNotSupported
            }
            Err(ServerError::NoSuchUpload { .. }) => ClassifiedPhase6Result::NoSuchUpload,
            Err(other) => panic!("unexpected phase 6 classified result: {other:?}"),
        }
    }

    fn copy_requester(fixtures: &IdentityFixtures, shape: CopyRequesterShape) -> Requester {
        match shape {
            CopyRequesterShape::BucketOwnerPrincipal => {
                Requester::authenticated(fixtures.owner_user.clone())
            }
            CopyRequesterShape::CrossAccountPrincipal => {
                Requester::authenticated(fixtures.cross_account.clone())
            }
        }
    }

    fn copy_requester_principal(
        fixtures: &IdentityFixtures,
        shape: CopyRequesterShape,
    ) -> Option<&str> {
        match shape {
            CopyRequesterShape::BucketOwnerPrincipal => Some(fixtures.owner_user.principal()),
            CopyRequesterShape::CrossAccountPrincipal => Some(fixtures.cross_account.principal()),
        }
    }

    fn copy_acl(fixtures: &IdentityFixtures, acl: CopyAclShape) -> PutObjectWriteAcl<'static> {
        match acl {
            CopyAclShape::None => PutObjectAcl::None.into(),
            CopyAclShape::CannedPrivate => PutObjectAcl::Private.into(),
            CopyAclShape::CannedPublicRead => PutObjectAcl::PublicRead.into(),
            CopyAclShape::GrantRead => phase6_acl_grants(fixtures, AclPermission::Read),
            CopyAclShape::GrantReadAcp => phase6_acl_grants(fixtures, AclPermission::ReadAcp),
            CopyAclShape::GrantWrite => phase6_acl_grants(fixtures, AclPermission::Write),
            CopyAclShape::GrantWriteAcp => phase6_acl_grants(fixtures, AclPermission::WriteAcp),
        }
    }

    fn copy_policy_context(
        fixtures: &IdentityFixtures,
        scenario: CopyObjectScenario,
    ) -> PutObjectPolicyContext<'static> {
        PutObjectPolicyContext::new(None, None, scenario.acl.canned_acl_condition_value())
            .with_acl_grant_headers(
                match scenario.acl {
                    CopyAclShape::GrantRead => {
                        Some(copy_grant_read_header(fixtures, CopyContextShape::Exact))
                    }
                    _ => None,
                },
                match scenario.acl {
                    CopyAclShape::GrantWrite => {
                        Some(copy_grant_write_header(fixtures, CopyContextShape::Exact))
                    }
                    _ => None,
                },
                match scenario.acl {
                    CopyAclShape::GrantReadAcp => Some(copy_grant_read_acp_header(
                        fixtures,
                        CopyContextShape::Exact,
                    )),
                    _ => None,
                },
                match scenario.acl {
                    CopyAclShape::GrantWriteAcp => Some(copy_grant_write_acp_header(
                        fixtures,
                        CopyContextShape::Exact,
                    )),
                    _ => None,
                },
                None,
            )
    }

    fn copy_source_policy_value(context: CopyContextShape) -> &'static str {
        match context {
            CopyContextShape::CopySourceMismatch => "*different-source-key",
            CopyContextShape::Exact
            | CopyContextShape::GrantReadMismatch
            | CopyContextShape::GrantReadAcpMismatch
            | CopyContextShape::GrantWriteMismatch
            | CopyContextShape::GrantWriteAcpMismatch => "*source-key",
        }
    }

    fn copy_grant_read_header(
        fixtures: &IdentityFixtures,
        context: CopyContextShape,
    ) -> &'static str {
        phase6_grant_header(
            context,
            CopyContextShape::GrantReadMismatch,
            fixtures.cross_account.canonical_user_id(),
        )
    }

    fn copy_grant_read_acp_header(
        fixtures: &IdentityFixtures,
        context: CopyContextShape,
    ) -> &'static str {
        phase6_grant_header(
            context,
            CopyContextShape::GrantReadAcpMismatch,
            fixtures.cross_account.canonical_user_id(),
        )
    }

    fn copy_grant_write_header(
        fixtures: &IdentityFixtures,
        context: CopyContextShape,
    ) -> &'static str {
        phase6_grant_header(
            context,
            CopyContextShape::GrantWriteMismatch,
            fixtures.cross_account.canonical_user_id(),
        )
    }

    fn copy_grant_write_acp_header(
        fixtures: &IdentityFixtures,
        context: CopyContextShape,
    ) -> &'static str {
        phase6_grant_header(
            context,
            CopyContextShape::GrantWriteAcpMismatch,
            fixtures.cross_account.canonical_user_id(),
        )
    }

    fn phase6_grant_header(
        context: CopyContextShape,
        mismatch: CopyContextShape,
        canonical_id: &CanonicalUserId,
    ) -> &'static str {
        if context == mismatch {
            r#"id="different-grantee""#
        } else {
            Box::leak(format!(r#"id="{}""#, canonical_id).into_boxed_str())
        }
    }

    fn phase6_acl_grants(
        fixtures: &IdentityFixtures,
        permission: AclPermission,
    ) -> PutObjectWriteAcl<'static> {
        PutObjectWriteAcl::Grants(AclGrants::new(vec![AclGrant::new(
            AclGrantee::CanonicalUser(fixtures.cross_account.canonical_user_id().clone()),
            permission,
        )]))
    }

    fn phase6_json_string(value: &str) -> String {
        format!(r#""{}""#, value.replace('\\', "\\\\").replace('"', "\\\""))
    }

    fn phase6_bucket_request<'a>(bucket: &'a str, requester: Requester) -> BucketRequest<'a> {
        BucketRequest::new(trusted_bucket_name(bucket), requester, None)
    }

    fn phase6_object_request<'a>(
        bucket: &'a str,
        key: &'a str,
        requester: Requester,
    ) -> ObjectRequest<'a> {
        ObjectRequest::new(
            trusted_bucket_name(bucket),
            trusted_object_key(key),
            requester,
            None,
        )
    }

    fn phase6_multipart_object_request<'a>(
        bucket: &'a str,
        key: &'a str,
        upload_id: &UploadId,
        requester: Requester,
    ) -> MultipartObjectRequest<'a> {
        MultipartObjectRequest::new(
            trusted_bucket_name(bucket),
            trusted_object_key(key),
            upload_id.clone(),
            requester,
            None,
        )
    }

    fn phase6_copy_source<'a>(bucket: &'a str, key: &'a str) -> CopySource<'a> {
        CopySource::new(
            trusted_bucket_name(bucket),
            trusted_object_key(key),
            None,
            NO_READ,
            None,
        )
    }
}

mod phase7_model {
    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum Phase7RequesterShape {
        BucketOwnerStandard,
        SameAccountStandard,
        SameAccountAdmin,
        CrossAccount,
    }

    impl fmt::Display for Phase7RequesterShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::BucketOwnerStandard => f.write_str("bucket-owner-standard"),
                Self::SameAccountStandard => f.write_str("same-account-standard"),
                Self::SameAccountAdmin => f.write_str("same-account-admin"),
                Self::CrossAccount => f.write_str("cross-account"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum ObjectLockActionShape {
        GetRetention,
        PutRetention,
        GetLegalHold,
        PutLegalHold,
    }

    impl fmt::Display for ObjectLockActionShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::GetRetention => f.write_str("GetObjectRetention"),
                Self::PutRetention => f.write_str("PutObjectRetention"),
                Self::GetLegalHold => f.write_str("GetObjectLegalHold"),
                Self::PutLegalHold => f.write_str("PutObjectLegalHold"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum ObjectLockPolicyShape {
        None,
        AllowGetRetention,
        AllowPutRetention,
        AllowPutRetentionAndBypass,
        AllowGetLegalHold,
        AllowPutLegalHold,
        DenyBypass,
    }

    impl fmt::Display for ObjectLockPolicyShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::None => f.write_str("no-policy"),
                Self::AllowGetRetention => f.write_str("allow-get-retention"),
                Self::AllowPutRetention => f.write_str("allow-put-retention"),
                Self::AllowPutRetentionAndBypass => f.write_str("allow-put-retention-and-bypass"),
                Self::AllowGetLegalHold => f.write_str("allow-get-legal-hold"),
                Self::AllowPutLegalHold => f.write_str("allow-put-legal-hold"),
                Self::DenyBypass => f.write_str("deny-bypass"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum ObjectLockRecordShape {
        None,
        GovernanceRetention,
    }

    impl fmt::Display for ObjectLockRecordShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::None => f.write_str("none"),
                Self::GovernanceRetention => f.write_str("governance-retention"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum ObjectLockOutcome {
        Allow,
        Deny,
        InvalidRequest,
    }

    impl fmt::Display for ObjectLockOutcome {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Allow => f.write_str("Allow"),
                Self::Deny => f.write_str("Deny"),
                Self::InvalidRequest => f.write_str("InvalidRequest"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct ObjectLockScenario {
        pub(super) name: &'static str,
        pub(super) action: ObjectLockActionShape,
        pub(super) requester: Phase7RequesterShape,
        pub(super) bucket_object_lock_enabled: bool,
        pub(super) existing_lock_state: ObjectLockRecordShape,
        pub(super) bypass_governance: bool,
        pub(super) policy: ObjectLockPolicyShape,
        pub(super) expected: ObjectLockOutcome,
    }

    impl ObjectLockScenario {
        pub(super) fn scenarios() -> Vec<Self> {
            vec![
                Self {
                    name: "same-account-standard-cannot-read-retention",
                    action: ObjectLockActionShape::GetRetention,
                    requester: Phase7RequesterShape::SameAccountStandard,
                    bucket_object_lock_enabled: true,
                    existing_lock_state: ObjectLockRecordShape::GovernanceRetention,
                    bypass_governance: false,
                    policy: ObjectLockPolicyShape::None,
                    expected: ObjectLockOutcome::Deny,
                },
                Self {
                    name: "same-account-admin-can-read-retention",
                    action: ObjectLockActionShape::GetRetention,
                    requester: Phase7RequesterShape::SameAccountAdmin,
                    bucket_object_lock_enabled: true,
                    existing_lock_state: ObjectLockRecordShape::GovernanceRetention,
                    bypass_governance: false,
                    policy: ObjectLockPolicyShape::None,
                    expected: ObjectLockOutcome::Allow,
                },
                Self {
                    name: "cross-account-retention-read-needs-policy",
                    action: ObjectLockActionShape::GetRetention,
                    requester: Phase7RequesterShape::CrossAccount,
                    bucket_object_lock_enabled: true,
                    existing_lock_state: ObjectLockRecordShape::GovernanceRetention,
                    bypass_governance: false,
                    policy: ObjectLockPolicyShape::AllowGetRetention,
                    expected: ObjectLockOutcome::Allow,
                },
                Self {
                    name: "cross-account-legal-hold-read-needs-policy",
                    action: ObjectLockActionShape::GetLegalHold,
                    requester: Phase7RequesterShape::CrossAccount,
                    bucket_object_lock_enabled: true,
                    existing_lock_state: ObjectLockRecordShape::None,
                    bypass_governance: false,
                    policy: ObjectLockPolicyShape::AllowGetLegalHold,
                    expected: ObjectLockOutcome::Allow,
                },
                Self {
                    name: "cross-account-legal-hold-update-needs-policy",
                    action: ObjectLockActionShape::PutLegalHold,
                    requester: Phase7RequesterShape::CrossAccount,
                    bucket_object_lock_enabled: true,
                    existing_lock_state: ObjectLockRecordShape::None,
                    bypass_governance: false,
                    policy: ObjectLockPolicyShape::AllowPutLegalHold,
                    expected: ObjectLockOutcome::Allow,
                },
                Self {
                    name: "cross-account-retention-bypass-needs-explicit-bypass-allow",
                    action: ObjectLockActionShape::PutRetention,
                    requester: Phase7RequesterShape::CrossAccount,
                    bucket_object_lock_enabled: true,
                    existing_lock_state: ObjectLockRecordShape::GovernanceRetention,
                    bypass_governance: true,
                    policy: ObjectLockPolicyShape::AllowPutRetention,
                    expected: ObjectLockOutcome::Deny,
                },
                Self {
                    name: "cross-account-retention-bypass-allowed-with-bypass-policy",
                    action: ObjectLockActionShape::PutRetention,
                    requester: Phase7RequesterShape::CrossAccount,
                    bucket_object_lock_enabled: true,
                    existing_lock_state: ObjectLockRecordShape::GovernanceRetention,
                    bypass_governance: true,
                    policy: ObjectLockPolicyShape::AllowPutRetentionAndBypass,
                    expected: ObjectLockOutcome::Allow,
                },
                Self {
                    name: "same-account-admin-retention-bypass-allowed-implicitly",
                    action: ObjectLockActionShape::PutRetention,
                    requester: Phase7RequesterShape::SameAccountAdmin,
                    bucket_object_lock_enabled: true,
                    existing_lock_state: ObjectLockRecordShape::GovernanceRetention,
                    bypass_governance: true,
                    policy: ObjectLockPolicyShape::None,
                    expected: ObjectLockOutcome::Allow,
                },
                Self {
                    name: "same-account-admin-retention-bypass-blocked-by-explicit-deny",
                    action: ObjectLockActionShape::PutRetention,
                    requester: Phase7RequesterShape::SameAccountAdmin,
                    bucket_object_lock_enabled: true,
                    existing_lock_state: ObjectLockRecordShape::GovernanceRetention,
                    bypass_governance: true,
                    policy: ObjectLockPolicyShape::DenyBypass,
                    expected: ObjectLockOutcome::Deny,
                },
                Self {
                    name: "same-account-admin-plain-bucket-returns-invalid-request",
                    action: ObjectLockActionShape::GetRetention,
                    requester: Phase7RequesterShape::SameAccountAdmin,
                    bucket_object_lock_enabled: false,
                    existing_lock_state: ObjectLockRecordShape::None,
                    bypass_governance: false,
                    policy: ObjectLockPolicyShape::None,
                    expected: ObjectLockOutcome::InvalidRequest,
                },
                Self {
                    name: "cross-account-plain-bucket-does-not-leak-lock-state",
                    action: ObjectLockActionShape::GetRetention,
                    requester: Phase7RequesterShape::CrossAccount,
                    bucket_object_lock_enabled: false,
                    existing_lock_state: ObjectLockRecordShape::None,
                    bypass_governance: false,
                    policy: ObjectLockPolicyShape::None,
                    expected: ObjectLockOutcome::Deny,
                },
            ]
        }
    }

    impl fmt::Display for ObjectLockScenario {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "{} requester={} lock-enabled={} state={} bypass={} policy={}",
                self.action,
                self.requester,
                self.bucket_object_lock_enabled,
                self.existing_lock_state,
                self.bypass_governance,
                self.policy
            )
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum DeleteTargetShape {
        CurrentExisting,
        CurrentMissing,
        SpecificVersionExisting,
        SpecificVersionMissing,
    }

    impl fmt::Display for DeleteTargetShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::CurrentExisting => f.write_str("current-existing"),
                Self::CurrentMissing => f.write_str("current-missing"),
                Self::SpecificVersionExisting => f.write_str("specific-version-existing"),
                Self::SpecificVersionMissing => f.write_str("specific-version-missing"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum DeletePolicyShape {
        None,
        AllowDeleteObject,
        AllowDeleteVersion,
        AllowDeleteVersionAndBypass,
        DenyBypass,
    }

    impl fmt::Display for DeletePolicyShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::None => f.write_str("no-policy"),
                Self::AllowDeleteObject => f.write_str("allow-delete-object"),
                Self::AllowDeleteVersion => f.write_str("allow-delete-version"),
                Self::AllowDeleteVersionAndBypass => f.write_str("allow-delete-version-and-bypass"),
                Self::DenyBypass => f.write_str("deny-bypass"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum DeleteObjectLockShape {
        None,
        Governance,
        Compliance,
        LegalHold,
    }

    impl fmt::Display for DeleteObjectLockShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::None => f.write_str("none"),
                Self::Governance => f.write_str("governance"),
                Self::Compliance => f.write_str("compliance"),
                Self::LegalHold => f.write_str("legal-hold"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum DeleteOutcome {
        Allow,
        Deny,
    }

    impl fmt::Display for DeleteOutcome {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Allow => f.write_str("Allow"),
                Self::Deny => f.write_str("Deny"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct DeleteScenario {
        pub(super) name: &'static str,
        pub(super) requester: Phase7RequesterShape,
        pub(super) target: DeleteTargetShape,
        pub(super) bucket_object_lock_enabled: bool,
        pub(super) object_lock: DeleteObjectLockShape,
        pub(super) bypass_governance: bool,
        pub(super) policy: DeletePolicyShape,
        pub(super) expected: DeleteOutcome,
    }

    impl DeleteScenario {
        pub(super) fn scenarios() -> Vec<Self> {
            vec![
                Self {
                    name: "current-missing-bypass-header-does-not-need-bypass-permission",
                    requester: Phase7RequesterShape::CrossAccount,
                    target: DeleteTargetShape::CurrentMissing,
                    bucket_object_lock_enabled: true,
                    object_lock: DeleteObjectLockShape::None,
                    bypass_governance: true,
                    policy: DeletePolicyShape::AllowDeleteObject,
                    expected: DeleteOutcome::Allow,
                },
                Self {
                    name: "current-existing-governance-delete-marker-insert-ignores-bypass-header",
                    requester: Phase7RequesterShape::BucketOwnerStandard,
                    target: DeleteTargetShape::CurrentExisting,
                    bucket_object_lock_enabled: true,
                    object_lock: DeleteObjectLockShape::Governance,
                    bypass_governance: true,
                    policy: DeletePolicyShape::None,
                    expected: DeleteOutcome::Allow,
                },
                Self {
                    name: "same-account-standard-cannot-version-delete-with-governance-bypass",
                    requester: Phase7RequesterShape::SameAccountStandard,
                    target: DeleteTargetShape::SpecificVersionExisting,
                    bucket_object_lock_enabled: true,
                    object_lock: DeleteObjectLockShape::Governance,
                    bypass_governance: true,
                    policy: DeletePolicyShape::None,
                    expected: DeleteOutcome::Deny,
                },
                Self {
                    name: "same-account-admin-can-version-delete-with-governance-bypass",
                    requester: Phase7RequesterShape::SameAccountAdmin,
                    target: DeleteTargetShape::SpecificVersionExisting,
                    bucket_object_lock_enabled: true,
                    object_lock: DeleteObjectLockShape::Governance,
                    bypass_governance: true,
                    policy: DeletePolicyShape::None,
                    expected: DeleteOutcome::Allow,
                },
                Self {
                    name: "same-account-admin-bypass-blocked-by-explicit-deny",
                    requester: Phase7RequesterShape::SameAccountAdmin,
                    target: DeleteTargetShape::SpecificVersionExisting,
                    bucket_object_lock_enabled: true,
                    object_lock: DeleteObjectLockShape::Governance,
                    bypass_governance: true,
                    policy: DeletePolicyShape::DenyBypass,
                    expected: DeleteOutcome::Deny,
                },
                Self {
                    name: "cross-account-version-delete-with-governance-needs-bypass-allow",
                    requester: Phase7RequesterShape::CrossAccount,
                    target: DeleteTargetShape::SpecificVersionExisting,
                    bucket_object_lock_enabled: true,
                    object_lock: DeleteObjectLockShape::Governance,
                    bypass_governance: true,
                    policy: DeletePolicyShape::AllowDeleteVersion,
                    expected: DeleteOutcome::Deny,
                },
                Self {
                    name: "cross-account-version-delete-with-governance-allowed-with-bypass-policy",
                    requester: Phase7RequesterShape::CrossAccount,
                    target: DeleteTargetShape::SpecificVersionExisting,
                    bucket_object_lock_enabled: true,
                    object_lock: DeleteObjectLockShape::Governance,
                    bypass_governance: true,
                    policy: DeletePolicyShape::AllowDeleteVersionAndBypass,
                    expected: DeleteOutcome::Allow,
                },
                Self {
                    name: "missing-version-delete-without-bypass-uses-delete-version-action",
                    requester: Phase7RequesterShape::CrossAccount,
                    target: DeleteTargetShape::SpecificVersionMissing,
                    bucket_object_lock_enabled: true,
                    object_lock: DeleteObjectLockShape::None,
                    bypass_governance: false,
                    policy: DeletePolicyShape::AllowDeleteVersion,
                    expected: DeleteOutcome::Allow,
                },
                Self {
                    name: "missing-version-delete-with-bypass-needs-bypass-permission",
                    requester: Phase7RequesterShape::CrossAccount,
                    target: DeleteTargetShape::SpecificVersionMissing,
                    bucket_object_lock_enabled: true,
                    object_lock: DeleteObjectLockShape::None,
                    bypass_governance: true,
                    policy: DeletePolicyShape::AllowDeleteVersion,
                    expected: DeleteOutcome::Deny,
                },
                Self {
                    name: "missing-version-delete-with-bypass-allowed-with-bypass-policy",
                    requester: Phase7RequesterShape::CrossAccount,
                    target: DeleteTargetShape::SpecificVersionMissing,
                    bucket_object_lock_enabled: true,
                    object_lock: DeleteObjectLockShape::None,
                    bypass_governance: true,
                    policy: DeletePolicyShape::AllowDeleteVersionAndBypass,
                    expected: DeleteOutcome::Allow,
                },
                Self {
                    name: "missing-version-bypass-on-plain-bucket-does-not-need-bypass-permission",
                    requester: Phase7RequesterShape::CrossAccount,
                    target: DeleteTargetShape::SpecificVersionMissing,
                    bucket_object_lock_enabled: false,
                    object_lock: DeleteObjectLockShape::None,
                    bypass_governance: true,
                    policy: DeletePolicyShape::AllowDeleteVersion,
                    expected: DeleteOutcome::Allow,
                },
                Self {
                    name: "compliance-retention-denies-delete-even-for-admin-bypass",
                    requester: Phase7RequesterShape::SameAccountAdmin,
                    target: DeleteTargetShape::SpecificVersionExisting,
                    bucket_object_lock_enabled: true,
                    object_lock: DeleteObjectLockShape::Compliance,
                    bypass_governance: true,
                    policy: DeletePolicyShape::None,
                    expected: DeleteOutcome::Deny,
                },
                Self {
                    name: "legal-hold-denies-delete-even-for-admin-bypass",
                    requester: Phase7RequesterShape::SameAccountAdmin,
                    target: DeleteTargetShape::SpecificVersionExisting,
                    bucket_object_lock_enabled: true,
                    object_lock: DeleteObjectLockShape::LegalHold,
                    bypass_governance: true,
                    policy: DeletePolicyShape::None,
                    expected: DeleteOutcome::Deny,
                },
            ]
        }
    }

    impl fmt::Display for DeleteScenario {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "Delete target={} requester={} lock={} bypass={} policy={}",
                self.target, self.requester, self.object_lock, self.bypass_governance, self.policy
            )
        }
    }
}

mod phase7_harness {
    use super::harness::{setup_coordinator, IdentityFixtures};
    use super::phase7_model::{
        DeleteObjectLockShape, DeleteOutcome, DeletePolicyShape, DeleteScenario, DeleteTargetShape,
        ObjectLockActionShape, ObjectLockOutcome, ObjectLockPolicyShape, ObjectLockRecordShape,
        ObjectLockScenario, Phase7RequesterShape,
    };
    use super::*;

    const KEY: &str = "phase7-key";
    const NO_PHASE7_DELETE: &DeleteCondition = &DeleteCondition::None;
    const NO_PHASE7_PUT_OBJECT_ACL: PutObjectAcl<'static> = PutObjectAcl::None;

    pub(super) struct Phase7Harness {
        _tmp: test_util::TempDir,
        coord: Coordinator,
        fixtures: IdentityFixtures,
    }

    impl Phase7Harness {
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

        pub(super) fn run_object_lock(
            &self,
            bucket: &str,
            scenario: ObjectLockScenario,
        ) -> ObjectLockOutcome {
            materialize_object_lock_bucket(&self.coord, &self.fixtures, bucket, scenario)
                .unwrap_or_else(|err| {
                    panic!(
                        "failed to materialize phase 7 object-lock state for {scenario}: {err:?}"
                    );
                });
            run_object_lock_action(&self.coord, &self.fixtures, bucket, scenario)
        }

        pub(super) fn run_delete(&self, bucket: &str, scenario: DeleteScenario) -> DeleteOutcome {
            let version_id =
                materialize_delete_bucket(&self.coord, &self.fixtures, bucket, scenario)
                    .unwrap_or_else(|err| {
                        panic!(
                            "failed to materialize phase 7 delete state for {scenario}: {err:?}"
                        );
                    });
            run_delete_action(&self.coord, &self.fixtures, bucket, scenario, version_id)
        }
    }

    pub(super) fn phase7_object_lock_bucket_name_for(index: usize) -> String {
        format!("authz-phase7-lock-{index:05}")
    }

    pub(super) fn phase7_delete_bucket_name_for(index: usize) -> String {
        format!("authz-phase7-delete-{index:05}")
    }

    fn materialize_object_lock_bucket(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: ObjectLockScenario,
    ) -> Result<(), ServerError> {
        create_phase7_bucket(coord, fixtures, bucket, scenario.bucket_object_lock_enabled)?;
        let owner_requester = Requester::authenticated(fixtures.owner_user.clone());
        let put = test_helpers::put_object(
            coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: ObjectRequest::new(
                    trusted_bucket_name(bucket),
                    trusted_object_key(KEY),
                    owner_requester.clone(),
                    None,
                ),
                data: b"phase7",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PHASE7_PUT_OBJECT_ACL.into(),
            },
        )?;

        apply_object_lock_state(
            coord,
            bucket,
            fixtures,
            put.version_id,
            scenario.existing_lock_state,
        )?;
        put_phase7_object_lock_policy(coord, fixtures, bucket, scenario.requester, scenario.policy)
    }

    fn run_object_lock_action(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: ObjectLockScenario,
    ) -> ObjectLockOutcome {
        let requester = phase7_requester(fixtures, scenario.requester);
        let object = ObjectVersionRequest::new(
            trusted_bucket_name(bucket),
            trusted_object_key(KEY),
            None,
            requester,
            None,
        );

        let result = match scenario.action {
            ObjectLockActionShape::GetRetention => {
                coord.authorize_get_object_retention(&object).map(|_| ())
            }
            ObjectLockActionShape::PutRetention => coord
                .authorize_put_object_retention(&PutObjectRetentionRequest {
                    object,
                    retention: ObjectRetention {
                        mode: ObjectLockMode::Governance,
                        retain_until_unix_seconds: Coordinator::current_unix_seconds()
                            .expect("phase 7 current time")
                            + 180,
                    },
                    bypass_governance: scenario.bypass_governance,
                })
                .map(|_| ()),
            ObjectLockActionShape::GetLegalHold => {
                coord.authorize_get_object_legal_hold(&object).map(|_| ())
            }
            ObjectLockActionShape::PutLegalHold => coord
                .authorize_put_object_legal_hold(&PutObjectLegalHoldRequest {
                    object,
                    legal_hold: LegalHoldStatus::On,
                })
                .map(|_| ()),
        };

        match result {
            Ok(()) => ObjectLockOutcome::Allow,
            Err(ServerError::AccessDenied | ServerError::AnonymousApiAccessDenied) => {
                ObjectLockOutcome::Deny
            }
            Err(ServerError::InvalidRequest { .. }) => ObjectLockOutcome::InvalidRequest,
            Err(other) => panic!("unexpected phase 7 object-lock result: {other:?}"),
        }
    }

    fn materialize_delete_bucket(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: DeleteScenario,
    ) -> Result<Option<VersionId>, ServerError> {
        create_phase7_bucket(coord, fixtures, bucket, scenario.bucket_object_lock_enabled)?;
        if matches!(
            scenario.target,
            DeleteTargetShape::SpecificVersionExisting | DeleteTargetShape::SpecificVersionMissing
        ) {
            coord.put_bucket_versioning(&PutBucketVersioningRequest {
                bucket: BucketRequest::new(
                    trusted_bucket_name(bucket),
                    Requester::authenticated(fixtures.owner_user.clone()),
                    None,
                ),
                state: BucketVersioningState::Enabled,
            })?;
        }
        let owner_requester = Requester::authenticated(fixtures.owner_user.clone());
        let version_id = match scenario.target {
            DeleteTargetShape::CurrentMissing => None,
            DeleteTargetShape::CurrentExisting
            | DeleteTargetShape::SpecificVersionExisting
            | DeleteTargetShape::SpecificVersionMissing => {
                let put = test_helpers::put_object(
                    coord,
                    &PutObjectRequest {
                        encryption: WriteEncryptionRequest::none(),
                        policy_context: PutObjectPolicyContext::default(),
                        object_lock: ObjectLockState::default(),
                        object: ObjectRequest::new(
                            trusted_bucket_name(bucket),
                            trusted_object_key(KEY),
                            owner_requester.clone(),
                            None,
                        ),
                        data: b"phase7",
                        metadata: &MetadataBlob::new(),
                        system_metadata: &SystemMetadata::EMPTY,
                        tags: None,
                        cond: NO_WRITE,
                        acl: NO_PHASE7_PUT_OBJECT_ACL.into(),
                    },
                )?;
                Some(put.version_id)
            }
        };

        if let Some(version_id) = version_id {
            apply_delete_lock_state(coord, bucket, fixtures, version_id, scenario.object_lock)?;
            if scenario.target == DeleteTargetShape::SpecificVersionMissing {
                coord.delete_object(&DeleteObjectRequest {
                    object: ObjectVersionRequest::new(
                        trusted_bucket_name(bucket),
                        trusted_object_key(KEY),
                        Some(version_id),
                        owner_requester,
                        None,
                    ),
                    bypass_governance: false,
                    cond: NO_PHASE7_DELETE,
                })?;
            }
        }

        put_phase7_delete_policy(coord, fixtures, bucket, scenario.requester, scenario.policy)?;
        Ok(version_id)
    }

    fn run_delete_action(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: DeleteScenario,
        version_id: Option<VersionId>,
    ) -> DeleteOutcome {
        let requester = phase7_requester(fixtures, scenario.requester);
        let result = coord.authorize_delete_object(&DeleteObjectRequest {
            object: ObjectVersionRequest::new(
                trusted_bucket_name(bucket),
                trusted_object_key(KEY),
                version_id,
                requester,
                None,
            ),
            bypass_governance: scenario.bypass_governance,
            cond: NO_PHASE7_DELETE,
        });

        match result {
            Ok(_) => DeleteOutcome::Allow,
            Err(ServerError::AccessDenied | ServerError::AnonymousApiAccessDenied) => {
                DeleteOutcome::Deny
            }
            Err(other) => panic!("unexpected phase 7 delete result: {other:?}"),
        }
    }

    fn create_phase7_bucket(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        object_lock_enabled: bool,
    ) -> Result<(), ServerError> {
        coord.create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name(bucket),
            requester: Requester::authenticated(fixtures.owner_user.clone()),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled,
        })
    }

    fn apply_object_lock_state(
        coord: &Coordinator,
        bucket: &str,
        fixtures: &IdentityFixtures,
        version_id: VersionId,
        state: ObjectLockRecordShape,
    ) -> Result<(), ServerError> {
        if state != ObjectLockRecordShape::GovernanceRetention {
            return Ok(());
        }

        coord.put_object_retention(&PutObjectRetentionRequest {
            object: ObjectVersionRequest::new(
                trusted_bucket_name(bucket),
                trusted_object_key(KEY),
                Some(version_id),
                Requester::authenticated(fixtures.owner_user.clone()),
                None,
            ),
            retention: ObjectRetention {
                mode: ObjectLockMode::Governance,
                retain_until_unix_seconds: Coordinator::current_unix_seconds()? + 3600,
            },
            bypass_governance: false,
        })
    }

    fn apply_delete_lock_state(
        coord: &Coordinator,
        bucket: &str,
        fixtures: &IdentityFixtures,
        version_id: VersionId,
        state: DeleteObjectLockShape,
    ) -> Result<(), ServerError> {
        match state {
            DeleteObjectLockShape::None => Ok(()),
            DeleteObjectLockShape::Governance | DeleteObjectLockShape::Compliance => coord
                .put_object_retention(&PutObjectRetentionRequest {
                    object: ObjectVersionRequest::new(
                        trusted_bucket_name(bucket),
                        trusted_object_key(KEY),
                        Some(version_id),
                        Requester::authenticated(fixtures.owner_user.clone()),
                        None,
                    ),
                    retention: ObjectRetention {
                        mode: match state {
                            DeleteObjectLockShape::Governance => ObjectLockMode::Governance,
                            DeleteObjectLockShape::Compliance => ObjectLockMode::Compliance,
                            DeleteObjectLockShape::None | DeleteObjectLockShape::LegalHold => {
                                unreachable!()
                            }
                        },
                        retain_until_unix_seconds: Coordinator::current_unix_seconds()? + 3600,
                    },
                    bypass_governance: false,
                }),
            DeleteObjectLockShape::LegalHold => {
                coord.put_object_legal_hold(&PutObjectLegalHoldRequest {
                    object: ObjectVersionRequest::new(
                        trusted_bucket_name(bucket),
                        trusted_object_key(KEY),
                        Some(version_id),
                        Requester::authenticated(fixtures.owner_user.clone()),
                        None,
                    ),
                    legal_hold: LegalHoldStatus::On,
                })
            }
        }
    }

    fn put_phase7_object_lock_policy(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        requester: Phase7RequesterShape,
        policy: ObjectLockPolicyShape,
    ) -> Result<(), ServerError> {
        let Some(document) =
            phase7_object_lock_policy_document(fixtures, bucket, requester, policy)
        else {
            return Ok(());
        };
        coord.put_bucket_policy(&PutBucketPolicyRequest {
            bucket: BucketRequest::new(
                trusted_bucket_name(bucket),
                Requester::authenticated(fixtures.owner_user.clone()),
                None,
            ),
            config: &document,
            confirm_remove_self_bucket_access: false,
        })
    }

    fn put_phase7_delete_policy(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        requester: Phase7RequesterShape,
        policy: DeletePolicyShape,
    ) -> Result<(), ServerError> {
        let Some(document) = phase7_delete_policy_document(fixtures, bucket, requester, policy)
        else {
            return Ok(());
        };
        coord.put_bucket_policy(&PutBucketPolicyRequest {
            bucket: BucketRequest::new(
                trusted_bucket_name(bucket),
                Requester::authenticated(fixtures.owner_user.clone()),
                None,
            ),
            config: &document,
            confirm_remove_self_bucket_access: false,
        })
    }

    fn phase7_object_lock_policy_document(
        fixtures: &IdentityFixtures,
        bucket: &str,
        requester: Phase7RequesterShape,
        policy: ObjectLockPolicyShape,
    ) -> Option<String> {
        let principal = phase7_requester_principal(fixtures, requester)?;
        let resource = format!("arn:aws:s3:::{bucket}/*");
        let statement = match policy {
            ObjectLockPolicyShape::None => return None,
            ObjectLockPolicyShape::AllowGetRetention => format!(
                r#"{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":"s3:GetObjectRetention","Resource":"{resource}"}}"#
            ),
            ObjectLockPolicyShape::AllowPutRetention => format!(
                r#"{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":"s3:PutObjectRetention","Resource":"{resource}"}}"#
            ),
            ObjectLockPolicyShape::AllowPutRetentionAndBypass => format!(
                r#"{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":["s3:PutObjectRetention","s3:BypassGovernanceRetention"],"Resource":"{resource}"}}"#
            ),
            ObjectLockPolicyShape::AllowGetLegalHold => format!(
                r#"{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":"s3:GetObjectLegalHold","Resource":"{resource}"}}"#
            ),
            ObjectLockPolicyShape::AllowPutLegalHold => format!(
                r#"{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":"s3:PutObjectLegalHold","Resource":"{resource}"}}"#
            ),
            ObjectLockPolicyShape::DenyBypass => format!(
                r#"{{"Effect":"Deny","Principal":{{"AWS":"{principal}"}},"Action":"s3:BypassGovernanceRetention","Resource":"{resource}"}}"#
            ),
        };
        Some(format!(
            r#"{{"Version":"2012-10-17","Statement":[{statement}]}}"#
        ))
    }

    fn phase7_delete_policy_document(
        fixtures: &IdentityFixtures,
        bucket: &str,
        requester: Phase7RequesterShape,
        policy: DeletePolicyShape,
    ) -> Option<String> {
        let principal = phase7_requester_principal(fixtures, requester)?;
        let resource = format!("arn:aws:s3:::{bucket}/*");
        let statement = match policy {
            DeletePolicyShape::None => return None,
            DeletePolicyShape::AllowDeleteObject => format!(
                r#"{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":"s3:DeleteObject","Resource":"{resource}"}}"#
            ),
            DeletePolicyShape::AllowDeleteVersion => format!(
                r#"{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":"s3:DeleteObjectVersion","Resource":"{resource}"}}"#
            ),
            DeletePolicyShape::AllowDeleteVersionAndBypass => format!(
                r#"{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":["s3:DeleteObjectVersion","s3:BypassGovernanceRetention"],"Resource":"{resource}"}}"#
            ),
            DeletePolicyShape::DenyBypass => format!(
                r#"{{"Effect":"Deny","Principal":{{"AWS":"{principal}"}},"Action":"s3:BypassGovernanceRetention","Resource":"{resource}"}}"#
            ),
        };
        Some(format!(
            r#"{{"Version":"2012-10-17","Statement":[{statement}]}}"#
        ))
    }

    fn phase7_requester(fixtures: &IdentityFixtures, shape: Phase7RequesterShape) -> Requester {
        match shape {
            Phase7RequesterShape::BucketOwnerStandard => {
                Requester::authenticated(fixtures.owner_user.clone())
            }
            Phase7RequesterShape::SameAccountStandard => {
                Requester::authenticated(fixtures.same_account_distinct.clone())
            }
            Phase7RequesterShape::SameAccountAdmin => {
                Requester::authenticated_owner_account_admin(fixtures.same_account_distinct.clone())
            }
            Phase7RequesterShape::CrossAccount => {
                Requester::authenticated(fixtures.cross_account.clone())
            }
        }
    }

    fn phase7_requester_principal(
        fixtures: &IdentityFixtures,
        shape: Phase7RequesterShape,
    ) -> Option<&str> {
        match shape {
            Phase7RequesterShape::BucketOwnerStandard => Some(fixtures.owner_user.principal()),
            Phase7RequesterShape::SameAccountStandard | Phase7RequesterShape::SameAccountAdmin => {
                Some(fixtures.same_account_distinct.principal())
            }
            Phase7RequesterShape::CrossAccount => Some(fixtures.cross_account.principal()),
        }
    }
}

mod phase7a_model {
    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum MultipartWriteAction {
        BeginStreamPart,
        CompleteMultipartUpload,
    }

    impl fmt::Display for MultipartWriteAction {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::BeginStreamPart => f.write_str("BeginStreamPart"),
                Self::CompleteMultipartUpload => f.write_str("CompleteMultipartUpload"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum MultipartManagementAction {
        AbortInProgress,
        AbortCompleted,
        ListParts,
    }

    impl fmt::Display for MultipartManagementAction {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::AbortInProgress => f.write_str("AbortMultipartUpload(in-progress)"),
                Self::AbortCompleted => f.write_str("AbortMultipartUpload(completed)"),
                Self::ListParts => f.write_str("ListParts"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum MultipartUploadShape {
        CrossAccountObjectWriter,
        CrossAccountBucketOwnerEnforced,
    }

    impl fmt::Display for MultipartUploadShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::CrossAccountObjectWriter => f.write_str("cross-account-object-writer"),
                Self::CrossAccountBucketOwnerEnforced => {
                    f.write_str("cross-account-bucket-owner-enforced")
                }
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum Phase7aRequesterShape {
        Initiator,
        UploadOwnerExact,
        BucketOwnerExact,
        SameAccountStandard,
        SameAccountAdmin,
        CrossAccountOther,
    }

    impl fmt::Display for Phase7aRequesterShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Initiator => f.write_str("initiator"),
                Self::UploadOwnerExact => f.write_str("upload-owner-exact"),
                Self::BucketOwnerExact => f.write_str("bucket-owner-exact"),
                Self::SameAccountStandard => f.write_str("same-account-standard"),
                Self::SameAccountAdmin => f.write_str("same-account-admin"),
                Self::CrossAccountOther => f.write_str("cross-account-other"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum MultipartPolicyShape {
        None,
        AllowRequesterPutObject,
    }

    impl fmt::Display for MultipartPolicyShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::None => f.write_str("no-policy"),
                Self::AllowRequesterPutObject => f.write_str("allow-requester-put-object"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum MultipartOutcome {
        Allow,
        Deny,
        NoSuchUpload,
    }

    impl fmt::Display for MultipartOutcome {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Allow => f.write_str("Allow"),
                Self::Deny => f.write_str("Deny"),
                Self::NoSuchUpload => f.write_str("NoSuchUpload"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct MultipartWriteScenario {
        pub(super) name: &'static str,
        pub(super) action: MultipartWriteAction,
        pub(super) upload: MultipartUploadShape,
        pub(super) requester: Phase7aRequesterShape,
        pub(super) policy: MultipartPolicyShape,
        pub(super) target_completed_upload: bool,
        pub(super) expected: MultipartOutcome,
    }

    impl MultipartWriteScenario {
        pub(super) fn scenarios() -> Vec<Self> {
            let mut scenarios = Vec::new();
            for action in [
                MultipartWriteAction::BeginStreamPart,
                MultipartWriteAction::CompleteMultipartUpload,
            ] {
                scenarios.extend([
                    Self {
                        name: "initiator-owner-needs-putobject-policy-on-private-bucket",
                        action,
                        upload: MultipartUploadShape::CrossAccountObjectWriter,
                        requester: Phase7aRequesterShape::Initiator,
                        policy: MultipartPolicyShape::None,
                        target_completed_upload: false,
                        expected: MultipartOutcome::Deny,
                    },
                    Self {
                        name: "initiator-owner-allowed-with-putobject-policy",
                        action,
                        upload: MultipartUploadShape::CrossAccountObjectWriter,
                        requester: Phase7aRequesterShape::Initiator,
                        policy: MultipartPolicyShape::AllowRequesterPutObject,
                        target_completed_upload: false,
                        expected: MultipartOutcome::Allow,
                    },
                    Self {
                        name: "bucket-owner-exact-can-continue-object-writer-upload-without-policy",
                        action,
                        upload: MultipartUploadShape::CrossAccountObjectWriter,
                        requester: Phase7aRequesterShape::BucketOwnerExact,
                        policy: MultipartPolicyShape::None,
                        target_completed_upload: false,
                        expected: MultipartOutcome::Allow,
                    },
                    Self {
                        name: "same-account-standard-can-continue-with-explicit-putobject-policy",
                        action,
                        upload: MultipartUploadShape::CrossAccountObjectWriter,
                        requester: Phase7aRequesterShape::SameAccountStandard,
                        policy: MultipartPolicyShape::AllowRequesterPutObject,
                        target_completed_upload: false,
                        expected: MultipartOutcome::Allow,
                    },
                    Self {
                        name: "bucket-owner-owned-upload-allows-owner-exact-principal",
                        action,
                        upload: MultipartUploadShape::CrossAccountBucketOwnerEnforced,
                        requester: Phase7aRequesterShape::UploadOwnerExact,
                        policy: MultipartPolicyShape::None,
                        target_completed_upload: false,
                        expected: MultipartOutcome::Allow,
                    },
                    Self {
                        name: "same-account-standard-cannot-continue-others-upload",
                        action,
                        upload: MultipartUploadShape::CrossAccountBucketOwnerEnforced,
                        requester: Phase7aRequesterShape::SameAccountStandard,
                        policy: MultipartPolicyShape::None,
                        target_completed_upload: false,
                        expected: MultipartOutcome::Deny,
                    },
                    Self {
                        name: "same-account-admin-can-continue-bucket-owner-account-upload",
                        action,
                        upload: MultipartUploadShape::CrossAccountBucketOwnerEnforced,
                        requester: Phase7aRequesterShape::SameAccountAdmin,
                        policy: MultipartPolicyShape::None,
                        target_completed_upload: false,
                        expected: MultipartOutcome::Allow,
                    },
                    Self {
                        name: "write-paths-see-completed-upload-as-no-such-upload",
                        action,
                        upload: MultipartUploadShape::CrossAccountObjectWriter,
                        requester: Phase7aRequesterShape::Initiator,
                        policy: MultipartPolicyShape::AllowRequesterPutObject,
                        target_completed_upload: true,
                        expected: MultipartOutcome::NoSuchUpload,
                    },
                ]);
            }
            scenarios
        }
    }

    impl fmt::Display for MultipartWriteScenario {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "name={} action={} upload={} requester={} policy={} completed={}",
                self.name,
                self.action,
                self.upload,
                self.requester,
                self.policy,
                self.target_completed_upload
            )
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct MultipartManagementScenario {
        pub(super) name: &'static str,
        pub(super) action: MultipartManagementAction,
        pub(super) upload: MultipartUploadShape,
        pub(super) requester: Phase7aRequesterShape,
        pub(super) policy: MultipartPolicyShape,
        pub(super) expected: MultipartOutcome,
    }

    impl MultipartManagementScenario {
        pub(super) fn scenarios() -> Vec<Self> {
            let mut scenarios = Vec::new();
            for action in [
                MultipartManagementAction::AbortInProgress,
                MultipartManagementAction::ListParts,
            ] {
                scenarios.extend([
                    Self {
                        name: "initiator-can-manage-own-upload-without-policy",
                        action,
                        upload: MultipartUploadShape::CrossAccountObjectWriter,
                        requester: Phase7aRequesterShape::Initiator,
                        policy: MultipartPolicyShape::None,
                        expected: MultipartOutcome::Allow,
                    },
                    Self {
                        name: "bucket-owner-exact-can-manage-object-writer-upload",
                        action,
                        upload: MultipartUploadShape::CrossAccountObjectWriter,
                        requester: Phase7aRequesterShape::BucketOwnerExact,
                        policy: MultipartPolicyShape::None,
                        expected: MultipartOutcome::Allow,
                    },
                    Self {
                        name: "same-account-standard-cannot-manage-object-writer-upload",
                        action,
                        upload: MultipartUploadShape::CrossAccountObjectWriter,
                        requester: Phase7aRequesterShape::SameAccountStandard,
                        policy: MultipartPolicyShape::None,
                        expected: MultipartOutcome::Deny,
                    },
                    Self {
                        name: "same-account-admin-can-manage-bucket-owner-account-upload",
                        action,
                        upload: MultipartUploadShape::CrossAccountObjectWriter,
                        requester: Phase7aRequesterShape::SameAccountAdmin,
                        policy: MultipartPolicyShape::None,
                        expected: MultipartOutcome::Allow,
                    },
                    Self {
                        name: "putobject-policy-does-not-help-management-only-paths",
                        action,
                        upload: MultipartUploadShape::CrossAccountObjectWriter,
                        requester: Phase7aRequesterShape::CrossAccountOther,
                        policy: MultipartPolicyShape::AllowRequesterPutObject,
                        expected: MultipartOutcome::Deny,
                    },
                    Self {
                        name: "bucket-owner-exact-can-manage-bucket-owned-upload",
                        action,
                        upload: MultipartUploadShape::CrossAccountBucketOwnerEnforced,
                        requester: Phase7aRequesterShape::BucketOwnerExact,
                        policy: MultipartPolicyShape::None,
                        expected: MultipartOutcome::Allow,
                    },
                ]);
            }
            scenarios.extend([
                Self {
                    name: "completed-abort-allows-initiator",
                    action: MultipartManagementAction::AbortCompleted,
                    upload: MultipartUploadShape::CrossAccountObjectWriter,
                    requester: Phase7aRequesterShape::Initiator,
                    policy: MultipartPolicyShape::None,
                    expected: MultipartOutcome::Allow,
                },
                Self {
                    name: "completed-abort-allows-bucket-owner-exact-on-object-writer-upload",
                    action: MultipartManagementAction::AbortCompleted,
                    upload: MultipartUploadShape::CrossAccountObjectWriter,
                    requester: Phase7aRequesterShape::BucketOwnerExact,
                    policy: MultipartPolicyShape::None,
                    expected: MultipartOutcome::Allow,
                },
                Self {
                    name: "completed-abort-allows-same-account-admin",
                    action: MultipartManagementAction::AbortCompleted,
                    upload: MultipartUploadShape::CrossAccountObjectWriter,
                    requester: Phase7aRequesterShape::SameAccountAdmin,
                    policy: MultipartPolicyShape::None,
                    expected: MultipartOutcome::Allow,
                },
                Self {
                    name: "completed-abort-allows-bucket-owner-exact-when-upload-owner-is-bucket-owner",
                    action: MultipartManagementAction::AbortCompleted,
                    upload: MultipartUploadShape::CrossAccountBucketOwnerEnforced,
                    requester: Phase7aRequesterShape::BucketOwnerExact,
                    policy: MultipartPolicyShape::None,
                    expected: MultipartOutcome::Allow,
                },
                Self {
                    name: "completed-abort-still-ignores-putobject-policy",
                    action: MultipartManagementAction::AbortCompleted,
                    upload: MultipartUploadShape::CrossAccountObjectWriter,
                    requester: Phase7aRequesterShape::CrossAccountOther,
                    policy: MultipartPolicyShape::AllowRequesterPutObject,
                    expected: MultipartOutcome::Deny,
                },
            ]);
            scenarios
        }
    }

    impl fmt::Display for MultipartManagementScenario {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "name={} action={} upload={} requester={} policy={}",
                self.name, self.action, self.upload, self.requester, self.policy
            )
        }
    }
}

mod phase7a_harness {
    use super::harness::{setup_coordinator, IdentityFixtures};
    use super::phase7a_model::{
        MultipartManagementAction, MultipartManagementScenario, MultipartOutcome,
        MultipartPolicyShape, MultipartUploadShape, MultipartWriteAction, MultipartWriteScenario,
        Phase7aRequesterShape,
    };
    use super::*;

    const PHASE7A_KEY: &str = "phase7a-key";

    pub(super) struct Phase7aHarness {
        _tmp: test_util::TempDir,
        coord: Coordinator,
        fixtures: IdentityFixtures,
        other_cross_account: AccountIdentity,
    }

    #[derive(Debug, Clone, Copy)]
    struct Phase7aUploadSpec {
        upload: MultipartUploadShape,
        policy_requester: Phase7aRequesterShape,
        policy: MultipartPolicyShape,
        complete_upload: bool,
    }

    impl Phase7aHarness {
        pub(super) fn new() -> Self {
            let tmp = test_util::tempdir();
            let coord = setup_coordinator(tmp.path());
            let fixtures = IdentityFixtures::new();
            let other_cross_account =
                AccountIdentity::from_principal("arn:aws:iam::777788889999:user/other");
            Self {
                _tmp: tmp,
                coord,
                fixtures,
                other_cross_account,
            }
        }

        pub(super) fn run_write(
            &self,
            bucket: &str,
            scenario: MultipartWriteScenario,
        ) -> MultipartOutcome {
            let upload_id = materialize_phase7a_upload(
                &self.coord,
                &self.fixtures,
                &self.other_cross_account,
                bucket,
                Phase7aUploadSpec {
                    upload: scenario.upload,
                    policy_requester: scenario.requester,
                    policy: scenario.policy,
                    complete_upload: scenario.target_completed_upload,
                },
            )
            .unwrap_or_else(|err| {
                panic!("failed to materialize phase 7A write state for {scenario}: {err:?}");
            });
            run_phase7a_write_action(
                &self.coord,
                &self.fixtures,
                &self.other_cross_account,
                bucket,
                &upload_id,
                scenario,
            )
        }

        pub(super) fn run_management(
            &self,
            bucket: &str,
            scenario: MultipartManagementScenario,
        ) -> MultipartOutcome {
            let completed = scenario.action == MultipartManagementAction::AbortCompleted;
            let upload_id = materialize_phase7a_upload(
                &self.coord,
                &self.fixtures,
                &self.other_cross_account,
                bucket,
                Phase7aUploadSpec {
                    upload: scenario.upload,
                    policy_requester: scenario.requester,
                    policy: scenario.policy,
                    complete_upload: completed,
                },
            )
            .unwrap_or_else(|err| {
                panic!("failed to materialize phase 7A management state for {scenario}: {err:?}");
            });
            run_phase7a_management_action(
                &self.coord,
                &self.fixtures,
                &self.other_cross_account,
                bucket,
                &upload_id,
                scenario,
            )
        }
    }

    pub(super) fn phase7a_write_bucket_name_for(index: usize) -> String {
        format!("authz-phase7a-write-{index:05}")
    }

    pub(super) fn phase7a_management_bucket_name_for(index: usize) -> String {
        format!("authz-phase7a-manage-{index:05}")
    }

    fn materialize_phase7a_upload(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        other_cross_account: &AccountIdentity,
        bucket: &str,
        spec: Phase7aUploadSpec,
    ) -> Result<UploadId, ServerError> {
        create_phase7a_bucket(coord, fixtures, bucket, spec.upload)?;
        put_phase7a_policy(
            coord,
            fixtures,
            bucket,
            fixtures.cross_account.principal(),
            MultipartPolicyShape::AllowRequesterPutObject,
        )?;

        let initiator = Requester::authenticated(fixtures.cross_account.clone());
        let created = coord.create_multipart_upload(&CreateMultipartUploadRequest {
            object: phase7a_object_request(bucket, PHASE7A_KEY, initiator.clone()),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: PutObjectAcl::None.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })?;

        if spec.complete_upload {
            let uploaded = test_helpers::upload_part(
                coord,
                &test_helpers::UploadPartRequest {
                    upload: phase7a_multipart_object_request(
                        bucket,
                        PHASE7A_KEY,
                        &created.upload_id,
                        initiator.clone(),
                    ),
                    part_number: 1,
                    data: b"phase7a",
                    claimed_checksum: None,
                    sse_customer: None,
                },
            )?;
            coord.complete_multipart_upload(&CompleteMultipartUploadRequest {
                upload: phase7a_multipart_object_request(
                    bucket,
                    PHASE7A_KEY,
                    &created.upload_id,
                    initiator,
                ),
                parts: &[CompletePart {
                    part_number: 1,
                    etag: uploaded.etag,
                    checksum: None,
                }],
                claimed_checksum: None,
                expected_object_size: None,
                cond: NO_WRITE,
                sse_customer: None,
            })?;
        }

        match spec.policy {
            MultipartPolicyShape::None => coord.delete_bucket_policy(&BucketRequest::new(
                trusted_bucket_name(bucket),
                Requester::authenticated(fixtures.owner_user.clone()),
                None,
            ))?,
            MultipartPolicyShape::AllowRequesterPutObject => {
                let principal = phase7a_requester_principal(
                    fixtures,
                    other_cross_account,
                    spec.upload,
                    spec.policy_requester,
                );
                put_phase7a_policy(coord, fixtures, bucket, principal, spec.policy)?;
            }
        }

        Ok(created.upload_id)
    }

    fn run_phase7a_write_action(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        other_cross_account: &AccountIdentity,
        bucket: &str,
        upload_id: &UploadId,
        scenario: MultipartWriteScenario,
    ) -> MultipartOutcome {
        let requester = phase7a_requester(
            fixtures,
            other_cross_account,
            scenario.upload,
            scenario.requester,
        );
        let result = match scenario.action {
            MultipartWriteAction::BeginStreamPart => coord
                .authorize_begin_stream_part(&BeginStreamPartRequest {
                    upload: phase7a_multipart_object_request(
                        bucket,
                        PHASE7A_KEY,
                        upload_id,
                        requester,
                    ),
                    part_number: 1,
                    policy_context: PutObjectPolicyContext::default(),
                    sse_customer: None,
                })
                .map(|_| ()),
            MultipartWriteAction::CompleteMultipartUpload => coord
                .authorize_complete_multipart_upload(&CompleteMultipartUploadRequest {
                    upload: phase7a_multipart_object_request(
                        bucket,
                        PHASE7A_KEY,
                        upload_id,
                        requester,
                    ),
                    parts: &[],
                    claimed_checksum: None,
                    expected_object_size: None,
                    cond: NO_WRITE,
                    sse_customer: None,
                })
                .map(|_| ()),
        };
        classify_phase7a_result(result)
    }

    fn run_phase7a_management_action(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        other_cross_account: &AccountIdentity,
        bucket: &str,
        upload_id: &UploadId,
        scenario: MultipartManagementScenario,
    ) -> MultipartOutcome {
        let requester = phase7a_requester(
            fixtures,
            other_cross_account,
            scenario.upload,
            scenario.requester,
        );
        let result = match scenario.action {
            MultipartManagementAction::AbortInProgress
            | MultipartManagementAction::AbortCompleted => coord
                .authorize_abort_multipart_upload(&phase7a_multipart_object_request(
                    bucket,
                    PHASE7A_KEY,
                    upload_id,
                    requester,
                ))
                .map(|_| ()),
            MultipartManagementAction::ListParts => coord
                .authorize_list_parts(&ListPartsRequest {
                    upload: phase7a_multipart_object_request(
                        bucket,
                        PHASE7A_KEY,
                        upload_id,
                        requester,
                    ),
                    part_number_marker: None,
                    max_parts: 100,
                })
                .map(|_| ()),
        };
        classify_phase7a_result(result)
    }

    fn classify_phase7a_result(result: Result<(), ServerError>) -> MultipartOutcome {
        match result {
            Ok(()) => MultipartOutcome::Allow,
            Err(ServerError::AccessDenied | ServerError::AnonymousApiAccessDenied) => {
                MultipartOutcome::Deny
            }
            Err(ServerError::NoSuchUpload { .. }) => MultipartOutcome::NoSuchUpload,
            Err(other) => panic!("unexpected phase 7A result: {other:?}"),
        }
    }

    fn create_phase7a_bucket(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        upload: MultipartUploadShape,
    ) -> Result<(), ServerError> {
        let owner = OwnerIdentity::new(
            fixtures.owner_user.principal(),
            fixtures.owner_user.canonical_user_id().clone(),
        );
        let grants = Coordinator::bucket_acl_grants_from_flags(&owner, false, false);
        coord.create_bucket_with_acl_grants(&owner, bucket, grants, false)?;
        if upload == MultipartUploadShape::CrossAccountBucketOwnerEnforced {
            coord.put_bucket_ownership_controls(&PutBucketOwnershipControlsRequest {
                bucket: BucketRequest::new(
                    trusted_bucket_name(bucket),
                    Requester::authenticated(fixtures.owner_user.clone()),
                    None,
                ),
                config: BucketOwnershipControls {
                    object_ownership: BucketObjectOwnership::BucketOwnerEnforced,
                },
            })?;
        }
        Ok(())
    }

    fn put_phase7a_policy(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        principal: &str,
        policy: MultipartPolicyShape,
    ) -> Result<(), ServerError> {
        if policy == MultipartPolicyShape::None {
            return Ok(());
        }
        let document = format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":"s3:PutObject","Resource":"arn:aws:s3:::{bucket}/{PHASE7A_KEY}"}}]}}"#
        );
        coord.put_bucket_policy(&PutBucketPolicyRequest {
            bucket: BucketRequest::new(
                trusted_bucket_name(bucket),
                Requester::authenticated(fixtures.owner_user.clone()),
                None,
            ),
            config: &document,
            confirm_remove_self_bucket_access: false,
        })
    }

    fn phase7a_requester(
        fixtures: &IdentityFixtures,
        other_cross_account: &AccountIdentity,
        upload: MultipartUploadShape,
        requester: Phase7aRequesterShape,
    ) -> Requester {
        match requester {
            Phase7aRequesterShape::Initiator => {
                Requester::authenticated(fixtures.cross_account.clone())
            }
            Phase7aRequesterShape::UploadOwnerExact => match upload {
                MultipartUploadShape::CrossAccountObjectWriter => {
                    Requester::authenticated(fixtures.cross_account.clone())
                }
                MultipartUploadShape::CrossAccountBucketOwnerEnforced => {
                    Requester::authenticated(fixtures.owner_user.clone())
                }
            },
            Phase7aRequesterShape::BucketOwnerExact => {
                Requester::authenticated(fixtures.owner_user.clone())
            }
            Phase7aRequesterShape::SameAccountStandard => {
                Requester::authenticated(fixtures.same_account_distinct.clone())
            }
            Phase7aRequesterShape::SameAccountAdmin => {
                Requester::authenticated_owner_account_admin(fixtures.same_account_distinct.clone())
            }
            Phase7aRequesterShape::CrossAccountOther => {
                Requester::authenticated(other_cross_account.clone())
            }
        }
    }

    fn phase7a_requester_principal<'a>(
        fixtures: &'a IdentityFixtures,
        other_cross_account: &'a AccountIdentity,
        upload: MultipartUploadShape,
        requester: Phase7aRequesterShape,
    ) -> &'a str {
        match requester {
            Phase7aRequesterShape::Initiator => fixtures.cross_account.principal(),
            Phase7aRequesterShape::UploadOwnerExact => match upload {
                MultipartUploadShape::CrossAccountObjectWriter => {
                    fixtures.cross_account.principal()
                }
                MultipartUploadShape::CrossAccountBucketOwnerEnforced => {
                    fixtures.owner_user.principal()
                }
            },
            Phase7aRequesterShape::BucketOwnerExact => fixtures.owner_user.principal(),
            Phase7aRequesterShape::SameAccountStandard
            | Phase7aRequesterShape::SameAccountAdmin => fixtures.same_account_distinct.principal(),
            Phase7aRequesterShape::CrossAccountOther => other_cross_account.principal(),
        }
    }

    fn phase7a_object_request<'a>(
        bucket: &'a str,
        key: &'a str,
        requester: Requester,
    ) -> ObjectRequest<'a> {
        ObjectRequest::new(
            trusted_bucket_name(bucket),
            trusted_object_key(key),
            requester,
            None,
        )
    }

    fn phase7a_multipart_object_request<'a>(
        bucket: &'a str,
        key: &'a str,
        upload_id: &UploadId,
        requester: Requester,
    ) -> MultipartObjectRequest<'a> {
        MultipartObjectRequest::new(
            trusted_bucket_name(bucket),
            trusted_object_key(key),
            upload_id.clone(),
            requester,
            None,
        )
    }
}

mod phase8_harness {
    use super::harness::{setup_coordinator, IdentityFixtures};
    use super::*;

    pub(super) const PHASE8_SRC_KEY: &str = "src";
    pub(super) const PHASE8_DST_KEY: &str = "dst";
    pub(super) const PHASE8_TAGS_XML: &str =
        "<Tagging><TagSet><Tag><Key>env</Key><Value>phase8</Value></Tag></TagSet></Tagging>";

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(super) enum GrantHeaderKind {
        FullControl,
        ReadAcp,
        Write,
        WriteAcp,
    }

    impl GrantHeaderKind {
        pub(super) const ALL: [Self; 4] = [
            Self::FullControl,
            Self::ReadAcp,
            Self::Write,
            Self::WriteAcp,
        ];

        pub(super) const fn policy_key(self) -> &'static str {
            match self {
                Self::FullControl => "s3:x-amz-grant-full-control",
                Self::ReadAcp => "s3:x-amz-grant-read-acp",
                Self::Write => "s3:x-amz-grant-write",
                Self::WriteAcp => "s3:x-amz-grant-write-acp",
            }
        }

        pub(super) const fn permission(self) -> AclPermission {
            match self {
                Self::FullControl => AclPermission::FullControl,
                Self::ReadAcp => AclPermission::ReadAcp,
                Self::Write => AclPermission::Write,
                Self::WriteAcp => AclPermission::WriteAcp,
            }
        }

        pub(super) const fn alternate(self) -> Self {
            match self {
                Self::FullControl => Self::ReadAcp,
                Self::ReadAcp => Self::Write,
                Self::Write => Self::WriteAcp,
                Self::WriteAcp => Self::FullControl,
            }
        }

        pub(super) const fn with_policy_context(
            self,
            mut policy_context: PutObjectPolicyContext<'static>,
            value: Option<&'static str>,
        ) -> PutObjectPolicyContext<'static> {
            match self {
                Self::FullControl => {
                    policy_context.grant_full_control = value;
                }
                Self::ReadAcp => {
                    policy_context.grant_read_acp = value;
                }
                Self::Write => {
                    policy_context.grant_write = value;
                }
                Self::WriteAcp => {
                    policy_context.grant_write_acp = value;
                }
            }
            policy_context
        }
    }

    pub(super) struct Phase8Harness {
        _tmp: test_util::TempDir,
        pub(super) coord: Coordinator,
        pub(super) fixtures: IdentityFixtures,
    }

    impl Phase8Harness {
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
    }

    pub(super) fn create_bucket(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
    ) -> Result<(), ServerError> {
        let owner = OwnerIdentity::new(
            fixtures.owner_user.principal(),
            fixtures.owner_user.canonical_user_id().clone(),
        );
        let grants = Coordinator::bucket_acl_grants_from_flags(&owner, false, false);
        coord
            .create_bucket_with_acl_grants(&owner, bucket, grants, false)
            .map(|_| ())
    }

    pub(super) fn put_private_acl_policy(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        action: &str,
    ) -> Result<(), ServerError> {
        let policy = format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"{}"}},"Action":"{action}","Resource":"arn:aws:s3:::{bucket}/*","Condition":{{"StringEquals":{{"s3:x-amz-acl":"private"}}}}}}]}}"#,
            fixtures.cross_account.principal()
        );
        coord.put_bucket_policy(&PutBucketPolicyRequest {
            bucket: BucketRequest::new(
                trusted_bucket_name(bucket),
                Requester::authenticated(fixtures.owner_user.clone()),
                None,
            ),
            config: &policy,
            confirm_remove_self_bucket_access: false,
        })
    }

    pub(super) fn put_grant_header_policy(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        action: &str,
        header_kind: GrantHeaderKind,
        header_value: &str,
    ) -> Result<(), ServerError> {
        let escaped_header_value = header_value.replace('\\', "\\\\").replace('"', "\\\"");
        let policy = format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"{}"}},"Action":"{action}","Resource":"arn:aws:s3:::{bucket}/*","Condition":{{"StringEquals":{{"{}":"{}"}}}}}}]}}"#,
            fixtures.cross_account.principal(),
            header_kind.policy_key(),
            escaped_header_value
        );
        coord.put_bucket_policy(&PutBucketPolicyRequest {
            bucket: BucketRequest::new(
                trusted_bucket_name(bucket),
                Requester::authenticated(fixtures.owner_user.clone()),
                None,
            ),
            config: &policy,
            confirm_remove_self_bucket_access: false,
        })
    }

    pub(super) fn put_copy_tagging_policy(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
    ) -> Result<(), ServerError> {
        let policy = format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"{}"}},"Action":"s3:GetObject","Resource":"arn:aws:s3:::{bucket}/{PHASE8_SRC_KEY}"}},{{"Effect":"Allow","Principal":{{"AWS":"{}"}},"Action":["s3:PutObject","s3:PutObjectTagging"],"Resource":"arn:aws:s3:::{bucket}/{PHASE8_DST_KEY}","Condition":{{"StringEquals":{{"s3:RequestObjectTag/env":"phase8"}}}}}}]}}"#,
            fixtures.cross_account.principal(),
            fixtures.cross_account.principal()
        );
        coord.put_bucket_policy(&PutBucketPolicyRequest {
            bucket: BucketRequest::new(
                trusted_bucket_name(bucket),
                Requester::authenticated(fixtures.owner_user.clone()),
                None,
            ),
            config: &policy,
            confirm_remove_self_bucket_access: false,
        })
    }

    pub(super) fn put_copy_private_acl_policy(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
    ) -> Result<(), ServerError> {
        let policy = format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"{}"}},"Action":"s3:GetObject","Resource":"arn:aws:s3:::{bucket}/{PHASE8_SRC_KEY}"}},{{"Effect":"Allow","Principal":{{"AWS":"{}"}},"Action":"s3:PutObject","Resource":"arn:aws:s3:::{bucket}/{PHASE8_DST_KEY}","Condition":{{"StringEquals":{{"s3:x-amz-acl":"private"}}}}}}]}}"#,
            fixtures.cross_account.principal(),
            fixtures.cross_account.principal()
        );
        coord.put_bucket_policy(&PutBucketPolicyRequest {
            bucket: BucketRequest::new(
                trusted_bucket_name(bucket),
                Requester::authenticated(fixtures.owner_user.clone()),
                None,
            ),
            config: &policy,
            confirm_remove_self_bucket_access: false,
        })
    }

    pub(super) fn cross_account_requester(fixtures: &IdentityFixtures) -> Requester {
        Requester::authenticated(fixtures.cross_account.clone())
    }

    pub(super) fn cross_account_grant_header_value(fixtures: &IdentityFixtures) -> &'static str {
        Box::leak(
            format!(r#"id="{}""#, fixtures.cross_account.canonical_user_id()).into_boxed_str(),
        )
    }

    pub(super) fn put_object_private(
        coord: &Coordinator,
        bucket: &str,
        key: &str,
        requester: Requester,
        policy_context: PutObjectPolicyContext<'static>,
    ) -> Result<(), ServerError> {
        test_helpers::put_object(
            coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context,
                object_lock: ObjectLockState::default(),
                object: ObjectRequest::new(
                    trusted_bucket_name(bucket),
                    trusted_object_key(key),
                    requester,
                    None,
                ),
                data: b"phase-8",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,
                acl: PutObjectAcl::Private.into(),
            },
        )
        .map(|_| ())
    }

    pub(super) fn create_multipart_upload_private(
        coord: &Coordinator,
        bucket: &str,
        key: &str,
        requester: Requester,
        policy_context: PutObjectPolicyContext<'static>,
    ) -> Result<(), ServerError> {
        let created = coord.create_multipart_upload(&CreateMultipartUploadRequest {
            object: ObjectRequest::new(
                trusted_bucket_name(bucket),
                trusted_object_key(key),
                requester.clone(),
                None,
            ),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: PutObjectAcl::Private.into(),
            policy_context,
            object_lock: ObjectLockState::default(),
            encryption: WriteEncryptionRequest::none(),
        })?;
        coord.abort_multipart_upload(&MultipartObjectRequest::new(
            trusted_bucket_name(bucket),
            trusted_object_key(key),
            created.upload_id,
            requester,
            None,
        ))
    }

    pub(super) fn put_object_grant(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        key: &str,
        requester: Requester,
        grant: GrantHeaderKind,
        policy_context: PutObjectPolicyContext<'static>,
    ) -> Result<(), ServerError> {
        test_helpers::put_object(
            coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context,
                object_lock: ObjectLockState::default(),
                object: ObjectRequest::new(
                    trusted_bucket_name(bucket),
                    trusted_object_key(key),
                    requester,
                    None,
                ),
                data: b"phase-8-grant",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,
                acl: PutObjectWriteAcl::Grants(AclGrants::new(vec![AclGrant::new(
                    AclGrantee::CanonicalUser(fixtures.cross_account.canonical_user_id().clone()),
                    grant.permission(),
                )])),
            },
        )
        .map(|_| ())
    }

    pub(super) fn put_acl_private(
        coord: &Coordinator,
        bucket: &str,
        key: &str,
        version_id: Option<VersionId>,
        requester: Requester,
        policy_context: PutObjectPolicyContext<'static>,
    ) -> Result<(), ServerError> {
        coord
            .put_object_acl(&PutObjectAclRequest {
                object: ObjectVersionRequest::new(
                    trusted_bucket_name(bucket),
                    trusted_object_key(key),
                    version_id,
                    requester,
                    None,
                ),
                acl: PutObjectAclInput::Canned(PutObjectAcl::Private),
                policy_context,
            })
            .map(|_| ())
    }

    pub(super) fn copy_object_private(
        coord: &Coordinator,
        bucket: &str,
        requester: Requester,
        policy_context: PutObjectPolicyContext<'static>,
    ) -> Result<(), ServerError> {
        coord
            .copy_object(&CopyObjectRequest {
                source: CopySource::new(
                    trusted_bucket_name(bucket),
                    trusted_object_key(PHASE8_SRC_KEY),
                    None,
                    NO_READ,
                    None,
                ),
                destination: ObjectRequest::new(
                    trusted_bucket_name(bucket),
                    trusted_object_key(PHASE8_DST_KEY),
                    requester,
                    None,
                ),
                dst_condition: NO_WRITE,
                directive: MetadataDirective::Copy,
                tagging: TaggingDirective::Copy,
                acl: PutObjectAcl::Private.into(),
                policy_context,
                source_sse_customer: None,
                destination_encryption: WriteEncryptionRequest::none(),
                object_lock: ObjectLockState::default(),
            })
            .map(|_| ())
    }

    pub(super) fn copy_object_replace_tags(
        coord: &Coordinator,
        bucket: &str,
        requester: Requester,
        replace_tags: bool,
    ) -> Result<(), ServerError> {
        coord
            .copy_object(&CopyObjectRequest {
                source: CopySource::new(
                    trusted_bucket_name(bucket),
                    trusted_object_key(PHASE8_SRC_KEY),
                    None,
                    NO_READ,
                    None,
                ),
                destination: ObjectRequest::new(
                    trusted_bucket_name(bucket),
                    trusted_object_key(PHASE8_DST_KEY),
                    requester,
                    None,
                ),
                dst_condition: NO_WRITE,
                directive: MetadataDirective::Copy,
                tagging: if replace_tags {
                    TaggingDirective::Replace(Some(PHASE8_TAGS_XML))
                } else {
                    TaggingDirective::Copy
                },
                acl: PutObjectWriteAcl::None,
                policy_context: PutObjectPolicyContext::default(),
                source_sse_customer: None,
                destination_encryption: WriteEncryptionRequest::none(),
                object_lock: ObjectLockState::default(),
            })
            .map(|_| ())
    }

    pub(super) fn owner_put_source_object(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        key: &str,
    ) -> Result<VersionId, ServerError> {
        test_helpers::put_object(
            coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: ObjectRequest::new(
                    trusted_bucket_name(bucket),
                    trusted_object_key(key),
                    Requester::authenticated(fixtures.owner_user.clone()),
                    None,
                ),
                data: b"phase-8-owner",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,
                acl: PutObjectWriteAcl::None,
            },
        )
        .map(|put| put.version_id)
    }
}

mod phase9_model {
    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum BucketAction {
        GetBucketAcl,
        PutBucketAcl,
        GetBucketVersioning,
        PutBucketVersioning,
        ListBucketVersions,
        ListBucketMultipartUploads,
    }

    impl fmt::Display for BucketAction {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::GetBucketAcl => f.write_str("GetBucketAcl"),
                Self::PutBucketAcl => f.write_str("PutBucketAcl"),
                Self::GetBucketVersioning => f.write_str("GetBucketVersioning"),
                Self::PutBucketVersioning => f.write_str("PutBucketVersioning"),
                Self::ListBucketVersions => f.write_str("ListBucketVersions"),
                Self::ListBucketMultipartUploads => f.write_str("ListBucketMultipartUploads"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum BucketOwnershipShape {
        Standard,
        BucketOwnerEnforced,
    }

    impl fmt::Display for BucketOwnershipShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Standard => f.write_str("standard"),
                Self::BucketOwnerEnforced => f.write_str("bucket-owner-enforced"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum BucketAccessShape {
        Private,
        PublicRead,
        GrantRead,
        GrantReadAcp,
        GrantWriteAcp,
    }

    impl fmt::Display for BucketAccessShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Private => f.write_str("private"),
                Self::PublicRead => f.write_str("public-read"),
                Self::GrantRead => f.write_str("grant-read"),
                Self::GrantReadAcp => f.write_str("grant-read-acp"),
                Self::GrantWriteAcp => f.write_str("grant-write-acp"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum BucketRequesterShape {
        OwnerExact,
        SameAccountAdmin,
        CrossAccount,
    }

    impl fmt::Display for BucketRequesterShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::OwnerExact => f.write_str("owner-exact"),
                Self::SameAccountAdmin => f.write_str("same-account-admin"),
                Self::CrossAccount => f.write_str("cross-account"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum BucketPolicyShape {
        None,
        AllowAction,
        DenyAction,
    }

    impl fmt::Display for BucketPolicyShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::None => f.write_str("no-policy"),
                Self::AllowAction => f.write_str("allow-action"),
                Self::DenyAction => f.write_str("deny-action"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum BucketActionOutcome {
        Allow,
        Deny,
    }

    impl fmt::Display for BucketActionOutcome {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Allow => f.write_str("Allow"),
                Self::Deny => f.write_str("Deny"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct BucketActionScenario {
        pub(super) name: &'static str,
        pub(super) action: BucketAction,
        pub(super) ownership: BucketOwnershipShape,
        pub(super) access: BucketAccessShape,
        pub(super) requester: BucketRequesterShape,
        pub(super) policy: BucketPolicyShape,
        pub(super) expected: BucketActionOutcome,
    }

    impl BucketActionScenario {
        pub(super) fn scenarios(action: BucketAction) -> Vec<Self> {
            use BucketAccessShape as Access;
            use BucketAction as Action;
            use BucketActionOutcome as Outcome;
            use BucketOwnershipShape as Ownership;
            use BucketPolicyShape as Policy;
            use BucketRequesterShape as Requester;

            match action {
                Action::GetBucketAcl => vec![
                    Self {
                        name: "owner-exact-retains-bucket-acl-read-fallback",
                        action,
                        ownership: Ownership::Standard,
                        access: Access::Private,
                        requester: Requester::OwnerExact,
                        policy: Policy::None,
                        expected: Outcome::Allow,
                    },
                    Self {
                        name: "cross-account-read-acp-grant-still-allows-read",
                        action,
                        ownership: Ownership::Standard,
                        access: Access::GrantReadAcp,
                        requester: Requester::CrossAccount,
                        policy: Policy::None,
                        expected: Outcome::Allow,
                    },
                    Self {
                        name: "same-account-admin-still-reads-acl-on-boe-bucket",
                        action,
                        ownership: Ownership::BucketOwnerEnforced,
                        access: Access::Private,
                        requester: Requester::SameAccountAdmin,
                        policy: Policy::None,
                        expected: Outcome::Allow,
                    },
                    Self {
                        name: "explicit-deny-overrides-boe-admin-read-fallback",
                        action,
                        ownership: Ownership::BucketOwnerEnforced,
                        access: Access::Private,
                        requester: Requester::SameAccountAdmin,
                        policy: Policy::DenyAction,
                        expected: Outcome::Deny,
                    },
                    Self {
                        name: "dedicated-policy-can-grant-cross-account-read",
                        action,
                        ownership: Ownership::Standard,
                        access: Access::Private,
                        requester: Requester::CrossAccount,
                        policy: Policy::AllowAction,
                        expected: Outcome::Allow,
                    },
                    Self {
                        name: "explicit-deny-overrides-read-acp-fallback",
                        action,
                        ownership: Ownership::Standard,
                        access: Access::GrantReadAcp,
                        requester: Requester::CrossAccount,
                        policy: Policy::DenyAction,
                        expected: Outcome::Deny,
                    },
                ],
                Action::PutBucketAcl => vec![
                    Self {
                        name: "owner-exact-retains-bucket-acl-write-fallback",
                        action,
                        ownership: Ownership::Standard,
                        access: Access::Private,
                        requester: Requester::OwnerExact,
                        policy: Policy::None,
                        expected: Outcome::Allow,
                    },
                    Self {
                        name: "cross-account-write-acp-grant-still-allows-write",
                        action,
                        ownership: Ownership::Standard,
                        access: Access::GrantWriteAcp,
                        requester: Requester::CrossAccount,
                        policy: Policy::None,
                        expected: Outcome::Allow,
                    },
                    Self {
                        name: "same-account-admin-does-not-gain-bucket-acl-write-fallback",
                        action,
                        ownership: Ownership::Standard,
                        access: Access::Private,
                        requester: Requester::SameAccountAdmin,
                        policy: Policy::None,
                        expected: Outcome::Deny,
                    },
                    Self {
                        name: "dedicated-policy-can-grant-cross-account-write",
                        action,
                        ownership: Ownership::Standard,
                        access: Access::Private,
                        requester: Requester::CrossAccount,
                        policy: Policy::AllowAction,
                        expected: Outcome::Allow,
                    },
                    Self {
                        name: "explicit-deny-overrides-write-acp-fallback",
                        action,
                        ownership: Ownership::Standard,
                        access: Access::GrantWriteAcp,
                        requester: Requester::CrossAccount,
                        policy: Policy::DenyAction,
                        expected: Outcome::Deny,
                    },
                ],
                Action::GetBucketVersioning => vec![
                    Self {
                        name: "owner-exact-retains-versioning-admin-fallback",
                        action,
                        ownership: Ownership::Standard,
                        access: Access::Private,
                        requester: Requester::OwnerExact,
                        policy: Policy::None,
                        expected: Outcome::Allow,
                    },
                    Self {
                        name: "same-account-admin-retains-versioning-admin-fallback",
                        action,
                        ownership: Ownership::Standard,
                        access: Access::Private,
                        requester: Requester::SameAccountAdmin,
                        policy: Policy::None,
                        expected: Outcome::Allow,
                    },
                    Self {
                        name: "cross-account-still-denied-without-dedicated-policy",
                        action,
                        ownership: Ownership::Standard,
                        access: Access::Private,
                        requester: Requester::CrossAccount,
                        policy: Policy::None,
                        expected: Outcome::Deny,
                    },
                    Self {
                        name: "dedicated-policy-can-grant-cross-account-versioning-read",
                        action,
                        ownership: Ownership::Standard,
                        access: Access::Private,
                        requester: Requester::CrossAccount,
                        policy: Policy::AllowAction,
                        expected: Outcome::Allow,
                    },
                ],
                Action::PutBucketVersioning => vec![
                    Self {
                        name: "owner-exact-retains-versioning-admin-write-fallback",
                        action,
                        ownership: Ownership::Standard,
                        access: Access::Private,
                        requester: Requester::OwnerExact,
                        policy: Policy::None,
                        expected: Outcome::Allow,
                    },
                    Self {
                        name: "same-account-admin-retains-versioning-admin-write-fallback",
                        action,
                        ownership: Ownership::Standard,
                        access: Access::Private,
                        requester: Requester::SameAccountAdmin,
                        policy: Policy::None,
                        expected: Outcome::Allow,
                    },
                    Self {
                        name: "cross-account-still-denied-without-dedicated-write-policy",
                        action,
                        ownership: Ownership::Standard,
                        access: Access::Private,
                        requester: Requester::CrossAccount,
                        policy: Policy::None,
                        expected: Outcome::Deny,
                    },
                    Self {
                        name: "dedicated-policy-can-grant-cross-account-versioning-write",
                        action,
                        ownership: Ownership::Standard,
                        access: Access::Private,
                        requester: Requester::CrossAccount,
                        policy: Policy::AllowAction,
                        expected: Outcome::Allow,
                    },
                ],
                Action::ListBucketVersions => vec![
                    Self {
                        name: "owner-exact-retains-listing-fallback",
                        action,
                        ownership: Ownership::Standard,
                        access: Access::Private,
                        requester: Requester::OwnerExact,
                        policy: Policy::None,
                        expected: Outcome::Allow,
                    },
                    Self {
                        name: "public-read-bucket-still-allows-cross-account-version-listing",
                        action,
                        ownership: Ownership::Standard,
                        access: Access::PublicRead,
                        requester: Requester::CrossAccount,
                        policy: Policy::None,
                        expected: Outcome::Allow,
                    },
                    Self {
                        name: "grant-read-still-allows-cross-account-version-listing",
                        action,
                        ownership: Ownership::Standard,
                        access: Access::GrantRead,
                        requester: Requester::CrossAccount,
                        policy: Policy::None,
                        expected: Outcome::Allow,
                    },
                    Self {
                        name: "cross-account-still-denied-on-private-bucket-without-policy",
                        action,
                        ownership: Ownership::Standard,
                        access: Access::Private,
                        requester: Requester::CrossAccount,
                        policy: Policy::None,
                        expected: Outcome::Deny,
                    },
                    Self {
                        name: "dedicated-policy-can-grant-cross-account-version-listing",
                        action,
                        ownership: Ownership::Standard,
                        access: Access::Private,
                        requester: Requester::CrossAccount,
                        policy: Policy::AllowAction,
                        expected: Outcome::Allow,
                    },
                    Self {
                        name: "explicit-deny-overrides-public-read-version-listing-fallback",
                        action,
                        ownership: Ownership::Standard,
                        access: Access::PublicRead,
                        requester: Requester::CrossAccount,
                        policy: Policy::DenyAction,
                        expected: Outcome::Deny,
                    },
                ],
                Action::ListBucketMultipartUploads => vec![
                    Self {
                        name: "owner-exact-retains-multipart-listing-fallback",
                        action,
                        ownership: Ownership::Standard,
                        access: Access::Private,
                        requester: Requester::OwnerExact,
                        policy: Policy::None,
                        expected: Outcome::Allow,
                    },
                    Self {
                        name: "public-read-bucket-still-allows-cross-account-multipart-listing",
                        action,
                        ownership: Ownership::Standard,
                        access: Access::PublicRead,
                        requester: Requester::CrossAccount,
                        policy: Policy::None,
                        expected: Outcome::Allow,
                    },
                    Self {
                        name: "grant-read-still-allows-cross-account-multipart-listing",
                        action,
                        ownership: Ownership::Standard,
                        access: Access::GrantRead,
                        requester: Requester::CrossAccount,
                        policy: Policy::None,
                        expected: Outcome::Allow,
                    },
                    Self {
                        name:
                            "cross-account-still-denied-on-private-bucket-without-multipart-policy",
                        action,
                        ownership: Ownership::Standard,
                        access: Access::Private,
                        requester: Requester::CrossAccount,
                        policy: Policy::None,
                        expected: Outcome::Deny,
                    },
                    Self {
                        name: "dedicated-policy-can-grant-cross-account-multipart-listing",
                        action,
                        ownership: Ownership::Standard,
                        access: Access::Private,
                        requester: Requester::CrossAccount,
                        policy: Policy::AllowAction,
                        expected: Outcome::Allow,
                    },
                    Self {
                        name: "explicit-deny-overrides-public-read-multipart-listing-fallback",
                        action,
                        ownership: Ownership::Standard,
                        access: Access::PublicRead,
                        requester: Requester::CrossAccount,
                        policy: Policy::DenyAction,
                        expected: Outcome::Deny,
                    },
                ],
            }
        }
    }

    impl fmt::Display for BucketActionScenario {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "name={} action={} ownership={} access={} requester={} policy={}",
                self.name, self.action, self.ownership, self.access, self.requester, self.policy
            )
        }
    }
}

mod phase9_harness {
    use super::harness::{setup_coordinator, IdentityFixtures};
    use super::phase9_model::{
        BucketAccessShape, BucketAction, BucketActionOutcome, BucketActionScenario,
        BucketOwnershipShape, BucketPolicyShape, BucketRequesterShape,
    };
    use super::*;

    pub(super) struct Phase9Harness {
        _tmp: test_util::TempDir,
        coord: Coordinator,
        fixtures: IdentityFixtures,
    }

    impl Phase9Harness {
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

        pub(super) fn run(
            &self,
            bucket: &str,
            scenario: BucketActionScenario,
        ) -> BucketActionOutcome {
            materialize_phase9_bucket(&self.coord, &self.fixtures, bucket, scenario)
                .unwrap_or_else(|err| {
                    panic!("failed to materialize phase 9 state for {scenario}: {err:?}");
                });
            run_phase9_action(&self.coord, &self.fixtures, bucket, scenario)
        }
    }

    pub(super) fn phase9_bucket_name_for(action: BucketAction, index: usize) -> String {
        let action_slug = match action {
            BucketAction::GetBucketAcl => "get-bucket-acl",
            BucketAction::PutBucketAcl => "put-bucket-acl",
            BucketAction::GetBucketVersioning => "get-bucket-versioning",
            BucketAction::PutBucketVersioning => "put-bucket-versioning",
            BucketAction::ListBucketVersions => "list-bucket-versions",
            BucketAction::ListBucketMultipartUploads => "list-bucket-multipart",
        };
        format!("authz-phase9-{action_slug}-{index:05}")
    }

    fn materialize_phase9_bucket(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: BucketActionScenario,
    ) -> Result<(), ServerError> {
        let owner = OwnerIdentity::new(
            fixtures.owner_user.principal(),
            fixtures.owner_user.canonical_user_id().clone(),
        );
        let mut grant_entries = Coordinator::bucket_acl_grants_from_flags(
            &owner,
            scenario.access == BucketAccessShape::PublicRead,
            false,
        )
        .iter()
        .cloned()
        .collect::<Vec<_>>();
        if let Some(permission) = phase9_acl_permission(scenario.access) {
            grant_entries.push(AclGrant::new(
                AclGrantee::CanonicalUser(fixtures.cross_account.canonical_user_id().clone()),
                permission,
            ));
        }
        let grants = AclGrants::new(grant_entries);
        coord.create_bucket_with_acl_grants(&owner, bucket, grants, false)?;

        if scenario.ownership == BucketOwnershipShape::BucketOwnerEnforced {
            coord.put_bucket_ownership_controls(&PutBucketOwnershipControlsRequest {
                bucket: BucketRequest::new(
                    trusted_bucket_name(bucket),
                    Requester::authenticated(fixtures.owner_user.clone()),
                    None,
                ),
                config: BucketOwnershipControls {
                    object_ownership: BucketObjectOwnership::BucketOwnerEnforced,
                },
            })?;
        }

        if let Some(policy) = phase9_policy_document(fixtures, bucket, scenario) {
            coord.put_bucket_policy(&PutBucketPolicyRequest {
                bucket: BucketRequest::new(
                    trusted_bucket_name(bucket),
                    Requester::authenticated(fixtures.owner_user.clone()),
                    None,
                ),
                config: &policy,
                confirm_remove_self_bucket_access: false,
            })?;
        }

        Ok(())
    }

    fn phase9_acl_permission(access: BucketAccessShape) -> Option<AclPermission> {
        match access {
            BucketAccessShape::Private | BucketAccessShape::PublicRead => None,
            BucketAccessShape::GrantRead => Some(AclPermission::Read),
            BucketAccessShape::GrantReadAcp => Some(AclPermission::ReadAcp),
            BucketAccessShape::GrantWriteAcp => Some(AclPermission::WriteAcp),
        }
    }

    fn phase9_policy_document(
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: BucketActionScenario,
    ) -> Option<String> {
        let principal = match scenario.requester {
            BucketRequesterShape::OwnerExact => fixtures.owner_user.principal(),
            BucketRequesterShape::SameAccountAdmin => fixtures.same_account_distinct.principal(),
            BucketRequesterShape::CrossAccount => fixtures.cross_account.principal(),
        };
        let effect = match scenario.policy {
            BucketPolicyShape::None => return None,
            BucketPolicyShape::AllowAction => "Allow",
            BucketPolicyShape::DenyAction => "Deny",
        };
        let action = phase9_policy_action_name(scenario.action);
        Some(format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"{effect}","Principal":{{"AWS":"{principal}"}},"Action":"{action}","Resource":"arn:aws:s3:::{bucket}"}}]}}"#
        ))
    }

    fn phase9_policy_action_name(action: BucketAction) -> &'static str {
        match action {
            BucketAction::GetBucketAcl => "s3:GetBucketAcl",
            BucketAction::PutBucketAcl => "s3:PutBucketAcl",
            BucketAction::GetBucketVersioning => "s3:GetBucketVersioning",
            BucketAction::PutBucketVersioning => "s3:PutBucketVersioning",
            BucketAction::ListBucketVersions => "s3:ListBucketVersions",
            BucketAction::ListBucketMultipartUploads => "s3:ListBucketMultipartUploads",
        }
    }

    fn run_phase9_action(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: BucketActionScenario,
    ) -> BucketActionOutcome {
        let requester = phase9_requester(fixtures, scenario.requester);
        let bucket_request =
            BucketRequest::new(trusted_bucket_name(bucket), requester.clone(), None);
        let result = match scenario.action {
            BucketAction::GetBucketAcl => {
                coord.authorize_get_bucket_acl(&bucket_request).map(|_| ())
            }
            BucketAction::PutBucketAcl => coord
                .authorize_put_bucket_acl(&PutBucketAclRequest {
                    bucket: bucket_request,
                    acl: PutBucketAclInput::Canned(BucketAcl::Private),
                    policy_context: PutObjectPolicyContext::default(),
                })
                .map(|_| ()),
            BucketAction::GetBucketVersioning => coord
                .authorize_get_bucket_versioning(&bucket_request)
                .map(|_| ()),
            BucketAction::PutBucketVersioning => coord
                .authorize_put_bucket_versioning(&PutBucketVersioningRequest {
                    bucket: bucket_request,
                    state: BucketVersioningState::Enabled,
                })
                .map(|_| ()),
            BucketAction::ListBucketVersions => coord
                .authorize_list_object_versions(&ListObjectVersionsRequest {
                    bucket: bucket_request,
                    prefix: None,
                    key_marker: None,
                    version_id_marker: None,
                    max_keys: 1000,
                })
                .map(|_| ()),
            BucketAction::ListBucketMultipartUploads => coord
                .authorize_list_multipart_uploads(&ListMultipartUploadsRequest {
                    bucket: bucket_request,
                    prefix: None,
                    key_marker: None,
                    upload_id_marker: None,
                    max_uploads: 1000,
                })
                .map(|_| ()),
        };
        match result {
            Ok(()) => BucketActionOutcome::Allow,
            Err(ServerError::AccessDenied | ServerError::AnonymousApiAccessDenied) => {
                BucketActionOutcome::Deny
            }
            Err(other) => panic!("unexpected phase 9 result for {scenario}: {other:?}"),
        }
    }

    fn phase9_requester(fixtures: &IdentityFixtures, shape: BucketRequesterShape) -> Requester {
        match shape {
            BucketRequesterShape::OwnerExact => {
                Requester::authenticated(fixtures.owner_user.clone())
            }
            BucketRequesterShape::SameAccountAdmin => {
                Requester::authenticated_owner_account_admin(fixtures.same_account_distinct.clone())
            }
            BucketRequesterShape::CrossAccount => {
                Requester::authenticated(fixtures.cross_account.clone())
            }
        }
    }
}

use harness::{bucket_name_for, to_existing_outcome, to_missing_outcome, MatrixHarness};
use model::{Action, MissingScenario, Scenario};
use phase4_harness::{
    acl_bucket_name_for, to_acl_outcome, to_write_outcome, write_bucket_name_for, Phase4Harness,
};
use phase4_model::{AclUpdateAction, AclUpdateScenario, WriteAction, WriteScenario};
use phase5_harness::{phase5_bucket_name_for, to_transition_outcome, Phase5Harness};
use phase5_model::{TransitionScenario, TransitionStep};
use phase6_harness::{
    copy_bucket_name_for, to_copy_outcome, to_upload_part_copy_outcome,
    upload_part_copy_bucket_name_for, Phase6Harness,
};
use phase6_model::{CopyObjectScenario, UploadPartCopyScenario};
use phase7_harness::{
    phase7_delete_bucket_name_for, phase7_object_lock_bucket_name_for, Phase7Harness,
};
use phase7_model::{DeleteScenario, ObjectLockScenario};
use phase7a_harness::{
    phase7a_management_bucket_name_for, phase7a_write_bucket_name_for, Phase7aHarness,
};
use phase7a_model::{MultipartManagementScenario, MultipartWriteScenario};
use phase8_harness::Phase8Harness;
use phase9_harness::{phase9_bucket_name_for, Phase9Harness};
use phase9_model::{BucketAction, BucketActionScenario};

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

#[test]
fn authz_model_phase4_put_object_write_matrix() {
    run_phase4_write_matrix(WriteAction::PutObject);
}

#[test]
fn authz_model_phase4_create_multipart_upload_write_matrix() {
    run_phase4_write_matrix(WriteAction::CreateMultipartUpload);
}

#[test]
fn authz_model_phase4_begin_stream_put_write_matrix() {
    run_phase4_write_matrix(WriteAction::BeginStreamPut);
}

#[test]
fn authz_model_phase4_put_object_acl_matrix() {
    run_phase4_acl_matrix(AclUpdateAction::PutObjectAcl);
}

#[test]
fn authz_model_phase4_put_object_version_acl_matrix() {
    run_phase4_acl_matrix(AclUpdateAction::PutObjectVersionAcl);
}

#[test]
fn authz_model_phase5_transition_matrix() {
    let scenarios = TransitionScenario::scenarios();
    assert!(
        !scenarios.is_empty(),
        "phase 5 transition matrix unexpectedly produced no scenarios"
    );
    let harness = Phase5Harness::new();

    for (index, scenario) in scenarios.into_iter().enumerate() {
        let bucket = phase5_bucket_name_for(index);
        harness.prepare(&bucket, scenario);

        let mut state = scenario.initial_state();
        let mut trace = vec![format!("seed {} {}", bucket, scenario.seed)];

        for step in scenario.steps {
            match *step {
                TransitionStep::Mutate(mutation) => {
                    harness.apply_mutation(&bucket, scenario, mutation);
                    state.apply(mutation);
                    trace.push(format!("mutate {mutation}"));
                }
                TransitionStep::Probe(probe) => {
                    let expected = state.expected_outcome(scenario.seed, probe);
                    let actual = to_transition_outcome(harness.probe(&bucket, scenario, probe));
                    trace.push(format!("probe {probe} => {actual}"));
                    assert_eq!(
                        actual,
                        expected,
                        "phase 5 transition mismatch\nscenario: {scenario}\ntrace:\n{}",
                        trace.join("\n")
                    );
                }
            }
        }
    }
}

#[test]
fn authz_model_phase6_copy_object_matrix() {
    let scenarios = CopyObjectScenario::scenarios();
    assert!(
        !scenarios.is_empty(),
        "phase 6 copy matrix unexpectedly produced no scenarios"
    );
    let harness = Phase6Harness::new();

    for (index, scenario) in scenarios.into_iter().enumerate() {
        let bucket = copy_bucket_name_for(index);
        let expected = scenario.expected_outcome();
        let actual = to_copy_outcome(harness.run_copy_object(&bucket, scenario));
        assert_eq!(
            actual, expected,
            "phase 6 copy-object mismatch\nscenario: {scenario}\nexpected: {expected}\nactual: {actual}"
        );
    }
}

#[test]
fn authz_model_phase6_upload_part_copy_matrix() {
    let scenarios = UploadPartCopyScenario::scenarios();
    assert!(
        !scenarios.is_empty(),
        "phase 6 upload-part-copy matrix unexpectedly produced no scenarios"
    );
    let harness = Phase6Harness::new();

    for (index, scenario) in scenarios.into_iter().enumerate() {
        let bucket = upload_part_copy_bucket_name_for(index);
        let expected = scenario.expected_outcome();
        let actual = to_upload_part_copy_outcome(harness.run_upload_part_copy(&bucket, scenario));
        assert_eq!(
            actual, expected,
            "phase 6 upload-part-copy mismatch\nscenario: {scenario}\nexpected: {expected}\nactual: {actual}"
        );
    }
}

#[test]
fn authz_model_phase7_object_lock_matrix() {
    let scenarios = ObjectLockScenario::scenarios();
    assert!(
        !scenarios.is_empty(),
        "phase 7 object-lock matrix unexpectedly produced no scenarios"
    );
    let harness = Phase7Harness::new();

    for (index, scenario) in scenarios.into_iter().enumerate() {
        let bucket = phase7_object_lock_bucket_name_for(index);
        let actual = harness.run_object_lock(&bucket, scenario);
        assert_eq!(
            actual, scenario.expected,
            "phase 7 object-lock mismatch\nscenario: {scenario}\nexpected: {}\nactual: {actual}",
            scenario.expected
        );
    }
}

#[test]
fn authz_model_phase7_delete_matrix() {
    let scenarios = DeleteScenario::scenarios();
    assert!(
        !scenarios.is_empty(),
        "phase 7 delete matrix unexpectedly produced no scenarios"
    );
    let harness = Phase7Harness::new();

    for (index, scenario) in scenarios.into_iter().enumerate() {
        let bucket = phase7_delete_bucket_name_for(index);
        let actual = harness.run_delete(&bucket, scenario);
        assert_eq!(
            actual, scenario.expected,
            "phase 7 delete mismatch\nscenario: {scenario}\nexpected: {}\nactual: {actual}",
            scenario.expected
        );
    }
}

#[test]
fn authz_model_phase7a_multipart_write_matrix() {
    let scenarios = MultipartWriteScenario::scenarios();
    assert!(
        !scenarios.is_empty(),
        "phase 7A multipart write matrix unexpectedly produced no scenarios"
    );
    let harness = Phase7aHarness::new();

    for (index, scenario) in scenarios.into_iter().enumerate() {
        let bucket = phase7a_write_bucket_name_for(index);
        let actual = harness.run_write(&bucket, scenario);
        assert_eq!(
            actual, scenario.expected,
            "phase 7A multipart write mismatch\nscenario: {scenario}\nexpected: {}\nactual: {actual}",
            scenario.expected
        );
    }
}

#[test]
fn authz_model_phase7a_multipart_management_matrix() {
    let scenarios = MultipartManagementScenario::scenarios();
    assert!(
        !scenarios.is_empty(),
        "phase 7A multipart management matrix unexpectedly produced no scenarios"
    );
    let harness = Phase7aHarness::new();

    for (index, scenario) in scenarios.into_iter().enumerate() {
        let bucket = phase7a_management_bucket_name_for(index);
        let actual = harness.run_management(&bucket, scenario);
        assert_eq!(
            actual, scenario.expected,
            "phase 7A multipart management mismatch\nscenario: {scenario}\nexpected: {}\nactual: {actual}",
            scenario.expected
        );
    }
}

#[test]
fn authz_model_phase8_put_object_private_acl_context_exactness() {
    let harness = Phase8Harness::new();
    let bucket = "authz-phase8-put-object";
    phase8_harness::create_bucket(&harness.coord, &harness.fixtures, bucket).unwrap();
    phase8_harness::put_private_acl_policy(
        &harness.coord,
        &harness.fixtures,
        bucket,
        "s3:PutObject",
    )
    .unwrap();

    let requester = phase8_harness::cross_account_requester(&harness.fixtures);
    let absent = phase8_harness::put_object_private(
        &harness.coord,
        bucket,
        "absent",
        requester.clone(),
        PutObjectPolicyContext::default(),
    );
    assert!(matches!(absent, Err(ServerError::AccessDenied)));

    let exact = phase8_harness::put_object_private(
        &harness.coord,
        bucket,
        "exact",
        requester,
        PutObjectPolicyContext::default().with_default_canned_acl(Some("private")),
    );
    assert!(
        exact.is_ok(),
        "expected exact private ACL context to allow PutObject: {exact:?}"
    );
}

#[test]
fn authz_model_phase8_create_multipart_upload_private_acl_context_exactness() {
    let harness = Phase8Harness::new();
    let bucket = "authz-phase8-mpu";
    phase8_harness::create_bucket(&harness.coord, &harness.fixtures, bucket).unwrap();
    phase8_harness::put_private_acl_policy(
        &harness.coord,
        &harness.fixtures,
        bucket,
        "s3:PutObject",
    )
    .unwrap();

    let requester = phase8_harness::cross_account_requester(&harness.fixtures);
    let absent = phase8_harness::create_multipart_upload_private(
        &harness.coord,
        bucket,
        "absent",
        requester.clone(),
        PutObjectPolicyContext::default(),
    );
    assert!(matches!(absent, Err(ServerError::AccessDenied)));

    let exact = phase8_harness::create_multipart_upload_private(
        &harness.coord,
        bucket,
        "exact",
        requester,
        PutObjectPolicyContext::default().with_default_canned_acl(Some("private")),
    );
    assert!(
        exact.is_ok(),
        "expected exact private ACL context to allow CreateMultipartUpload: {exact:?}"
    );
}

#[test]
fn authz_model_phase8_put_object_acl_private_acl_context_exactness() {
    let harness = Phase8Harness::new();
    let bucket = "authz-phase8-put-acl";
    let key = "target";
    phase8_harness::create_bucket(&harness.coord, &harness.fixtures, bucket).unwrap();
    phase8_harness::put_private_acl_policy(
        &harness.coord,
        &harness.fixtures,
        bucket,
        "s3:PutObjectAcl",
    )
    .unwrap();
    phase8_harness::owner_put_source_object(&harness.coord, &harness.fixtures, bucket, key)
        .unwrap();

    let requester = phase8_harness::cross_account_requester(&harness.fixtures);
    let absent = phase8_harness::put_acl_private(
        &harness.coord,
        bucket,
        key,
        None,
        requester.clone(),
        PutObjectPolicyContext::default(),
    );
    assert!(matches!(absent, Err(ServerError::AccessDenied)));

    let exact = phase8_harness::put_acl_private(
        &harness.coord,
        bucket,
        key,
        None,
        requester,
        PutObjectPolicyContext::default().with_default_canned_acl(Some("private")),
    );
    assert!(
        exact.is_ok(),
        "expected exact private ACL context to allow PutObjectAcl: {exact:?}"
    );
}

#[test]
fn authz_model_phase8_put_object_version_acl_private_acl_context_exactness() {
    let harness = Phase8Harness::new();
    let bucket = "authz-phase8-put-version-acl";
    let key = "target";
    phase8_harness::create_bucket(&harness.coord, &harness.fixtures, bucket).unwrap();
    harness
        .coord
        .put_bucket_versioning(&PutBucketVersioningRequest {
            bucket: BucketRequest::new(
                trusted_bucket_name(bucket),
                Requester::authenticated(harness.fixtures.owner_user.clone()),
                None,
            ),
            state: BucketVersioningState::Enabled,
        })
        .unwrap();
    phase8_harness::put_private_acl_policy(
        &harness.coord,
        &harness.fixtures,
        bucket,
        "s3:PutObjectVersionAcl",
    )
    .unwrap();
    let version_id =
        phase8_harness::owner_put_source_object(&harness.coord, &harness.fixtures, bucket, key)
            .unwrap();

    let requester = phase8_harness::cross_account_requester(&harness.fixtures);
    let absent = phase8_harness::put_acl_private(
        &harness.coord,
        bucket,
        key,
        Some(version_id),
        requester.clone(),
        PutObjectPolicyContext::default(),
    );
    assert!(matches!(absent, Err(ServerError::AccessDenied)));

    let exact = phase8_harness::put_acl_private(
        &harness.coord,
        bucket,
        key,
        Some(version_id),
        requester,
        PutObjectPolicyContext::default().with_default_canned_acl(Some("private")),
    );
    assert!(
        exact.is_ok(),
        "expected exact private ACL context to allow PutObjectVersionAcl: {exact:?}"
    );
}

#[test]
fn authz_model_phase8_copy_object_private_acl_context_exactness() {
    let harness = Phase8Harness::new();
    let bucket = "authz-phase8-copy";
    phase8_harness::create_bucket(&harness.coord, &harness.fixtures, bucket).unwrap();
    phase8_harness::put_copy_private_acl_policy(&harness.coord, &harness.fixtures, bucket).unwrap();
    phase8_harness::owner_put_source_object(
        &harness.coord,
        &harness.fixtures,
        bucket,
        phase8_harness::PHASE8_SRC_KEY,
    )
    .unwrap();

    let requester = phase8_harness::cross_account_requester(&harness.fixtures);
    let absent = phase8_harness::copy_object_private(
        &harness.coord,
        bucket,
        requester.clone(),
        PutObjectPolicyContext::default(),
    );
    assert!(matches!(absent, Err(ServerError::AccessDenied)));

    let exact = phase8_harness::copy_object_private(
        &harness.coord,
        bucket,
        requester,
        PutObjectPolicyContext::default().with_default_canned_acl(Some("private")),
    );
    assert!(
        exact.is_ok(),
        "expected exact private ACL context to allow CopyObject: {exact:?}"
    );
}

#[test]
fn authz_model_phase8_put_object_grant_header_exactness() {
    let harness = Phase8Harness::new();
    let bucket = "authz-phase8-put-grants";
    phase8_harness::create_bucket(&harness.coord, &harness.fixtures, bucket).unwrap();
    let requester = phase8_harness::cross_account_requester(&harness.fixtures);
    let header_value = phase8_harness::cross_account_grant_header_value(&harness.fixtures);

    for grant in phase8_harness::GrantHeaderKind::ALL {
        phase8_harness::put_grant_header_policy(
            &harness.coord,
            &harness.fixtures,
            bucket,
            "s3:PutObject",
            grant,
            header_value,
        )
        .unwrap();

        let absent = phase8_harness::put_object_grant(
            &harness.coord,
            &harness.fixtures,
            bucket,
            &format!("absent-{grant:?}"),
            requester.clone(),
            grant,
            PutObjectPolicyContext::default(),
        );
        assert!(matches!(absent, Err(ServerError::AccessDenied)));

        let wrong = phase8_harness::put_object_grant(
            &harness.coord,
            &harness.fixtures,
            bucket,
            &format!("wrong-{grant:?}"),
            requester.clone(),
            grant.alternate(),
            grant
                .alternate()
                .with_policy_context(PutObjectPolicyContext::default(), Some(header_value)),
        );
        assert!(matches!(wrong, Err(ServerError::AccessDenied)));

        let exact = phase8_harness::put_object_grant(
            &harness.coord,
            &harness.fixtures,
            bucket,
            &format!("exact-{grant:?}"),
            requester.clone(),
            grant,
            grant.with_policy_context(PutObjectPolicyContext::default(), Some(header_value)),
        );
        assert!(
            exact.is_ok(),
            "expected exact grant header context to allow PutObject for {grant:?}: {exact:?}"
        );
    }
}

#[test]
fn authz_model_phase8_copy_object_replace_tags_distinct_from_plain_copy() {
    let harness = Phase8Harness::new();
    let bucket = "authz-phase8-copy-tags";
    phase8_harness::create_bucket(&harness.coord, &harness.fixtures, bucket).unwrap();
    phase8_harness::put_copy_tagging_policy(&harness.coord, &harness.fixtures, bucket).unwrap();
    phase8_harness::owner_put_source_object(
        &harness.coord,
        &harness.fixtures,
        bucket,
        phase8_harness::PHASE8_SRC_KEY,
    )
    .unwrap();

    let requester = phase8_harness::cross_account_requester(&harness.fixtures);
    let plain =
        phase8_harness::copy_object_replace_tags(&harness.coord, bucket, requester.clone(), false);
    assert!(matches!(plain, Err(ServerError::AccessDenied)));

    let replace = phase8_harness::copy_object_replace_tags(&harness.coord, bucket, requester, true);
    assert!(
        replace.is_ok(),
        "expected CopyObject tagging replace to satisfy RequestObjectTag policy: {replace:?}"
    );
}

#[test]
fn authz_model_phase9_get_bucket_acl_matrix() {
    run_phase9_bucket_matrix(BucketAction::GetBucketAcl);
}

#[test]
fn authz_model_phase9_put_bucket_acl_matrix() {
    run_phase9_bucket_matrix(BucketAction::PutBucketAcl);
}

#[test]
fn authz_model_phase9_get_bucket_versioning_matrix() {
    run_phase9_bucket_matrix(BucketAction::GetBucketVersioning);
}

#[test]
fn authz_model_phase9_put_bucket_versioning_matrix() {
    run_phase9_bucket_matrix(BucketAction::PutBucketVersioning);
}

#[test]
fn authz_model_phase9_list_bucket_versions_matrix() {
    run_phase9_bucket_matrix(BucketAction::ListBucketVersions);
}

#[test]
fn authz_model_phase9_list_bucket_multipart_uploads_matrix() {
    run_phase9_bucket_matrix(BucketAction::ListBucketMultipartUploads);
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

fn run_phase4_write_matrix(action: WriteAction) {
    let scenarios = WriteScenario::scenarios(action);
    assert!(
        !scenarios.is_empty(),
        "phase 4 matrix unexpectedly produced no scenarios for {action}"
    );
    let harness = Phase4Harness::new();

    for (index, scenario) in scenarios.into_iter().enumerate() {
        let bucket = write_bucket_name_for(action, index);
        let expected = scenario.expected_outcome();
        let actual = to_write_outcome(harness.run_write(&bucket, scenario));
        assert_eq!(
            actual, expected,
            "phase 4 authz model mismatch\nscenario: {scenario}\nexpected: {expected}\nactual: {actual}"
        );
    }
}

fn run_phase4_acl_matrix(action: AclUpdateAction) {
    let scenarios = AclUpdateScenario::scenarios(action);
    assert!(
        !scenarios.is_empty(),
        "phase 4 matrix unexpectedly produced no scenarios for {action}"
    );
    let harness = Phase4Harness::new();

    for (index, scenario) in scenarios.into_iter().enumerate() {
        let bucket = acl_bucket_name_for(action, index);
        let expected = scenario.expected_outcome();
        let actual = to_acl_outcome(harness.run_acl_update(&bucket, scenario));
        assert_eq!(
            actual, expected,
            "phase 4 authz model mismatch\nscenario: {scenario}\nexpected: {expected}\nactual: {actual}"
        );
    }
}

fn run_phase9_bucket_matrix(action: BucketAction) {
    let scenarios = BucketActionScenario::scenarios(action);
    assert!(
        !scenarios.is_empty(),
        "phase 9 matrix unexpectedly produced no scenarios for {action}"
    );
    let harness = Phase9Harness::new();

    for (index, scenario) in scenarios.into_iter().enumerate() {
        let bucket = phase9_bucket_name_for(action, index);
        let actual = harness.run(&bucket, scenario);
        assert_eq!(
            actual, scenario.expected,
            "phase 9 bucket-action mismatch\nscenario: {scenario}\nexpected: {}\nactual: {actual}",
            scenario.expected
        );
    }
}
