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

use harness::{bucket_name_for, to_existing_outcome, to_missing_outcome, MatrixHarness};
use model::{Action, MissingScenario, Scenario};
use phase4_harness::{
    acl_bucket_name_for, to_acl_outcome, to_write_outcome, write_bucket_name_for, Phase4Harness,
};
use phase4_model::{AclUpdateAction, AclUpdateScenario, WriteAction, WriteScenario};
use phase5_harness::{phase5_bucket_name_for, to_transition_outcome, Phase5Harness};
use phase5_model::{TransitionScenario, TransitionStep};

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
