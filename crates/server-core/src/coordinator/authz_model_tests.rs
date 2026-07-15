use super::authz::modern::{
    self, BoeBucketSummary, ModernObjectReadAuthorization, ModernObjectWriteAuthorization,
    ModernReadAction, ModernWriteAction, PreloadedBucketTags,
};
use super::response_types::ModernBucketSummary;
use super::test_helpers;
use super::test_hooks::{
    install_bucket_policy_load_test_hooks, BucketPolicyLoadTestHooks,
    BUCKET_POLICY_LOAD_TEST_SERIAL,
};
use super::test_support::{
    open_test_storage_cluster, put_bucket_ownership_controls_test, put_bucket_policy_test,
    NO_DELETE, NO_PUT_OBJECT_ACL,
};
use super::*;
use crate::conditional::{ReadCondition, WriteCondition};
use crate::metadata_blob::MetadataBlob;
use crate::sse::ManagedWrappingKeyConfig;
use crate::system_metadata::SystemMetadata;
use proptest::prelude::*;
use proptest::test_runner::Config as ProptestConfig;
use std::cell::Cell;
use std::fmt;
use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

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
const BOE_BUCKET_TAGS_XML: &str =
    "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>";
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
        CrossAccount,
    }

    impl ObjectOwnerKind {
        const ALL: [Self; 4] = [
            Self::BucketOwner,
            Self::SameAccountSharedOther,
            Self::SameAccountDistinct,
            Self::CrossAccount,
        ];
    }

    impl fmt::Display for ObjectOwnerKind {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::BucketOwner => f.write_str("bucket-owner-principal"),
                Self::SameAccountSharedOther => f.write_str("same-account-shared-other"),
                Self::SameAccountDistinct => f.write_str("same-account-distinct"),
                Self::CrossAccount => f.write_str("cross-account"),
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
    pub(super) enum ModernOutcome {
        Allow,
        Deny,
    }

    impl fmt::Display for ModernOutcome {
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

        pub(super) fn expected_modern_existing_outcome(self) -> ModernOutcome {
            match self.action {
                Action::GetObject => self.expected_modern_existing_outcome_for_single_action(
                    self.policy.primary,
                    self.modern_default_get_object_allowed(),
                ),
                Action::GetObjectAttributes => {
                    let read = self.expected_modern_existing_outcome_for_single_action(
                        self.policy.primary,
                        self.modern_default_get_object_allowed(),
                    );
                    let attrs = self.expected_modern_existing_outcome_for_single_action(
                        self.policy.attrs_decision(),
                        self.modern_default_get_object_attributes_allowed(),
                    );
                    Self::combine_modern_outcome(read, attrs)
                }
                Action::GetObjectAcl => {
                    panic!("modern existing outcome is only defined for fast-path modern reads")
                }
                Action::GetObjectTagging
                | Action::PutObjectTagging
                | Action::DeleteObjectTagging => {
                    panic!("modern existing outcome is only defined for read-family actions")
                }
            }
        }

        fn expected_modern_existing_outcome_for_single_action(
            self,
            mut decision: PolicyDecisionShape,
            modern_default_allowed: bool,
        ) -> ModernOutcome {
            decision = self.filter_policy_decision_for_foreign_owned_read_family(decision);
            let policy_allow_survives = !self.modern_policy_is_public()
                || !self.bucket.restrict_public_buckets
                || self.requester.is_bucket_owner_account();

            match decision {
                PolicyDecisionShape::ExplicitDeny => ModernOutcome::Deny,
                PolicyDecisionShape::ExplicitAllowPrivate
                | PolicyDecisionShape::ExplicitAllowPublic
                    if policy_allow_survives =>
                {
                    ModernOutcome::Allow
                }
                PolicyDecisionShape::ExplicitAllowPublic
                | PolicyDecisionShape::ExplicitAllowPrivate
                | PolicyDecisionShape::NoPolicy
                | PolicyDecisionShape::NoMatch => {
                    if modern_default_allowed {
                        ModernOutcome::Allow
                    } else {
                        ModernOutcome::Deny
                    }
                }
            }
        }

        fn combine_modern_outcome(first: ModernOutcome, second: ModernOutcome) -> ModernOutcome {
            match (first, second) {
                (ModernOutcome::Deny, _) | (_, ModernOutcome::Deny) => ModernOutcome::Deny,
                (ModernOutcome::Allow, ModernOutcome::Allow) => ModernOutcome::Allow,
            }
        }

        fn modern_default_get_object_allowed(self) -> bool {
            if self.bucket.ownership == OwnershipShape::BucketOwnerEnforced {
                self.requester.can_bucket_owner_account_admin()
            } else {
                self.requester_matches_object_owner()
            }
        }

        fn modern_default_get_object_attributes_allowed(self) -> bool {
            if self.bucket.ownership == OwnershipShape::BucketOwnerEnforced {
                self.requester_principal_matches_object_owner()
            } else {
                self.requester_matches_object_owner()
            }
        }

        fn modern_policy_is_public(self) -> bool {
            match self.action {
                Action::GetObject => self.policy.primary.is_public_allow(),
                Action::GetObjectAttributes => {
                    self.policy.primary.is_public_allow()
                        || self.policy.attrs_decision().is_public_allow()
                }
                Action::GetObjectAcl
                | Action::GetObjectTagging
                | Action::PutObjectTagging
                | Action::DeleteObjectTagging => false,
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
            let decision = self.filter_policy_decision_for_foreign_owned_read_family(decision);
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

        fn filter_policy_decision_for_foreign_owned_read_family(
            self,
            decision: PolicyDecisionShape,
        ) -> PolicyDecisionShape {
            if self.bucket.ownership == OwnershipShape::BucketOwnerEnforced {
                return decision;
            }
            if self.object.owner_kind != ObjectOwnerKind::CrossAccount {
                return decision;
            }
            if !matches!(
                self.action,
                Action::GetObject | Action::GetObjectAttributes | Action::GetObjectAcl
            ) {
                return decision;
            }
            match decision {
                PolicyDecisionShape::ExplicitAllowPrivate
                | PolicyDecisionShape::ExplicitAllowPublic => PolicyDecisionShape::NoMatch,
                _ => decision,
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
                ) | (
                    RequesterIdentityShape::CrossAccountPrincipal,
                    ObjectOwnerKind::CrossAccount,
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
                ObjectOwnerKind::CrossAccount => Some(CanonicalGroup::CrossAccount),
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
                ObjectOwnerKind::CrossAccount => {
                    Requester::authenticated(self.cross_account.clone())
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
                ObjectOwnerKind::CrossAccount => &self.cross_account,
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
                ObjectOwnerKind::CrossAccount => self.cross_account.principal(),
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

        pub(super) fn run_existing_modern(
            &self,
            bucket: &str,
            scenario: Scenario,
        ) -> ModernObjectReadAuthorization {
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

            run_modern_action(
                &self.coord,
                &self.fixtures,
                bucket,
                scenario,
                object_version,
            )
        }

        pub(super) fn run_existing_boe_fast_path_invariant(
            &self,
            bucket: &str,
            scenario: Scenario,
        ) -> (ClassifiedResult, ModernObjectReadAuthorization) {
            assert_eq!(
                scenario.bucket.ownership,
                model::OwnershipShape::BucketOwnerEnforced,
                "BOE fast-path invariant only applies to BOE scenarios: {scenario}"
            );
            assert!(
                matches!(
                    scenario.action,
                    model::Action::GetObject | model::Action::GetObjectAttributes
                ),
                "BOE fast-path invariant only applies to modern read actions: {scenario}"
            );

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

            self.coord
                .remove_bucket_fast_path(&trusted_bucket_name(bucket));
            assert!(
                self.coord
                    .get_bucket_fast_path(&trusted_bucket_name(bucket))
                    .is_none(),
                "test setup should begin with a cold bucket fast path for {scenario}"
            );

            let warmed = classify(run_action(
                &self.coord,
                &self.fixtures,
                bucket,
                scenario,
                object_version,
            ));
            assert!(
                matches!(
                    warmed,
                    ClassifiedResult::Allow | ClassifiedResult::AccessDenied
                ),
                "existing-object BOE warm-up produced impossible result for {scenario}: {warmed:?}"
            );
            assert!(
                self.coord
                    .get_bucket_fast_path(&trusted_bucket_name(bucket))
                    .is_some(),
                "BOE warm-up should populate the bucket fast path for {scenario}"
            );

            let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
                .get_or_init(|| std::sync::Mutex::new(()))
                .lock()
                .unwrap();
            let saw_storage_load = Arc::new(AtomicBool::new(false));
            let saw_fast_path = Arc::new(AtomicBool::new(false));
            let saw_storage_load_hook = Arc::clone(&saw_storage_load);
            let saw_fast_path_hook = Arc::clone(&saw_fast_path);
            let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
                bucket: Some(bucket.to_string()),
                before_storage_load: Some(Arc::new(move || {
                    saw_storage_load_hook.store(true, Ordering::SeqCst);
                })),
                after_policy_fast_path_hit: Some(Arc::new(move || {
                    saw_fast_path_hook.store(true, Ordering::SeqCst);
                })),
            });

            let actual = classify(run_action(
                &self.coord,
                &self.fixtures,
                bucket,
                scenario,
                object_version,
            ));
            assert!(
                saw_fast_path.load(Ordering::SeqCst),
                "warm BOE read should use the bucket fast path for {scenario}"
            );
            assert!(
                !saw_storage_load.load(Ordering::SeqCst),
                "warm BOE read should not reload bucket policy/tags from storage for {scenario}"
            );

            let modern = run_modern_action(
                &self.coord,
                &self.fixtures,
                bucket,
                scenario,
                object_version,
            );

            (actual, modern)
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

        fn setup_boe_abac_bucket_with_bucket_tag_policy(
            &self,
            bucket: &str,
            policy_action: &str,
        ) -> Requester {
            self.coord
                .create_bucket_for_owner(self.fixtures.owner_user.principal(), bucket, false)
                .unwrap();
            put_bucket_ownership_controls_test(
                &self.coord,
                bucket,
                "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
                Requester::authenticated(self.fixtures.owner_user.clone()),
                None,
            )
            .unwrap();
            self.coord
                .put_bucket_tags(&PutBucketConfigRequest {
                    bucket: BucketRequest::new(
                        trusted_bucket_name(bucket),
                        Requester::authenticated(self.fixtures.owner_user.clone()),
                        None,
                    ),
                    config: BOE_BUCKET_TAGS_XML,
                })
                .unwrap();
            self.coord
                .put_bucket_abac(&PutBucketAbacRequest {
                    bucket: BucketRequest::new(
                        trusted_bucket_name(bucket),
                        Requester::authenticated(self.fixtures.owner_user.clone()),
                        None,
                    ),
                    enabled: true,
                })
                .unwrap();
            put_bucket_policy_test(
                &self.coord,
                bucket,
                &format!(
                    r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"{}"}},"Action":"{}","Resource":"arn:aws:s3:::{}/*","Condition":{{"StringEquals":{{"s3:BucketTag/security":"public"}}}}}}]}}"#,
                    self.fixtures.cross_account.principal(),
                    policy_action,
                    bucket
                ),
                Requester::authenticated(self.fixtures.owner_user.clone()),
                None,
            )
            .unwrap();
            Requester::authenticated(self.fixtures.cross_account.clone())
        }

        pub(super) fn run_boe_bucket_tag_snapshot_loader_invariant(&self, bucket: &str) {
            let requester =
                self.setup_boe_abac_bucket_with_bucket_tag_policy(bucket, "s3:GetObject");
            test_helpers::put_object(
                &self.coord,
                &PutObjectRequest {
                    encryption: WriteEncryptionRequest::none(),
                    policy_context: PutObjectPolicyContext::default(),
                    object_lock: ObjectLockState::default(),
                    object: ObjectRequest::new(
                        trusted_bucket_name(bucket),
                        trusted_object_key(KEY),
                        Requester::authenticated(self.fixtures.owner_user.clone()),
                        None,
                    ),
                    data: b"boe-abac-read",
                    metadata: &MetadataBlob::new(),
                    system_metadata: &SystemMetadata::EMPTY,
                    tags: None,
                    cond: NO_WRITE,
                    acl: NO_PUT_OBJECT_ACL.into(),
                },
            )
            .unwrap();

            self.coord
                .remove_bucket_fast_path(&trusted_bucket_name(bucket));
            self.coord
                .head_object(&GetObjectRequest {
                    sse_customer: None,
                    object: ObjectVersionRequest::new(
                        trusted_bucket_name(bucket),
                        trusted_object_key(KEY),
                        None,
                        requester,
                        None,
                    ),
                    cond: NO_READ,
                })
                .unwrap();
            let cached = self
                .coord
                .get_bucket_fast_path(&trusted_bucket_name(bucket))
                .expect("cold BOE ABAC read should populate fast path");
            assert!(matches!(
                cached.tags,
                storage::BucketFastPathTags::Loaded(_)
            ));
        }

        pub(super) fn run_boe_bucket_tag_fast_path_loader_invariant(&self, bucket: &str) {
            let requester =
                self.setup_boe_abac_bucket_with_bucket_tag_policy(bucket, "s3:GetObject");
            test_helpers::put_object(
                &self.coord,
                &PutObjectRequest {
                    encryption: WriteEncryptionRequest::none(),
                    policy_context: PutObjectPolicyContext::default(),
                    object_lock: ObjectLockState::default(),
                    object: ObjectRequest::new(
                        trusted_bucket_name(bucket),
                        trusted_object_key(KEY),
                        Requester::authenticated(self.fixtures.owner_user.clone()),
                        None,
                    ),
                    data: b"boe-abac-read",
                    metadata: &MetadataBlob::new(),
                    system_metadata: &SystemMetadata::EMPTY,
                    tags: None,
                    cond: NO_WRITE,
                    acl: NO_PUT_OBJECT_ACL.into(),
                },
            )
            .unwrap();
            self.coord
                .head_object(&GetObjectRequest {
                    sse_customer: None,
                    object: ObjectVersionRequest::new(
                        trusted_bucket_name(bucket),
                        trusted_object_key(KEY),
                        None,
                        requester,
                        None,
                    ),
                    cond: NO_READ,
                })
                .unwrap();

            let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
                .get_or_init(|| std::sync::Mutex::new(()))
                .lock()
                .unwrap();
            let saw_storage_load = Arc::new(AtomicBool::new(false));
            let saw_fast_path = Arc::new(AtomicBool::new(false));
            let saw_storage_load_hook = Arc::clone(&saw_storage_load);
            let saw_fast_path_hook = Arc::clone(&saw_fast_path);
            let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
                bucket: Some(bucket.to_string()),
                before_storage_load: Some(Arc::new(move || {
                    saw_storage_load_hook.store(true, Ordering::SeqCst);
                })),
                after_policy_fast_path_hit: Some(Arc::new(move || {
                    saw_fast_path_hook.store(true, Ordering::SeqCst);
                })),
            });

            self.coord
                .head_object(&GetObjectRequest {
                    sse_customer: None,
                    object: ObjectVersionRequest::new(
                        trusted_bucket_name(bucket),
                        trusted_object_key(KEY),
                        None,
                        Requester::authenticated(self.fixtures.cross_account.clone()),
                        None,
                    ),
                    cond: NO_READ,
                })
                .unwrap();
            assert!(
                saw_fast_path.load(Ordering::SeqCst),
                "warm BOE ABAC load should hit the fast path"
            );
            assert!(
                !saw_storage_load.load(Ordering::SeqCst),
                "warm BOE ABAC load should not reload storage"
            );
        }

        pub(super) fn run_boe_bucket_tag_put_object_invariant(&self, bucket: &str) {
            let requester =
                self.setup_boe_abac_bucket_with_bucket_tag_policy(bucket, "s3:PutObject");
            test_helpers::put_object(
                &self.coord,
                &PutObjectRequest {
                    encryption: WriteEncryptionRequest::none(),
                    policy_context: PutObjectPolicyContext::default(),
                    object_lock: ObjectLockState::default(),
                    object: ObjectRequest::new(
                        trusted_bucket_name(bucket),
                        trusted_object_key(KEY),
                        requester,
                        None,
                    ),
                    data: b"boe-abac-write",
                    metadata: &MetadataBlob::new(),
                    system_metadata: &SystemMetadata::EMPTY,
                    tags: None,
                    cond: NO_WRITE,
                    acl: NO_PUT_OBJECT_ACL.into(),
                },
            )
            .unwrap();
        }

        pub(super) fn run_boe_bucket_tag_delete_invariant(&self, bucket: &str) {
            let requester =
                self.setup_boe_abac_bucket_with_bucket_tag_policy(bucket, "s3:DeleteObject");
            test_helpers::put_object(
                &self.coord,
                &PutObjectRequest {
                    encryption: WriteEncryptionRequest::none(),
                    policy_context: PutObjectPolicyContext::default(),
                    object_lock: ObjectLockState::default(),
                    object: ObjectRequest::new(
                        trusted_bucket_name(bucket),
                        trusted_object_key(KEY),
                        Requester::authenticated(self.fixtures.owner_user.clone()),
                        None,
                    ),
                    data: b"boe-abac-delete",
                    metadata: &MetadataBlob::new(),
                    system_metadata: &SystemMetadata::EMPTY,
                    tags: None,
                    cond: NO_WRITE,
                    acl: NO_PUT_OBJECT_ACL.into(),
                },
            )
            .unwrap();

            self.coord
                .delete_object(&DeleteObjectRequest {
                    object: ObjectVersionRequest::new(
                        trusted_bucket_name(bucket),
                        trusted_object_key(KEY),
                        None,
                        requester,
                        None,
                    ),
                    bypass_governance: false,
                    cond: NO_DELETE,
                })
                .unwrap();
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
        let pg_ids: Vec<u32> = (0..1).collect();
        let storage_cluster = open_test_storage_cluster(dir, &pg_ids);
        setup_same_process_coordinator_with_storage_cluster(storage_cluster)
    }

    pub(super) fn setup_same_process_coordinator_with_storage_cluster(
        storage_cluster: Arc<storage::StorageCluster>,
    ) -> Coordinator {
        Coordinator::new_with_managed_key_provider_for_storage_cluster_without_background_sweepers(
            storage_cluster,
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

    fn run_modern_action(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: Scenario,
        object_version: VersionId,
    ) -> ModernObjectReadAuthorization {
        assert_eq!(
            scenario.bucket.ownership,
            model::OwnershipShape::BucketOwnerEnforced,
            "BOE modern read evaluation only applies to BOE scenarios: {scenario}"
        );
        let requester = fixtures.requester(scenario.bucket.owner_principal, scenario.requester);
        let version_id = match scenario.target {
            ExistingTarget::Current => None,
            ExistingTarget::Versioned => Some(object_version),
        };
        let object = coord
            .lookup_object_record(
                &trusted_bucket_name(bucket),
                &trusted_object_key(KEY),
                version_id,
            )
            .unwrap_or_else(|err| panic!("failed to load object record for {scenario}: {err:?}"));
        let policy = policy_document(fixtures, bucket, scenario).map(|body| {
            auth::parse_bucket_policy(&body)
                .unwrap_or_else(|err| panic!("failed to parse policy for {scenario}: {err:?}"))
        });
        let bucket_summary =
            modern_bucket_summary(fixtures, bucket, scenario.bucket, policy.as_ref());
        let action = modern_read_action(scenario.action, version_id);

        let bucket = BoeBucketSummary::assume_boe(&bucket_summary);
        let bucket_tags = PreloadedBucketTags::new(None);
        modern::read_object_authorization_with_bucket_policy(
            &requester,
            bucket,
            bucket_tags,
            &object,
            action,
            policy.as_ref(),
        )
        .unwrap_or_else(|err| {
            panic!("modern auth evaluation failed for {scenario}: {err:?}");
        })
    }

    fn modern_bucket_summary(
        fixtures: &IdentityFixtures,
        bucket: &str,
        shape: BucketShape,
        policy: Option<&auth::BucketPolicy>,
    ) -> ModernBucketSummary {
        assert_eq!(
            shape.ownership,
            model::OwnershipShape::BucketOwnerEnforced,
            "BOE modern read summary only applies to BOE scenarios"
        );
        let owner = fixtures.bucket_owner_account(shape.owner_principal);
        ModernBucketSummary {
            name: trusted_bucket_name(bucket),
            owner_principal: owner.principal().to_string(),
            owner_canonical_id: owner.canonical_user_id().clone(),
            created_at: 0,
            versioning: BucketVersioningState::Enabled,
            object_lock: BucketObjectLockConfig::default(),
            public_access_block: if shape.block_public_acls
                || shape.ignore_public_acls
                || shape.restrict_public_buckets
            {
                Some(PublicAccessBlockConfig {
                    block_public_acls: shape.block_public_acls,
                    ignore_public_acls: shape.ignore_public_acls,
                    block_public_policy: false,
                    restrict_public_buckets: shape.restrict_public_buckets,
                })
            } else {
                None
            },
            ownership_controls: match shape.ownership {
                OwnershipShape::BucketOwnerEnforced => Some(BucketOwnershipControls {
                    object_ownership: BucketObjectOwnership::BucketOwnerEnforced,
                }),
                OwnershipShape::ObjectWriter => None,
            },
            bucket_policy_present: policy.is_some(),
            bucket_policy_public: policy.is_some_and(auth::BucketPolicy::is_public),
            bucket_policy_generation: 0,
            bucket_lifecycle_present: false,
            bucket_lifecycle_generation: 0,
            multipart_upload_id_key: storage::MultipartUploadIdKey::from_bytes([1; 32]),
            bucket_abac_enabled: false,
            encryption: EffectiveBucketEncryptionConfig::default(),
        }
    }

    fn modern_read_action(action: Action, version_id: Option<VersionId>) -> ModernReadAction {
        match action {
            Action::GetObject => ModernReadAction::from_get_object_version(version_id),
            Action::GetObjectAttributes => {
                ModernReadAction::from_get_object_attributes_version(version_id)
            }
            Action::GetObjectAcl => panic!("modern read action is not defined for GetObjectAcl"),
            Action::GetObjectTagging | Action::PutObjectTagging | Action::DeleteObjectTagging => {
                panic!("modern read action is only defined for fast-path modern reads")
            }
        }
    }

    fn classify(result: Result<(), ServerError>) -> ClassifiedResult {
        match result {
            Ok(()) => ClassifiedResult::Allow,
            Err(ServerError::AccessDenied | ServerError::ObjectLockProtectedAccessDenied) => {
                ClassifiedResult::AccessDenied
            }
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
    use super::model::{Outcome, OwnershipShape, PolicyDecisionShape};
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

        pub(super) fn expected_modern_boe_outcome(self) -> Outcome {
            debug_assert_eq!(self.bucket.ownership, OwnershipShape::BucketOwnerEnforced);
            if self.action == WriteAction::CreateMultipartUpload && self.requester.is_anonymous() {
                return Outcome::Deny;
            }
            let fallback = self.requester.is_bucket_owner_account_admin();
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
            if allowed {
                Outcome::Allow
            } else {
                Outcome::Deny
            }
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

        pub(super) fn run_write_modern(
            &self,
            bucket: &str,
            scenario: WriteScenario,
        ) -> ModernObjectWriteAuthorization {
            materialize_write_bucket(&self.coord, &self.fixtures, bucket, scenario.bucket)
                .unwrap_or_else(|err| {
                    panic!("failed to materialize phase 4 write bucket for {scenario}: {err:?}");
                });
            materialize_write_policy(&self.coord, &self.fixtures, bucket, scenario).unwrap_or_else(
                |err| {
                    panic!("failed to materialize phase 4 write policy for {scenario}: {err:?}");
                },
            );
            run_modern_write_action(&self.coord, &self.fixtures, bucket, scenario)
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

    fn run_modern_write_action(
        _coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: WriteScenario,
    ) -> ModernObjectWriteAuthorization {
        let requester = write_requester(fixtures, scenario.requester);
        let policy = write_policy_document(fixtures, bucket, scenario).map(|body| {
            auth::parse_bucket_policy(&body)
                .unwrap_or_else(|err| panic!("failed to parse policy for {scenario}: {err:?}"))
        });
        let bucket_summary =
            modern_write_bucket_summary(fixtures, bucket, scenario.bucket, policy.as_ref());
        let policy_context = match scenario.action {
            WriteAction::BeginStreamPut => PutObjectPolicyContext::default()
                .with_default_canned_acl(scenario.acl.policy_condition_value()),
            WriteAction::PutObject | WriteAction::CreateMultipartUpload => {
                PutObjectPolicyContext::default()
            }
        };
        let bucket = BoeBucketSummary::assume_boe(&bucket_summary);
        let bucket_tags = PreloadedBucketTags::new(None);
        modern::put_object_authorization_with_bucket_policy(
            &requester,
            bucket,
            bucket_tags,
            PHASE4_KEY,
            match scenario.action {
                WriteAction::PutObject | WriteAction::BeginStreamPut => {
                    ModernWriteAction::PutObject
                }
                WriteAction::CreateMultipartUpload => ModernWriteAction::CreateMultipartUpload,
            },
            &policy_context,
            policy.as_ref(),
        )
        .unwrap_or_else(|err| {
            panic!("modern BOE write evaluation failed for {scenario}: {err:?}");
        })
    }

    fn modern_write_bucket_summary(
        fixtures: &IdentityFixtures,
        bucket: &str,
        shape: WriteBucketShape,
        policy: Option<&auth::BucketPolicy>,
    ) -> ModernBucketSummary {
        ModernBucketSummary {
            name: trusted_bucket_name(bucket),
            owner_principal: fixtures.owner_user.principal().to_string(),
            owner_canonical_id: fixtures.owner_user.canonical_user_id().clone(),
            created_at: 0,
            versioning: BucketVersioningState::Enabled,
            object_lock: BucketObjectLockConfig::default(),
            public_access_block: if shape.block_public_acls
                || shape.ignore_public_acls
                || shape.restrict_public_buckets
            {
                Some(PublicAccessBlockConfig {
                    block_public_acls: shape.block_public_acls,
                    ignore_public_acls: shape.ignore_public_acls,
                    block_public_policy: false,
                    restrict_public_buckets: shape.restrict_public_buckets,
                })
            } else {
                None
            },
            ownership_controls: match shape.ownership {
                OwnershipShape::BucketOwnerEnforced => Some(BucketOwnershipControls {
                    object_ownership: BucketObjectOwnership::BucketOwnerEnforced,
                }),
                OwnershipShape::ObjectWriter => None,
            },
            bucket_policy_present: policy.is_some(),
            bucket_policy_public: policy.is_some_and(auth::BucketPolicy::is_public),
            bucket_policy_generation: 0,
            bucket_lifecycle_present: false,
            bucket_lifecycle_generation: 0,
            multipart_upload_id_key: storage::MultipartUploadIdKey::from_bytes([1; 32]),
            bucket_abac_enabled: false,
            encryption: EffectiveBucketEncryptionConfig::default(),
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
            Err(
                ServerError::AccessDenied
                | ServerError::ObjectLockProtectedAccessDenied
                | ServerError::AnonymousApiAccessDenied,
            ) => ClassifiedWriteResult::Deny,
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
    use super::harness::{setup_same_process_coordinator_with_storage_cluster, IdentityFixtures};
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
            let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
            let admin =
                setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
            let reader = setup_same_process_coordinator_with_storage_cluster(storage_cluster);
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
            Err(
                ServerError::AccessDenied
                | ServerError::ObjectLockProtectedAccessDenied
                | ServerError::AnonymousApiAccessDenied,
            ) => ClassifiedTransitionResult::Deny,
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
                Self {
                    name: "boe-cross-account-initiator-no-policy-denied",
                    requester: CopyRequesterShape::CrossAccountPrincipal,
                    bucket: CopyBucketShape::BOE,
                    source_access: CopySourceAccess::Readable,
                    upload_owner: UploadOwnerShape::Requester,
                    upload_state: UploadStateShape::InProgress,
                    policy: UploadPartCopyPolicyShape::NoPolicy,
                    context: CopyContextShape::Exact,
                },
                Self {
                    name: "boe-cross-account-initiator-copy-source-policy-allowed",
                    requester: CopyRequesterShape::CrossAccountPrincipal,
                    bucket: CopyBucketShape::BOE,
                    source_access: CopySourceAccess::Readable,
                    upload_owner: UploadOwnerShape::Requester,
                    upload_state: UploadStateShape::InProgress,
                    policy: UploadPartCopyPolicyShape::AllowWithCopySourceCondition,
                    context: CopyContextShape::Exact,
                },
                Self {
                    name: "boe-bucket-owner-no-policy-allowed",
                    requester: CopyRequesterShape::BucketOwnerPrincipal,
                    bucket: CopyBucketShape::BOE,
                    source_access: CopySourceAccess::Readable,
                    upload_owner: UploadOwnerShape::Requester,
                    upload_state: UploadStateShape::InProgress,
                    policy: UploadPartCopyPolicyShape::NoPolicy,
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
            let fallback = if self.bucket.ownership == OwnershipShape::BucketOwnerEnforced {
                self.requester == CopyRequesterShape::BucketOwnerPrincipal
            } else {
                let can_write_bucket = self.requester == CopyRequesterShape::BucketOwnerPrincipal
                    || self.bucket.bucket_public_write;
                can_manage && can_write_bucket
            };
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
                website_redirect_location: None,
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
            return coord.delete_bucket_policy(&BucketRequest::new(
                trusted_bucket_name(bucket),
                Requester::authenticated(fixtures.owner_user.clone()),
                None,
            ));
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
                policy_context: PutObjectPolicyContext::new(Some("*source-key"), None, None),
                source_sse_customer: None,
                sse_customer: None,
            })
            .map(|_| ())
    }

    fn classify(result: Result<(), ServerError>) -> ClassifiedPhase6Result {
        match result {
            Ok(()) => ClassifiedPhase6Result::Allow,
            Err(
                ServerError::AccessDenied
                | ServerError::ObjectLockProtectedAccessDenied
                | ServerError::AnonymousApiAccessDenied,
            ) => ClassifiedPhase6Result::Deny,
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
        PutObjectPolicyContext::new(
            Some("*source-key"),
            None,
            scenario.acl.canned_acl_condition_value(),
        )
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
    pub(super) enum DeleteOwnershipShape {
        ObjectWriter,
        BucketOwnerEnforced,
    }

    impl fmt::Display for DeleteOwnershipShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::ObjectWriter => f.write_str("object-writer"),
                Self::BucketOwnerEnforced => f.write_str("bucket-owner-enforced"),
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
        pub(super) ownership: DeleteOwnershipShape,
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
                    ownership: DeleteOwnershipShape::ObjectWriter,
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
                    ownership: DeleteOwnershipShape::ObjectWriter,
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
                    ownership: DeleteOwnershipShape::ObjectWriter,
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
                    ownership: DeleteOwnershipShape::ObjectWriter,
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
                    ownership: DeleteOwnershipShape::ObjectWriter,
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
                    ownership: DeleteOwnershipShape::ObjectWriter,
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
                    ownership: DeleteOwnershipShape::ObjectWriter,
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
                    ownership: DeleteOwnershipShape::ObjectWriter,
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
                    ownership: DeleteOwnershipShape::ObjectWriter,
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
                    ownership: DeleteOwnershipShape::ObjectWriter,
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
                    ownership: DeleteOwnershipShape::ObjectWriter,
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
                    ownership: DeleteOwnershipShape::ObjectWriter,
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
                    ownership: DeleteOwnershipShape::ObjectWriter,
                    target: DeleteTargetShape::SpecificVersionExisting,
                    bucket_object_lock_enabled: true,
                    object_lock: DeleteObjectLockShape::LegalHold,
                    bypass_governance: true,
                    policy: DeletePolicyShape::None,
                    expected: DeleteOutcome::Deny,
                },
                Self {
                    name: "boe-owner-exact-retains-delete-fallback-without-policy",
                    requester: Phase7RequesterShape::BucketOwnerStandard,
                    ownership: DeleteOwnershipShape::BucketOwnerEnforced,
                    target: DeleteTargetShape::CurrentExisting,
                    bucket_object_lock_enabled: false,
                    object_lock: DeleteObjectLockShape::None,
                    bypass_governance: false,
                    policy: DeletePolicyShape::None,
                    expected: DeleteOutcome::Allow,
                },
                Self {
                    name: "boe-same-account-admin-retains-delete-fallback-without-policy",
                    requester: Phase7RequesterShape::SameAccountAdmin,
                    ownership: DeleteOwnershipShape::BucketOwnerEnforced,
                    target: DeleteTargetShape::CurrentExisting,
                    bucket_object_lock_enabled: false,
                    object_lock: DeleteObjectLockShape::None,
                    bypass_governance: false,
                    policy: DeletePolicyShape::None,
                    expected: DeleteOutcome::Allow,
                },
                Self {
                    name: "boe-same-account-standard-does-not-get-delete-fallback",
                    requester: Phase7RequesterShape::SameAccountStandard,
                    ownership: DeleteOwnershipShape::BucketOwnerEnforced,
                    target: DeleteTargetShape::CurrentExisting,
                    bucket_object_lock_enabled: false,
                    object_lock: DeleteObjectLockShape::None,
                    bypass_governance: false,
                    policy: DeletePolicyShape::None,
                    expected: DeleteOutcome::Deny,
                },
                Self {
                    name: "boe-cross-account-current-delete-needs-delete-object-policy",
                    requester: Phase7RequesterShape::CrossAccount,
                    ownership: DeleteOwnershipShape::BucketOwnerEnforced,
                    target: DeleteTargetShape::CurrentExisting,
                    bucket_object_lock_enabled: false,
                    object_lock: DeleteObjectLockShape::None,
                    bypass_governance: false,
                    policy: DeletePolicyShape::AllowDeleteObject,
                    expected: DeleteOutcome::Allow,
                },
                Self {
                    name: "boe-cross-account-version-delete-needs-delete-version-policy",
                    requester: Phase7RequesterShape::CrossAccount,
                    ownership: DeleteOwnershipShape::BucketOwnerEnforced,
                    target: DeleteTargetShape::SpecificVersionExisting,
                    bucket_object_lock_enabled: false,
                    object_lock: DeleteObjectLockShape::None,
                    bypass_governance: false,
                    policy: DeletePolicyShape::AllowDeleteVersion,
                    expected: DeleteOutcome::Allow,
                },
            ]
        }
    }

    impl fmt::Display for DeleteScenario {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "Delete ownership={} target={} requester={} lock={} bypass={} policy={}",
                self.ownership,
                self.target,
                self.requester,
                self.object_lock,
                self.bypass_governance,
                self.policy
            )
        }
    }
}

mod phase7_harness {
    use super::harness::{setup_coordinator, IdentityFixtures};
    use super::phase7_model::{
        DeleteObjectLockShape, DeleteOutcome, DeleteOwnershipShape, DeletePolicyShape,
        DeleteScenario, DeleteTargetShape, ObjectLockActionShape, ObjectLockOutcome,
        ObjectLockPolicyShape, ObjectLockRecordShape, ObjectLockScenario, Phase7RequesterShape,
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
            ObjectLockActionShape::GetRetention => coord.get_object_retention(&object).map(|_| ()),
            ObjectLockActionShape::PutRetention => coord
                .put_object_retention(&PutObjectRetentionRequest {
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
            ObjectLockActionShape::GetLegalHold => coord.get_object_legal_hold(&object).map(|_| ()),
            ObjectLockActionShape::PutLegalHold => coord
                .put_object_legal_hold(&PutObjectLegalHoldRequest {
                    object,
                    legal_hold: LegalHoldStatus::On,
                })
                .map(|_| ()),
        };

        match result {
            Ok(()) => ObjectLockOutcome::Allow,
            Err(
                ServerError::AccessDenied
                | ServerError::ObjectLockProtectedAccessDenied
                | ServerError::AnonymousApiAccessDenied,
            ) => ObjectLockOutcome::Deny,
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
        coord.create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name(bucket),
            requester: Requester::authenticated(fixtures.owner_user.clone()),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: match scenario.ownership {
                DeleteOwnershipShape::ObjectWriter => BucketObjectOwnership::ObjectWriter,
                DeleteOwnershipShape::BucketOwnerEnforced => {
                    BucketObjectOwnership::BucketOwnerEnforced
                }
            },
            object_lock_enabled: scenario.bucket_object_lock_enabled,
        })?;
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
                        acl: NO_PUT_OBJECT_ACL.into(),
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
            Err(
                ServerError::AccessDenied
                | ServerError::ObjectLockProtectedAccessDenied
                | ServerError::AnonymousApiAccessDenied,
            ) => DeleteOutcome::Deny,
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

        coord
            .put_object_retention(&PutObjectRetentionRequest {
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
            .map(|_| ())
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
                })
                .map(|_| ()),
            DeleteObjectLockShape::LegalHold => coord
                .put_object_legal_hold(&PutObjectLegalHoldRequest {
                    object: ObjectVersionRequest::new(
                        trusted_bucket_name(bucket),
                        trusted_object_key(KEY),
                        Some(version_id),
                        Requester::authenticated(fixtures.owner_user.clone()),
                        None,
                    ),
                    legal_hold: LegalHoldStatus::On,
                })
                .map(|_| ()),
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
                        name: "boe-cross-account-initiator-needs-putobject-policy",
                        action,
                        upload: MultipartUploadShape::CrossAccountBucketOwnerEnforced,
                        requester: Phase7aRequesterShape::Initiator,
                        policy: MultipartPolicyShape::None,
                        target_completed_upload: false,
                        expected: MultipartOutcome::Deny,
                    },
                    Self {
                        name: "boe-cross-account-initiator-allowed-with-putobject-policy",
                        action,
                        upload: MultipartUploadShape::CrossAccountBucketOwnerEnforced,
                        requester: Phase7aRequesterShape::Initiator,
                        policy: MultipartPolicyShape::AllowRequesterPutObject,
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
                        name: "only-completion-replays-a-completed-upload",
                        action,
                        upload: MultipartUploadShape::CrossAccountObjectWriter,
                        requester: Phase7aRequesterShape::Initiator,
                        policy: MultipartPolicyShape::AllowRequesterPutObject,
                        target_completed_upload: true,
                        expected: match action {
                            MultipartWriteAction::BeginStreamPart => MultipartOutcome::NoSuchUpload,
                            MultipartWriteAction::CompleteMultipartUpload => {
                                MultipartOutcome::Allow
                            }
                        },
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
            Err(
                ServerError::AccessDenied
                | ServerError::ObjectLockProtectedAccessDenied
                | ServerError::AnonymousApiAccessDenied,
            ) => MultipartOutcome::Deny,
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
                website_redirect_location: None,
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
                website_redirect_location: None,
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
                    delimiter: None,
                    key_marker: None,
                    version_id_marker: None,
                    max_keys: 1000,
                    requested_max_keys: Some(1000),
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
            Err(
                ServerError::AccessDenied
                | ServerError::ObjectLockProtectedAccessDenied
                | ServerError::AnonymousApiAccessDenied,
            ) => BucketActionOutcome::Deny,
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

mod phase10_harness {
    use super::harness::{setup_coordinator, IdentityFixtures};
    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum GrantHeaderKind {
        FullControl,
        Read,
        ReadAcp,
        Write,
        WriteAcp,
    }

    impl GrantHeaderKind {
        pub(super) const ALL: [Self; 5] = [
            Self::FullControl,
            Self::Read,
            Self::ReadAcp,
            Self::Write,
            Self::WriteAcp,
        ];

        pub(super) const fn header_name(self) -> &'static str {
            match self {
                Self::FullControl => "s3:x-amz-grant-full-control",
                Self::Read => "s3:x-amz-grant-read",
                Self::ReadAcp => "s3:x-amz-grant-read-acp",
                Self::Write => "s3:x-amz-grant-write",
                Self::WriteAcp => "s3:x-amz-grant-write-acp",
            }
        }

        pub(super) const fn permission(self) -> AclPermission {
            match self {
                Self::FullControl => AclPermission::FullControl,
                Self::Read => AclPermission::Read,
                Self::ReadAcp => AclPermission::ReadAcp,
                Self::Write => AclPermission::Write,
                Self::WriteAcp => AclPermission::WriteAcp,
            }
        }

        pub(super) const fn alternate(self) -> Self {
            match self {
                Self::FullControl => Self::Read,
                Self::Read => Self::ReadAcp,
                Self::ReadAcp => Self::Write,
                Self::Write => Self::WriteAcp,
                Self::WriteAcp => Self::FullControl,
            }
        }

        pub(super) fn with_policy_context<'a>(
            self,
            policy_context: PutObjectPolicyContext<'a>,
            value: Option<&'a str>,
        ) -> PutObjectPolicyContext<'a> {
            match self {
                Self::FullControl => {
                    policy_context.with_acl_grant_headers(None, None, None, None, value)
                }
                Self::Read => policy_context.with_acl_grant_headers(value, None, None, None, None),
                Self::ReadAcp => {
                    policy_context.with_acl_grant_headers(None, None, value, None, None)
                }
                Self::Write => policy_context.with_acl_grant_headers(None, value, None, None, None),
                Self::WriteAcp => {
                    policy_context.with_acl_grant_headers(None, None, None, value, None)
                }
            }
        }
    }

    impl fmt::Display for GrantHeaderKind {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::FullControl => f.write_str("grant-full-control"),
                Self::Read => f.write_str("grant-read"),
                Self::ReadAcp => f.write_str("grant-read-acp"),
                Self::Write => f.write_str("grant-write"),
                Self::WriteAcp => f.write_str("grant-write-acp"),
            }
        }
    }

    pub(super) struct Phase10Harness {
        _tmp: test_util::TempDir,
        pub(super) coord: Coordinator,
        pub(super) fixtures: IdentityFixtures,
    }

    impl Phase10Harness {
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
        coord.create_bucket_for_owner(fixtures.owner_user.principal(), bucket, false)
    }

    pub(super) fn cross_account_requester(fixtures: &IdentityFixtures) -> Requester {
        Requester::authenticated(fixtures.cross_account.clone())
    }

    pub(super) fn cross_account_grant_header_value(fixtures: &IdentityFixtures) -> String {
        format!(r#"id="{}""#, fixtures.cross_account.canonical_user_id())
    }

    pub(super) fn put_bucket_acl_policy(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        statement: &str,
    ) -> Result<(), ServerError> {
        let document = format!(r#"{{"Version":"2012-10-17","Statement":[{statement}]}}"#);
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

    pub(super) fn put_canned_acl_private(
        coord: &Coordinator,
        bucket: &str,
        requester: Requester,
        policy_context: PutObjectPolicyContext<'_>,
    ) -> Result<(), ServerError> {
        coord.put_bucket_acl(&PutBucketAclRequest {
            bucket: BucketRequest::new(trusted_bucket_name(bucket), requester, None),
            acl: PutBucketAclInput::Canned(BucketAcl::Private),
            policy_context,
        })
    }

    pub(super) fn put_grant_acl(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        requester: Requester,
        grant: GrantHeaderKind,
        policy_context: PutObjectPolicyContext<'_>,
    ) -> Result<(), ServerError> {
        let acl_grants = AclGrants::new(vec![AclGrant::new(
            AclGrantee::CanonicalUser(fixtures.cross_account.canonical_user_id().clone()),
            grant.permission(),
        )]);
        coord.put_bucket_acl(&PutBucketAclRequest {
            bucket: BucketRequest::new(trusted_bucket_name(bucket), requester, None),
            acl: PutBucketAclInput::Grants(acl_grants),
            policy_context,
        })
    }
}

mod phase11_model {
    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum BucketMetaAction {
        HeadBucket,
        GetBucketLocation,
    }

    impl fmt::Display for BucketMetaAction {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::HeadBucket => f.write_str("HeadBucket"),
                Self::GetBucketLocation => f.write_str("GetBucketLocation"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum BucketMetaAccessShape {
        Private,
        PublicRead,
        GrantRead,
    }

    impl fmt::Display for BucketMetaAccessShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Private => f.write_str("private"),
                Self::PublicRead => f.write_str("public-read"),
                Self::GrantRead => f.write_str("grant-read"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum BucketMetaRequesterShape {
        OwnerExact,
        SameAccountAdmin,
        CrossAccount,
    }

    impl fmt::Display for BucketMetaRequesterShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::OwnerExact => f.write_str("owner-exact"),
                Self::SameAccountAdmin => f.write_str("same-account-admin"),
                Self::CrossAccount => f.write_str("cross-account"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum BucketMetaPolicyShape {
        None,
        AllowListBucket,
        AllowGetBucketLocation,
    }

    impl fmt::Display for BucketMetaPolicyShape {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::None => f.write_str("no-policy"),
                Self::AllowListBucket => f.write_str("allow-list-bucket"),
                Self::AllowGetBucketLocation => f.write_str("allow-get-bucket-location"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum BucketMetaOutcome {
        Allow,
        Deny,
    }

    impl fmt::Display for BucketMetaOutcome {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Allow => f.write_str("Allow"),
                Self::Deny => f.write_str("Deny"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct BucketMetaScenario {
        pub(super) name: &'static str,
        pub(super) action: BucketMetaAction,
        pub(super) access: BucketMetaAccessShape,
        pub(super) requester: BucketMetaRequesterShape,
        pub(super) policy: BucketMetaPolicyShape,
        pub(super) expected: BucketMetaOutcome,
    }

    impl BucketMetaScenario {
        pub(super) fn scenarios(action: BucketMetaAction) -> Vec<Self> {
            use BucketMetaAccessShape as Access;
            use BucketMetaAction as Action;
            use BucketMetaOutcome as Outcome;
            use BucketMetaPolicyShape as Policy;
            use BucketMetaRequesterShape as Requester;

            match action {
                Action::HeadBucket => vec![
                    Self {
                        name: "owner-exact-retains-private-head-bucket-read-fallback",
                        action,
                        access: Access::Private,
                        requester: Requester::OwnerExact,
                        policy: Policy::None,
                        expected: Outcome::Allow,
                    },
                    Self {
                        name: "public-read-bucket-still-allows-cross-account-head-bucket",
                        action,
                        access: Access::PublicRead,
                        requester: Requester::CrossAccount,
                        policy: Policy::None,
                        expected: Outcome::Allow,
                    },
                    Self {
                        name: "grant-read-bucket-still-allows-cross-account-head-bucket",
                        action,
                        access: Access::GrantRead,
                        requester: Requester::CrossAccount,
                        policy: Policy::None,
                        expected: Outcome::Allow,
                    },
                    Self {
                        name: "list-bucket-policy-still-does-not-grant-head-bucket",
                        action,
                        access: Access::Private,
                        requester: Requester::CrossAccount,
                        policy: Policy::AllowListBucket,
                        expected: Outcome::Deny,
                    },
                ],
                Action::GetBucketLocation => vec![
                    Self {
                        name: "owner-exact-retains-bucket-location-admin-fallback",
                        action,
                        access: Access::Private,
                        requester: Requester::OwnerExact,
                        policy: Policy::None,
                        expected: Outcome::Allow,
                    },
                    Self {
                        name: "same-account-admin-retains-bucket-location-admin-fallback",
                        action,
                        access: Access::Private,
                        requester: Requester::SameAccountAdmin,
                        policy: Policy::None,
                        expected: Outcome::Allow,
                    },
                    Self {
                        name: "cross-account-still-denied-without-dedicated-location-policy",
                        action,
                        access: Access::Private,
                        requester: Requester::CrossAccount,
                        policy: Policy::None,
                        expected: Outcome::Deny,
                    },
                    Self {
                        name: "list-bucket-policy-still-does-not-grant-get-bucket-location",
                        action,
                        access: Access::Private,
                        requester: Requester::CrossAccount,
                        policy: Policy::AllowListBucket,
                        expected: Outcome::Deny,
                    },
                    Self {
                        name: "dedicated-policy-grants-cross-account-get-bucket-location",
                        action,
                        access: Access::Private,
                        requester: Requester::CrossAccount,
                        policy: Policy::AllowGetBucketLocation,
                        expected: Outcome::Allow,
                    },
                ],
            }
        }
    }

    impl fmt::Display for BucketMetaScenario {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "name={} action={} access={} requester={} policy={}",
                self.name, self.action, self.access, self.requester, self.policy
            )
        }
    }
}

mod phase11_harness {
    use super::harness::{setup_coordinator, IdentityFixtures};
    use super::phase11_model::{
        BucketMetaAccessShape, BucketMetaAction, BucketMetaOutcome, BucketMetaPolicyShape,
        BucketMetaRequesterShape, BucketMetaScenario,
    };
    use super::*;

    pub(super) struct Phase11Harness {
        _tmp: test_util::TempDir,
        coord: Coordinator,
        fixtures: IdentityFixtures,
    }

    impl Phase11Harness {
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

        pub(super) fn run(&self, bucket: &str, scenario: BucketMetaScenario) -> BucketMetaOutcome {
            materialize_phase11_bucket(&self.coord, &self.fixtures, bucket, scenario)
                .unwrap_or_else(|err| {
                    panic!("failed to materialize phase 11 state for {scenario}: {err:?}");
                });
            run_phase11_action(&self.coord, &self.fixtures, bucket, scenario)
        }
    }

    pub(super) fn phase11_bucket_name_for(action: BucketMetaAction, index: usize) -> String {
        let action_slug = match action {
            BucketMetaAction::HeadBucket => "head-bucket",
            BucketMetaAction::GetBucketLocation => "get-bucket-location",
        };
        format!("authz-phase11-{action_slug}-{index:05}")
    }

    fn materialize_phase11_bucket(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: BucketMetaScenario,
    ) -> Result<(), ServerError> {
        let owner = OwnerIdentity::new(
            fixtures.owner_user.principal(),
            fixtures.owner_user.canonical_user_id().clone(),
        );
        let mut grant_entries = Coordinator::bucket_acl_grants_from_flags(
            &owner,
            scenario.access == BucketMetaAccessShape::PublicRead,
            false,
        )
        .iter()
        .cloned()
        .collect::<Vec<_>>();
        if scenario.access == BucketMetaAccessShape::GrantRead {
            grant_entries.push(AclGrant::new(
                AclGrantee::CanonicalUser(fixtures.cross_account.canonical_user_id().clone()),
                AclPermission::Read,
            ));
        }
        let grants = AclGrants::new(grant_entries);
        coord.create_bucket_with_acl_grants(&owner, bucket, grants, false)?;

        if let Some(policy) = phase11_policy_document(fixtures, bucket, scenario) {
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

    fn phase11_policy_document(
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: BucketMetaScenario,
    ) -> Option<String> {
        let principal = match scenario.requester {
            BucketMetaRequesterShape::OwnerExact => fixtures.owner_user.principal(),
            BucketMetaRequesterShape::SameAccountAdmin => {
                fixtures.same_account_distinct.principal()
            }
            BucketMetaRequesterShape::CrossAccount => fixtures.cross_account.principal(),
        };
        let action = match scenario.policy {
            BucketMetaPolicyShape::None => return None,
            BucketMetaPolicyShape::AllowListBucket => "s3:ListBucket",
            BucketMetaPolicyShape::AllowGetBucketLocation => "s3:GetBucketLocation",
        };
        Some(format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":"{action}","Resource":"arn:aws:s3:::{bucket}"}}]}}"#
        ))
    }

    fn run_phase11_action(
        coord: &Coordinator,
        fixtures: &IdentityFixtures,
        bucket: &str,
        scenario: BucketMetaScenario,
    ) -> BucketMetaOutcome {
        let requester = phase11_requester(fixtures, scenario.requester);
        let bucket_request =
            BucketRequest::new(trusted_bucket_name(bucket), requester.clone(), None);
        let result = match scenario.action {
            BucketMetaAction::HeadBucket => {
                coord.authorize_head_bucket(&bucket_request).map(|_| ())
            }
            BucketMetaAction::GetBucketLocation => coord
                .authorize_get_bucket_location(&bucket_request)
                .map(|_| ()),
        };
        match result {
            Ok(()) => BucketMetaOutcome::Allow,
            Err(
                ServerError::AccessDenied
                | ServerError::ObjectLockProtectedAccessDenied
                | ServerError::AnonymousApiAccessDenied,
            ) => BucketMetaOutcome::Deny,
            Err(other) => panic!("unexpected phase 11 result for {scenario}: {other:?}"),
        }
    }

    fn phase11_requester(
        fixtures: &IdentityFixtures,
        shape: BucketMetaRequesterShape,
    ) -> Requester {
        match shape {
            BucketMetaRequesterShape::OwnerExact => {
                Requester::authenticated(fixtures.owner_user.clone())
            }
            BucketMetaRequesterShape::SameAccountAdmin => {
                Requester::authenticated_owner_account_admin(fixtures.same_account_distinct.clone())
            }
            BucketMetaRequesterShape::CrossAccount => {
                Requester::authenticated(fixtures.cross_account.clone())
            }
        }
    }
}

mod phase12_model {
    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum BoeTracePolicyState {
        None,
        AllowObjectOnlyPrivate,
        AllowBothPrivate,
        AllowBothPublic,
        DenyBothPrivate,
        AllowBothPrivateTagPublic,
        AllowWritePrivate,
        AllowWritePublic,
        DenyWritePrivate,
        AllowWritePrivateTagPublic,
        AllowDeletePrivate,
        AllowDeletePublic,
        DenyDeletePrivate,
        AllowDeletePrivateTagPublic,
    }

    impl fmt::Display for BoeTracePolicyState {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::None => f.write_str("no-policy"),
                Self::AllowObjectOnlyPrivate => f.write_str("allow-object-only-private"),
                Self::AllowBothPrivate => f.write_str("allow-both-private"),
                Self::AllowBothPublic => f.write_str("allow-both-public"),
                Self::DenyBothPrivate => f.write_str("deny-both-private"),
                Self::AllowBothPrivateTagPublic => {
                    f.write_str("allow-both-private-bucket-tag-public")
                }
                Self::AllowWritePrivate => f.write_str("allow-write-private"),
                Self::AllowWritePublic => f.write_str("allow-write-public"),
                Self::DenyWritePrivate => f.write_str("deny-write-private"),
                Self::AllowWritePrivateTagPublic => {
                    f.write_str("allow-write-private-bucket-tag-public")
                }
                Self::AllowDeletePrivate => f.write_str("allow-delete-private"),
                Self::AllowDeletePublic => f.write_str("allow-delete-public"),
                Self::DenyDeletePrivate => f.write_str("deny-delete-private"),
                Self::AllowDeletePrivateTagPublic => {
                    f.write_str("allow-delete-private-bucket-tag-public")
                }
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum BoeTraceBucketTagState {
        Public,
        Private,
    }

    impl fmt::Display for BoeTraceBucketTagState {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Public => f.write_str("tag-public"),
                Self::Private => f.write_str("tag-private"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum BoeTraceProbe {
        GetObject,
        GetObjectAttributes,
        PutObject,
        CreateMultipartUpload,
        DeleteObject,
    }

    impl fmt::Display for BoeTraceProbe {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::GetObject => f.write_str("get-object"),
                Self::GetObjectAttributes => f.write_str("get-object-attributes"),
                Self::PutObject => f.write_str("put-object"),
                Self::CreateMultipartUpload => f.write_str("create-multipart-upload"),
                Self::DeleteObject => f.write_str("delete-object"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum BoeTraceMutation {
        Policy(BoeTracePolicyState),
        RestrictPublicBuckets(bool),
        BucketAbac(bool),
        BucketTags(BoeTraceBucketTagState),
    }

    impl fmt::Display for BoeTraceMutation {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Policy(policy) => write!(f, "set-policy={policy}"),
                Self::RestrictPublicBuckets(enabled) => {
                    write!(f, "set-restrict-public-buckets={enabled}")
                }
                Self::BucketAbac(enabled) => write!(f, "set-bucket-abac={enabled}"),
                Self::BucketTags(tags) => write!(f, "set-bucket-tags={tags}"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum BoeTraceDecision {
        NoMatch,
        ExplicitAllowPrivate,
        ExplicitAllowPublic,
        ExplicitDeny,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct BoeTraceState {
        pub(super) policy: BoeTracePolicyState,
        pub(super) restrict_public_buckets: bool,
        pub(super) bucket_abac_enabled: bool,
        pub(super) bucket_tags: BoeTraceBucketTagState,
    }

    impl BoeTraceState {
        pub(super) fn new() -> Self {
            Self {
                policy: BoeTracePolicyState::None,
                restrict_public_buckets: false,
                bucket_abac_enabled: false,
                bucket_tags: BoeTraceBucketTagState::Public,
            }
        }

        pub(super) fn apply(&mut self, mutation: BoeTraceMutation) {
            match mutation {
                BoeTraceMutation::Policy(policy) => self.policy = policy,
                BoeTraceMutation::RestrictPublicBuckets(enabled) => {
                    self.restrict_public_buckets = enabled;
                }
                BoeTraceMutation::BucketAbac(enabled) => {
                    self.bucket_abac_enabled = enabled;
                }
                BoeTraceMutation::BucketTags(tags) => self.bucket_tags = tags,
            }
        }

        pub(super) fn expected_outcome(self, probe: BoeTraceProbe) -> model::Outcome {
            let read = self.decision_for_object_read();
            match probe {
                BoeTraceProbe::GetObject => self.resolve_single_action(read),
                BoeTraceProbe::GetObjectAttributes => {
                    let attrs = self.decision_for_attributes_read();
                    if matches!(read, BoeTraceDecision::ExplicitDeny)
                        || matches!(attrs, BoeTraceDecision::ExplicitDeny)
                    {
                        return model::Outcome::Deny;
                    }
                    if self.resolve_single_action(read) == model::Outcome::Allow
                        && self.resolve_single_action(attrs) == model::Outcome::Allow
                    {
                        model::Outcome::Allow
                    } else {
                        model::Outcome::Deny
                    }
                }
                BoeTraceProbe::PutObject | BoeTraceProbe::CreateMultipartUpload => {
                    self.resolve_single_action(self.decision_for_write())
                }
                BoeTraceProbe::DeleteObject => {
                    self.resolve_single_action(self.decision_for_delete())
                }
            }
        }

        fn resolve_single_action(self, decision: BoeTraceDecision) -> model::Outcome {
            match decision {
                BoeTraceDecision::ExplicitDeny => model::Outcome::Deny,
                BoeTraceDecision::ExplicitAllowPrivate => model::Outcome::Allow,
                BoeTraceDecision::ExplicitAllowPublic => {
                    if self.restrict_public_buckets {
                        model::Outcome::Deny
                    } else {
                        model::Outcome::Allow
                    }
                }
                BoeTraceDecision::NoMatch => model::Outcome::Deny,
            }
        }

        fn decision_for_object_read(self) -> BoeTraceDecision {
            match self.policy {
                BoeTracePolicyState::None => BoeTraceDecision::NoMatch,
                BoeTracePolicyState::AllowObjectOnlyPrivate
                | BoeTracePolicyState::AllowBothPrivate => BoeTraceDecision::ExplicitAllowPrivate,
                BoeTracePolicyState::AllowBothPublic => BoeTraceDecision::ExplicitAllowPublic,
                BoeTracePolicyState::DenyBothPrivate => BoeTraceDecision::ExplicitDeny,
                BoeTracePolicyState::AllowBothPrivateTagPublic => {
                    if self.bucket_abac_enabled
                        && self.bucket_tags == BoeTraceBucketTagState::Public
                    {
                        BoeTraceDecision::ExplicitAllowPrivate
                    } else {
                        BoeTraceDecision::NoMatch
                    }
                }
                BoeTracePolicyState::AllowWritePrivate
                | BoeTracePolicyState::AllowWritePublic
                | BoeTracePolicyState::DenyWritePrivate
                | BoeTracePolicyState::AllowWritePrivateTagPublic
                | BoeTracePolicyState::AllowDeletePrivate
                | BoeTracePolicyState::AllowDeletePublic
                | BoeTracePolicyState::DenyDeletePrivate
                | BoeTracePolicyState::AllowDeletePrivateTagPublic => BoeTraceDecision::NoMatch,
            }
        }

        fn decision_for_attributes_read(self) -> BoeTraceDecision {
            match self.policy {
                BoeTracePolicyState::AllowBothPrivate => BoeTraceDecision::ExplicitAllowPrivate,
                BoeTracePolicyState::AllowBothPublic => BoeTraceDecision::ExplicitAllowPublic,
                BoeTracePolicyState::DenyBothPrivate => BoeTraceDecision::ExplicitDeny,
                BoeTracePolicyState::AllowBothPrivateTagPublic => {
                    BoeTraceDecision::ExplicitAllowPrivate
                }
                BoeTracePolicyState::None
                | BoeTracePolicyState::AllowObjectOnlyPrivate
                | BoeTracePolicyState::AllowWritePrivate
                | BoeTracePolicyState::AllowWritePublic
                | BoeTracePolicyState::DenyWritePrivate
                | BoeTracePolicyState::AllowWritePrivateTagPublic
                | BoeTracePolicyState::AllowDeletePrivate
                | BoeTracePolicyState::AllowDeletePublic
                | BoeTracePolicyState::DenyDeletePrivate
                | BoeTracePolicyState::AllowDeletePrivateTagPublic => BoeTraceDecision::NoMatch,
            }
        }

        fn decision_for_write(self) -> BoeTraceDecision {
            match self.policy {
                BoeTracePolicyState::AllowWritePrivate => BoeTraceDecision::ExplicitAllowPrivate,
                BoeTracePolicyState::AllowWritePublic => BoeTraceDecision::ExplicitAllowPublic,
                BoeTracePolicyState::DenyWritePrivate => BoeTraceDecision::ExplicitDeny,
                BoeTracePolicyState::AllowWritePrivateTagPublic => {
                    if self.bucket_abac_enabled
                        && self.bucket_tags == BoeTraceBucketTagState::Public
                    {
                        BoeTraceDecision::ExplicitAllowPrivate
                    } else {
                        BoeTraceDecision::NoMatch
                    }
                }
                _ => BoeTraceDecision::NoMatch,
            }
        }

        fn decision_for_delete(self) -> BoeTraceDecision {
            match self.policy {
                BoeTracePolicyState::AllowDeletePrivate => BoeTraceDecision::ExplicitAllowPrivate,
                BoeTracePolicyState::AllowDeletePublic => BoeTraceDecision::ExplicitAllowPublic,
                BoeTracePolicyState::DenyDeletePrivate => BoeTraceDecision::ExplicitDeny,
                BoeTracePolicyState::AllowDeletePrivateTagPublic => {
                    if self.bucket_abac_enabled
                        && self.bucket_tags == BoeTraceBucketTagState::Public
                    {
                        BoeTraceDecision::ExplicitAllowPrivate
                    } else {
                        BoeTraceDecision::NoMatch
                    }
                }
                _ => BoeTraceDecision::NoMatch,
            }
        }
    }
}

mod phase12_harness {
    use super::harness::{setup_coordinator, IdentityFixtures};
    use super::model::Outcome;
    use super::phase12_model::{
        BoeTraceBucketTagState, BoeTraceMutation, BoeTracePolicyState, BoeTraceProbe,
    };
    use super::*;

    const PHASE12_KEY: &str = "key";
    const PHASE12_BUCKET_TAGS_PUBLIC_XML: &str =
        "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>";
    const PHASE12_BUCKET_TAGS_PRIVATE_XML: &str =
        "<Tagging><TagSet><Tag><Key>security</Key><Value>private</Value></Tag></TagSet></Tagging>";

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum ClassifiedBoeTraceResult {
        Allow,
        Deny,
    }

    pub(super) struct Phase12Harness {
        _tmp: test_util::TempDir,
        coord: Coordinator,
        fixtures: IdentityFixtures,
        bucket_abac_enabled: Cell<bool>,
    }

    impl Phase12Harness {
        pub(super) fn new() -> Self {
            let tmp = test_util::tempdir();
            let coord = setup_coordinator(tmp.path());
            let fixtures = IdentityFixtures::new();
            Self {
                _tmp: tmp,
                coord,
                fixtures,
                bucket_abac_enabled: Cell::new(false),
            }
        }

        pub(super) fn prepare(&self, bucket: &str) {
            let owner = OwnerIdentity::new(
                self.fixtures.owner_user.principal(),
                self.fixtures.owner_user.canonical_user_id().clone(),
            );
            let grants = Coordinator::bucket_acl_grants_from_flags(&owner, false, false);
            self.coord
                .create_bucket_with_acl_grants(&owner, bucket, grants, false)
                .unwrap_or_else(|err| panic!("failed to create phase 12 bucket: {err:?}"));
            self.coord
                .put_bucket_ownership_controls(&PutBucketOwnershipControlsRequest {
                    bucket: BucketRequest::new(
                        trusted_bucket_name(bucket),
                        Requester::authenticated(self.fixtures.owner_user.clone()),
                        None,
                    ),
                    config: BucketOwnershipControls {
                        object_ownership: BucketObjectOwnership::BucketOwnerEnforced,
                    },
                })
                .unwrap_or_else(|err| panic!("failed to enable BOE for phase 12 bucket: {err:?}"));
            self.coord
                .put_bucket_tags(&PutBucketConfigRequest {
                    bucket: BucketRequest::new(
                        trusted_bucket_name(bucket),
                        Requester::authenticated(self.fixtures.owner_user.clone()),
                        None,
                    ),
                    config: PHASE12_BUCKET_TAGS_PUBLIC_XML,
                })
                .unwrap_or_else(|err| panic!("failed to seed phase 12 bucket tags: {err:?}"));
            self.bucket_abac_enabled.set(false);
            test_helpers::put_object(
                &self.coord,
                &PutObjectRequest {
                    encryption: WriteEncryptionRequest::none(),
                    policy_context: PutObjectPolicyContext::default(),
                    object_lock: ObjectLockState::default(),
                    object: ObjectRequest::new(
                        trusted_bucket_name(bucket),
                        trusted_object_key(PHASE12_KEY),
                        Requester::authenticated(self.fixtures.owner_user.clone()),
                        None,
                    ),
                    data: b"phase-12-object",
                    metadata: &MetadataBlob::new(),
                    system_metadata: &SystemMetadata::EMPTY,
                    tags: None,
                    cond: NO_WRITE,
                    acl: PutObjectWriteAcl::None,
                },
            )
            .unwrap_or_else(|err| panic!("failed to seed phase 12 object: {err:?}"));
        }

        pub(super) fn apply_mutation(&self, bucket: &str, mutation: BoeTraceMutation) {
            let owner_requester = Requester::authenticated(self.fixtures.owner_user.clone());
            match mutation {
                BoeTraceMutation::Policy(policy) => match policy {
                    BoeTracePolicyState::None => self
                        .coord
                        .delete_bucket_policy(&BucketRequest::new(
                            trusted_bucket_name(bucket),
                            owner_requester,
                            None,
                        ))
                        .unwrap_or_else(|err| {
                            panic!("failed to delete phase 12 bucket policy: {err:?}")
                        }),
                    _ => {
                        let config = phase12_policy_document(&self.fixtures, bucket, policy);
                        self.coord
                            .put_bucket_policy(&PutBucketPolicyRequest {
                                bucket: BucketRequest::new(
                                    trusted_bucket_name(bucket),
                                    owner_requester,
                                    None,
                                ),
                                config: &config,
                                confirm_remove_self_bucket_access: false,
                            })
                            .unwrap_or_else(|err| {
                                panic!("failed to set phase 12 bucket policy {policy}: {err:?}")
                            });
                    }
                },
                BoeTraceMutation::RestrictPublicBuckets(enabled) => {
                    if enabled {
                        self.coord
                            .put_bucket_public_access_block(&PutBucketPublicAccessBlockRequest {
                                bucket: BucketRequest::new(
                                    trusted_bucket_name(bucket),
                                    owner_requester,
                                    None,
                                ),
                                config: PublicAccessBlockConfig {
                                    block_public_acls: false,
                                    ignore_public_acls: false,
                                    block_public_policy: false,
                                    restrict_public_buckets: true,
                                },
                            })
                            .unwrap_or_else(|err| {
                                panic!(
                                    "failed to enable restrict-public-buckets for phase 12: {err:?}"
                                )
                            });
                    } else {
                        self.coord
                            .delete_bucket_public_access_block(&BucketRequest::new(
                                trusted_bucket_name(bucket),
                                owner_requester,
                                None,
                            ))
                            .unwrap_or_else(|err| {
                                panic!("failed to delete public access block for phase 12: {err:?}")
                            });
                    }
                }
                BoeTraceMutation::BucketAbac(enabled) => {
                    self.coord
                        .put_bucket_abac(&PutBucketAbacRequest {
                            bucket: BucketRequest::new(
                                trusted_bucket_name(bucket),
                                owner_requester,
                                None,
                            ),
                            enabled,
                        })
                        .unwrap_or_else(|err| {
                            panic!("failed to set bucket ABAC for phase 12 bucket: {err:?}")
                        });
                    self.bucket_abac_enabled.set(enabled);
                }
                BoeTraceMutation::BucketTags(tags) => {
                    let config = match tags {
                        BoeTraceBucketTagState::Public => PHASE12_BUCKET_TAGS_PUBLIC_XML,
                        BoeTraceBucketTagState::Private => PHASE12_BUCKET_TAGS_PRIVATE_XML,
                    };
                    if self.bucket_abac_enabled.get() {
                        self.coord
                            .put_bucket_tags_for_tag_resource(&PutBucketTagControlRequest {
                                control: BucketTagControlRequest {
                                    bucket: BucketRequest::new(
                                        trusted_bucket_name(bucket),
                                        owner_requester,
                                        None,
                                    ),
                                    account_id: "111122223333",
                                },
                                config,
                                request_tags: &[],
                            })
                            .unwrap_or_else(|err| {
                                panic!("failed to set bucket tags through tag control: {err:?}")
                            });
                    } else {
                        self.coord
                            .put_bucket_tags(&PutBucketConfigRequest {
                                bucket: BucketRequest::new(
                                    trusted_bucket_name(bucket),
                                    owner_requester,
                                    None,
                                ),
                                config,
                            })
                            .unwrap_or_else(|err| {
                                panic!("failed to set bucket tags through tagging API: {err:?}")
                            });
                    }
                }
            }
        }

        pub(super) fn probe(&self, bucket: &str, probe: BoeTraceProbe) -> ClassifiedBoeTraceResult {
            let requester = Requester::authenticated(self.fixtures.cross_account.clone());
            let result = match probe {
                BoeTraceProbe::GetObject => self
                    .coord
                    .get_object(&GetObjectRequest {
                        sse_customer: None,
                        object: ObjectVersionRequest::new(
                            trusted_bucket_name(bucket),
                            trusted_object_key(PHASE12_KEY),
                            None,
                            requester,
                            None,
                        ),
                        cond: NO_READ,
                    })
                    .and_then(|result| {
                        let _ = read_all_phase12_body(result.body)?;
                        Ok(())
                    }),
                BoeTraceProbe::GetObjectAttributes => self
                    .coord
                    .get_object_attributes(&GetObjectAttributesRequest {
                        object: ObjectVersionRequest::new(
                            trusted_bucket_name(bucket),
                            trusted_object_key(PHASE12_KEY),
                            None,
                            requester,
                            None,
                        ),
                        cond: NO_READ,
                        want_parts: false,
                        part_number_marker: None,
                        max_parts: 0,
                        sse_customer: None,
                    })
                    .map(|_| ()),
                BoeTraceProbe::PutObject => self
                    .coord
                    .put_object(&PutObjectRequest {
                        encryption: WriteEncryptionRequest::none(),
                        policy_context: PutObjectPolicyContext::default(),
                        object_lock: ObjectLockState::default(),
                        object: ObjectRequest::new(
                            trusted_bucket_name(bucket),
                            trusted_object_key(PHASE12_KEY),
                            requester,
                            None,
                        ),
                        data: b"phase12-overwrite",
                        metadata: &MetadataBlob::new(),
                        system_metadata: &SystemMetadata::EMPTY,
                        tags: None,
                        cond: NO_WRITE,
                        acl: NO_PUT_OBJECT_ACL.into(),
                    })
                    .map(|_| ()),
                BoeTraceProbe::CreateMultipartUpload => self
                    .coord
                    .create_multipart_upload(&CreateMultipartUploadRequest {
                        object: ObjectRequest::new(
                            trusted_bucket_name(bucket),
                            trusted_object_key(PHASE12_KEY),
                            requester,
                            None,
                        ),
                        metadata: &MetadataBlob::new(),
                        system_metadata: &SystemMetadata::EMPTY,
                        tags: None,
                        checksum: None,
                        acl: NO_PUT_OBJECT_ACL.into(),
                        policy_context: PutObjectPolicyContext::default(),
                        object_lock: ObjectLockState::default(),
                        encryption: WriteEncryptionRequest::none(),
                    })
                    .map(|_| ()),
                BoeTraceProbe::DeleteObject => self
                    .coord
                    .delete_object(&DeleteObjectRequest {
                        object: ObjectVersionRequest::new(
                            trusted_bucket_name(bucket),
                            trusted_object_key(PHASE12_KEY),
                            None,
                            requester,
                            None,
                        ),
                        bypass_governance: false,
                        cond: NO_DELETE,
                    })
                    .map(|_| ()),
            };
            classify_phase12(result)
        }
    }

    pub(super) fn to_boe_trace_outcome(result: ClassifiedBoeTraceResult) -> Outcome {
        match result {
            ClassifiedBoeTraceResult::Allow => Outcome::Allow,
            ClassifiedBoeTraceResult::Deny => Outcome::Deny,
        }
    }

    fn phase12_policy_document(
        fixtures: &IdentityFixtures,
        bucket: &str,
        policy: BoeTracePolicyState,
    ) -> String {
        match policy {
            BoeTracePolicyState::None => panic!("no policy document for no-policy state"),
            BoeTracePolicyState::AllowObjectOnlyPrivate => format!(
                r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"{}"}},"Action":"s3:GetObject","Resource":"arn:aws:s3:::{bucket}/{PHASE12_KEY}"}}]}}"#,
                fixtures.cross_account.principal()
            ),
            BoeTracePolicyState::AllowBothPrivate => format!(
                r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"{}"}},"Action":["s3:GetObject","s3:GetObjectAttributes"],"Resource":"arn:aws:s3:::{bucket}/{PHASE12_KEY}"}}]}}"#,
                fixtures.cross_account.principal()
            ),
            BoeTracePolicyState::AllowBothPublic => format!(
                r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":"*","Action":["s3:GetObject","s3:GetObjectAttributes"],"Resource":"arn:aws:s3:::{bucket}/{PHASE12_KEY}"}}]}}"#
            ),
            BoeTracePolicyState::DenyBothPrivate => format!(
                r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Deny","Principal":{{"AWS":"{}"}},"Action":["s3:GetObject","s3:GetObjectAttributes"],"Resource":"arn:aws:s3:::{bucket}/{PHASE12_KEY}"}}]}}"#,
                fixtures.cross_account.principal()
            ),
            BoeTracePolicyState::AllowBothPrivateTagPublic => format!(
                r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"{}"}},"Action":"s3:GetObject","Resource":"arn:aws:s3:::{bucket}/{PHASE12_KEY}","Condition":{{"StringEquals":{{"s3:BucketTag/security":"public"}}}}}},{{"Effect":"Allow","Principal":{{"AWS":"{}"}},"Action":"s3:GetObjectAttributes","Resource":"arn:aws:s3:::{bucket}/{PHASE12_KEY}"}}]}}"#,
                fixtures.cross_account.principal(),
                fixtures.cross_account.principal()
            ),
            BoeTracePolicyState::AllowWritePrivate => format!(
                r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"{}"}},"Action":"s3:PutObject","Resource":"arn:aws:s3:::{bucket}/{PHASE12_KEY}"}}]}}"#,
                fixtures.cross_account.principal()
            ),
            BoeTracePolicyState::AllowWritePublic => format!(
                r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::{bucket}/{PHASE12_KEY}"}}]}}"#
            ),
            BoeTracePolicyState::DenyWritePrivate => format!(
                r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Deny","Principal":{{"AWS":"{}"}},"Action":"s3:PutObject","Resource":"arn:aws:s3:::{bucket}/{PHASE12_KEY}"}}]}}"#,
                fixtures.cross_account.principal()
            ),
            BoeTracePolicyState::AllowWritePrivateTagPublic => format!(
                r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"{}"}},"Action":"s3:PutObject","Resource":"arn:aws:s3:::{bucket}/{PHASE12_KEY}","Condition":{{"StringEquals":{{"s3:BucketTag/security":"public"}}}}}}]}}"#,
                fixtures.cross_account.principal()
            ),
            BoeTracePolicyState::AllowDeletePrivate => format!(
                r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"{}"}},"Action":"s3:DeleteObject","Resource":"arn:aws:s3:::{bucket}/{PHASE12_KEY}"}}]}}"#,
                fixtures.cross_account.principal()
            ),
            BoeTracePolicyState::AllowDeletePublic => format!(
                r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":"*","Action":"s3:DeleteObject","Resource":"arn:aws:s3:::{bucket}/{PHASE12_KEY}"}}]}}"#
            ),
            BoeTracePolicyState::DenyDeletePrivate => format!(
                r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Deny","Principal":{{"AWS":"{}"}},"Action":"s3:DeleteObject","Resource":"arn:aws:s3:::{bucket}/{PHASE12_KEY}"}}]}}"#,
                fixtures.cross_account.principal()
            ),
            BoeTracePolicyState::AllowDeletePrivateTagPublic => format!(
                r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"{}"}},"Action":"s3:DeleteObject","Resource":"arn:aws:s3:::{bucket}/{PHASE12_KEY}","Condition":{{"StringEquals":{{"s3:BucketTag/security":"public"}}}}}}]}}"#,
                fixtures.cross_account.principal()
            ),
        }
    }

    fn classify_phase12(result: Result<(), ServerError>) -> ClassifiedBoeTraceResult {
        match result {
            Ok(()) => ClassifiedBoeTraceResult::Allow,
            Err(
                ServerError::AccessDenied
                | ServerError::ObjectLockProtectedAccessDenied
                | ServerError::AnonymousApiAccessDenied,
            ) => ClassifiedBoeTraceResult::Deny,
            Err(other) => panic!("unexpected phase 12 probe result: {other:?}"),
        }
    }

    fn read_all_phase12_body(mut body: ReadHandle) -> Result<Vec<u8>, ServerError> {
        let mut out = Vec::new();
        while let Some(chunk) = body.next_chunk(INTERNAL_SEGMENT_SIZE)? {
            out.extend_from_slice(&chunk);
        }
        Ok(out)
    }
}

mod phase13_model {
    use super::phase7_model::DeletePolicyShape;
    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum Phase13ReadPolicyState {
        None,
        AllowPrivate,
    }

    impl fmt::Display for Phase13ReadPolicyState {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::None => f.write_str("no-read-policy"),
                Self::AllowPrivate => f.write_str("allow-read-private"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum Phase13CurrentState {
        Live,
        DeleteMarker,
        Missing,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum Phase13RetentionState {
        None,
        Governance,
        Compliance,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum Phase13Mutation {
        ReadPolicy(Phase13ReadPolicyState),
        DeletePolicy(DeletePolicyShape),
        OwnerPutCurrentVersion,
        OwnerDelete,
        OwnerDeleteTrackedVersion,
        ApplyTrackedGovernanceRetention,
        ApplyTrackedComplianceRetention,
        ApplyTrackedLegalHold,
    }

    impl fmt::Display for Phase13Mutation {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::ReadPolicy(state) => write!(f, "set-read-policy={state}"),
                Self::DeletePolicy(policy) => write!(f, "set-delete-policy={policy}"),
                Self::OwnerPutCurrentVersion => f.write_str("owner-put-current-version"),
                Self::OwnerDelete => f.write_str("owner-delete-current"),
                Self::OwnerDeleteTrackedVersion => f.write_str("owner-delete-tracked-version"),
                Self::ApplyTrackedGovernanceRetention => {
                    f.write_str("apply-tracked-governance-retention")
                }
                Self::ApplyTrackedComplianceRetention => {
                    f.write_str("apply-tracked-compliance-retention")
                }
                Self::ApplyTrackedLegalHold => f.write_str("apply-tracked-legal-hold"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum Phase13Probe {
        Object,
        Attributes,
        Delete,
        DeleteTrackedVersion,
        DeleteTrackedVersionBypass,
    }

    impl fmt::Display for Phase13Probe {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Object => f.write_str("get-object-current"),
                Self::Attributes => f.write_str("get-object-attributes-current"),
                Self::Delete => f.write_str("delete-current"),
                Self::DeleteTrackedVersion => f.write_str("delete-tracked-version"),
                Self::DeleteTrackedVersionBypass => f.write_str("delete-tracked-version-bypass"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum Phase13ExecutionProbe {
        Current,
        Version,
        VersionBypass,
    }

    impl fmt::Display for Phase13ExecutionProbe {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Current => f.write_str("delete-current-then-read-current"),
                Self::Version => f.write_str("delete-tracked-version-then-read-current"),
                Self::VersionBypass => {
                    f.write_str("delete-tracked-version-bypass-then-read-current")
                }
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum Phase13ExecutionOutcome {
        Denied,
        Applied {
            delete_marker: bool,
            current_read: model::Outcome,
            tracked_version_read: Option<model::Outcome>,
        },
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct Phase13State {
        pub(super) read_policy: Phase13ReadPolicyState,
        pub(super) delete_policy: DeletePolicyShape,
        pub(super) current: Phase13CurrentState,
        pub(super) tracked_version_present: bool,
        pub(super) tracked_version_is_current: bool,
        pub(super) revealed_after_tracked_delete: Phase13CurrentState,
        pub(super) tracked_version_retention: Phase13RetentionState,
        pub(super) tracked_version_legal_hold: bool,
    }

    impl Phase13State {
        pub(super) fn new() -> Self {
            Self {
                read_policy: Phase13ReadPolicyState::None,
                delete_policy: DeletePolicyShape::None,
                current: Phase13CurrentState::Live,
                tracked_version_present: true,
                tracked_version_is_current: true,
                revealed_after_tracked_delete: Phase13CurrentState::Missing,
                tracked_version_retention: Phase13RetentionState::None,
                tracked_version_legal_hold: false,
            }
        }

        pub(super) fn apply(&mut self, mutation: Phase13Mutation) {
            match mutation {
                Phase13Mutation::ReadPolicy(state) => self.read_policy = state,
                Phase13Mutation::DeletePolicy(policy) => self.delete_policy = policy,
                Phase13Mutation::OwnerPutCurrentVersion => {
                    self.revealed_after_tracked_delete = self.current;
                    self.current = Phase13CurrentState::Live;
                    self.tracked_version_present = true;
                    self.tracked_version_is_current = true;
                    self.tracked_version_retention = Phase13RetentionState::None;
                    self.tracked_version_legal_hold = false;
                }
                Phase13Mutation::OwnerDelete => {
                    self.current = Phase13CurrentState::DeleteMarker;
                    self.tracked_version_is_current = false;
                }
                Phase13Mutation::OwnerDeleteTrackedVersion => {
                    if self.tracked_version_present
                        && self.tracked_version_retention == Phase13RetentionState::None
                        && !self.tracked_version_legal_hold
                    {
                        self.tracked_version_present = false;
                        if self.tracked_version_is_current {
                            self.current = self.revealed_after_tracked_delete;
                        }
                        self.tracked_version_is_current = false;
                        self.tracked_version_retention = Phase13RetentionState::None;
                        self.tracked_version_legal_hold = false;
                    }
                }
                Phase13Mutation::ApplyTrackedGovernanceRetention => {
                    if self.tracked_version_present
                        && self.tracked_version_retention == Phase13RetentionState::None
                    {
                        self.tracked_version_retention = Phase13RetentionState::Governance;
                    }
                }
                Phase13Mutation::ApplyTrackedComplianceRetention => {
                    if self.tracked_version_present
                        && self.tracked_version_retention == Phase13RetentionState::None
                    {
                        self.tracked_version_retention = Phase13RetentionState::Compliance;
                    }
                }
                Phase13Mutation::ApplyTrackedLegalHold => {
                    if self.tracked_version_present {
                        self.tracked_version_legal_hold = true;
                    }
                }
            }
        }

        pub(super) fn expected_outcome(self, probe: Phase13Probe) -> model::Outcome {
            match probe {
                Phase13Probe::Object | Phase13Probe::Attributes => self.current_read_outcome(),
                Phase13Probe::Delete => {
                    if self.delete_policy == DeletePolicyShape::AllowDeleteObject {
                        model::Outcome::Allow
                    } else {
                        model::Outcome::Deny
                    }
                }
                Phase13Probe::DeleteTrackedVersion => self.expected_delete_version(false),
                Phase13Probe::DeleteTrackedVersionBypass => self.expected_delete_version(true),
            }
        }

        pub(super) fn expected_execution_outcome(
            self,
            probe: Phase13ExecutionProbe,
        ) -> Phase13ExecutionOutcome {
            match probe {
                Phase13ExecutionProbe::Current => {
                    if self.delete_policy != DeletePolicyShape::AllowDeleteObject {
                        return Phase13ExecutionOutcome::Denied;
                    }
                    let current = Phase13CurrentState::DeleteMarker;
                    Self::execution_outcome_from_read_policy(self.read_policy, current, true, None)
                }
                Phase13ExecutionProbe::Version => self.expected_version_delete_execution(false),
                Phase13ExecutionProbe::VersionBypass => {
                    self.expected_version_delete_execution(true)
                }
            }
        }

        fn expected_delete_version(self, bypass: bool) -> model::Outcome {
            let delete_allowed = matches!(
                self.delete_policy,
                DeletePolicyShape::AllowDeleteVersion
                    | DeletePolicyShape::AllowDeleteVersionAndBypass
            );
            if !delete_allowed {
                return model::Outcome::Deny;
            }

            let bypass_allowed =
                self.delete_policy == DeletePolicyShape::AllowDeleteVersionAndBypass;

            if !self.tracked_version_present {
                if !bypass || bypass_allowed {
                    return model::Outcome::Allow;
                }
                return model::Outcome::Deny;
            }

            if self.tracked_version_legal_hold {
                return model::Outcome::Deny;
            }

            match self.tracked_version_retention {
                Phase13RetentionState::None => model::Outcome::Allow,
                Phase13RetentionState::Governance => {
                    if bypass && bypass_allowed {
                        model::Outcome::Allow
                    } else {
                        model::Outcome::Deny
                    }
                }
                Phase13RetentionState::Compliance => model::Outcome::Deny,
            }
        }

        fn expected_version_delete_execution(self, bypass: bool) -> Phase13ExecutionOutcome {
            if self.expected_delete_version(bypass) != model::Outcome::Allow {
                return Phase13ExecutionOutcome::Denied;
            }

            let current = if self.tracked_version_present && self.tracked_version_is_current {
                self.revealed_after_tracked_delete
            } else {
                self.current
            };

            Self::execution_outcome_from_read_policy(
                self.read_policy,
                current,
                false,
                Some(model::Outcome::Deny),
            )
        }

        fn current_read_outcome(self) -> model::Outcome {
            if self.read_policy == Phase13ReadPolicyState::AllowPrivate
                && self.current == Phase13CurrentState::Live
            {
                model::Outcome::Allow
            } else {
                model::Outcome::Deny
            }
        }

        fn execution_outcome_from_read_policy(
            read_policy: Phase13ReadPolicyState,
            current: Phase13CurrentState,
            delete_marker: bool,
            tracked_version_read: Option<model::Outcome>,
        ) -> Phase13ExecutionOutcome {
            let current_read = if read_policy == Phase13ReadPolicyState::AllowPrivate
                && current == Phase13CurrentState::Live
            {
                model::Outcome::Allow
            } else {
                model::Outcome::Deny
            };

            Phase13ExecutionOutcome::Applied {
                delete_marker,
                current_read,
                tracked_version_read,
            }
        }
    }
}

mod phase13_harness {
    use std::cell::{Cell, RefCell};

    use super::harness::{setup_coordinator, IdentityFixtures};
    use super::model::Outcome;
    use super::phase13_model::{
        Phase13ExecutionOutcome, Phase13ExecutionProbe, Phase13Mutation, Phase13Probe,
        Phase13ReadPolicyState,
    };
    use super::phase7_model::{DeleteObjectLockShape, DeletePolicyShape};
    use super::*;

    const PHASE13_KEY: &str = "phase13-key";

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum ClassifiedPhase13Result {
        Allow,
        Deny,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum ClassifiedPhase13ExecutionResult {
        Denied,
        Applied {
            delete_marker: bool,
            current_read: Outcome,
            tracked_version_read: Option<Outcome>,
        },
    }

    pub(super) struct Phase13Harness {
        _tmp: test_util::TempDir,
        coord: Coordinator,
        fixtures: IdentityFixtures,
        read_policy: Cell<Phase13ReadPolicyState>,
        delete_policy: Cell<DeletePolicyShape>,
        tracked_version_id: RefCell<Option<VersionId>>,
        tracked_version_present: Cell<bool>,
    }

    impl Phase13Harness {
        pub(super) fn new() -> Self {
            let tmp = test_util::tempdir();
            let coord = setup_coordinator(tmp.path());
            let fixtures = IdentityFixtures::new();
            Self {
                _tmp: tmp,
                coord,
                fixtures,
                read_policy: Cell::new(Phase13ReadPolicyState::None),
                delete_policy: Cell::new(DeletePolicyShape::None),
                tracked_version_id: RefCell::new(None),
                tracked_version_present: Cell::new(false),
            }
        }

        pub(super) fn prepare(&self, bucket: &str) {
            self.coord
                .create_bucket(&CreateBucketRequest {
                    name: trusted_bucket_name(bucket),
                    requester: Requester::authenticated(self.fixtures.owner_user.clone()),
                    namespace: BucketNamespace::Global,
                    acl: CreateBucketAcl::DefaultPrivate,
                    ownership: BucketObjectOwnership::BucketOwnerEnforced,
                    object_lock_enabled: true,
                })
                .unwrap_or_else(|err| panic!("failed to create phase 13 bucket: {err:?}"));
            self.coord
                .put_bucket_versioning(&PutBucketVersioningRequest {
                    bucket: BucketRequest::new(
                        trusted_bucket_name(bucket),
                        Requester::authenticated(self.fixtures.owner_user.clone()),
                        None,
                    ),
                    state: BucketVersioningState::Enabled,
                })
                .unwrap_or_else(|err| panic!("failed to enable versioning for phase 13: {err:?}"));
            let version_id = self.owner_put_current_version(bucket);
            self.tracked_version_id.replace(Some(version_id));
            self.tracked_version_present.set(true);
            self.read_policy.set(Phase13ReadPolicyState::None);
            self.delete_policy.set(DeletePolicyShape::None);
        }

        pub(super) fn apply_mutation(&self, bucket: &str, mutation: Phase13Mutation) {
            match mutation {
                Phase13Mutation::ReadPolicy(state) => {
                    self.read_policy.set(state);
                    self.apply_bucket_policy(bucket);
                }
                Phase13Mutation::DeletePolicy(policy) => {
                    self.delete_policy.set(policy);
                    self.apply_bucket_policy(bucket);
                }
                Phase13Mutation::OwnerPutCurrentVersion => {
                    let version_id = self.owner_put_current_version(bucket);
                    self.tracked_version_id.replace(Some(version_id));
                    self.tracked_version_present.set(true);
                }
                Phase13Mutation::OwnerDelete => {
                    self.coord
                        .delete_object(&DeleteObjectRequest {
                            object: ObjectVersionRequest::new(
                                trusted_bucket_name(bucket),
                                trusted_object_key(PHASE13_KEY),
                                None,
                                Requester::authenticated(self.fixtures.owner_user.clone()),
                                None,
                            ),
                            bypass_governance: false,
                            cond: NO_DELETE,
                        })
                        .unwrap_or_else(|err| {
                            panic!("failed to owner-delete current version for phase 13: {err:?}")
                        });
                }
                Phase13Mutation::OwnerDeleteTrackedVersion => {
                    let tracked_version_id = *self.tracked_version_id.borrow();
                    if let Some(version_id) = tracked_version_id {
                        match self.coord.delete_object(&DeleteObjectRequest {
                            object: ObjectVersionRequest::new(
                                trusted_bucket_name(bucket),
                                trusted_object_key(PHASE13_KEY),
                                Some(version_id),
                                Requester::authenticated(self.fixtures.owner_user.clone()),
                                None,
                            ),
                            bypass_governance: false,
                            cond: NO_DELETE,
                        }) {
                            Ok(_) => {
                                self.tracked_version_id.replace(None);
                                self.tracked_version_present.set(false);
                            }
                            Err(
                                ServerError::AccessDenied
                                | ServerError::ObjectLockProtectedAccessDenied
                                | ServerError::AnonymousApiAccessDenied,
                            ) => {}
                            Err(err) => {
                                panic!(
                                    "failed to owner-delete tracked version for phase 13: {err:?}"
                                )
                            }
                        }
                    }
                }
                Phase13Mutation::ApplyTrackedGovernanceRetention => {
                    self.apply_tracked_version_lock(bucket, DeleteObjectLockShape::Governance)
                }
                Phase13Mutation::ApplyTrackedComplianceRetention => {
                    self.apply_tracked_version_lock(bucket, DeleteObjectLockShape::Compliance)
                }
                Phase13Mutation::ApplyTrackedLegalHold => {
                    self.apply_tracked_version_lock(bucket, DeleteObjectLockShape::LegalHold)
                }
            }
        }

        pub(super) fn probe(&self, bucket: &str, probe: Phase13Probe) -> ClassifiedPhase13Result {
            let requester = Requester::authenticated(self.fixtures.cross_account.clone());
            let version_id = self
                .tracked_version_id
                .borrow()
                .unwrap_or_else(|| VersionId::from_u64(999_999));
            let result = match probe {
                Phase13Probe::Object => self
                    .coord
                    .get_object(&GetObjectRequest {
                        sse_customer: None,
                        object: ObjectVersionRequest::new(
                            trusted_bucket_name(bucket),
                            trusted_object_key(PHASE13_KEY),
                            None,
                            requester,
                            None,
                        ),
                        cond: NO_READ,
                    })
                    .and_then(|result| {
                        let _ = read_all_phase13_body(result.body)?;
                        Ok(())
                    }),
                Phase13Probe::Attributes => self
                    .coord
                    .get_object_attributes(&GetObjectAttributesRequest {
                        object: ObjectVersionRequest::new(
                            trusted_bucket_name(bucket),
                            trusted_object_key(PHASE13_KEY),
                            None,
                            requester,
                            None,
                        ),
                        cond: NO_READ,
                        want_parts: false,
                        part_number_marker: None,
                        max_parts: 0,
                        sse_customer: None,
                    })
                    .map(|_| ()),
                Phase13Probe::Delete => self
                    .coord
                    .authorize_delete_object(&DeleteObjectRequest {
                        object: ObjectVersionRequest::new(
                            trusted_bucket_name(bucket),
                            trusted_object_key(PHASE13_KEY),
                            None,
                            requester,
                            None,
                        ),
                        bypass_governance: false,
                        cond: NO_DELETE,
                    })
                    .map(|_| ()),
                Phase13Probe::DeleteTrackedVersion => self
                    .coord
                    .authorize_delete_object(&DeleteObjectRequest {
                        object: ObjectVersionRequest::new(
                            trusted_bucket_name(bucket),
                            trusted_object_key(PHASE13_KEY),
                            Some(version_id),
                            requester,
                            None,
                        ),
                        bypass_governance: false,
                        cond: NO_DELETE,
                    })
                    .map(|_| ()),
                Phase13Probe::DeleteTrackedVersionBypass => self
                    .coord
                    .authorize_delete_object(&DeleteObjectRequest {
                        object: ObjectVersionRequest::new(
                            trusted_bucket_name(bucket),
                            trusted_object_key(PHASE13_KEY),
                            Some(version_id),
                            requester,
                            None,
                        ),
                        bypass_governance: true,
                        cond: NO_DELETE,
                    })
                    .map(|_| ()),
            };
            classify_phase13(result)
        }

        pub(super) fn execution_probe(
            &self,
            bucket: &str,
            probe: Phase13ExecutionProbe,
        ) -> ClassifiedPhase13ExecutionResult {
            let requester = Requester::authenticated(self.fixtures.cross_account.clone());
            let version_id = self
                .tracked_version_id
                .borrow()
                .unwrap_or_else(|| VersionId::from_u64(999_999));
            let (delete_result, read_current, read_tracked_version) = match probe {
                Phase13ExecutionProbe::Current => (
                    self.coord.delete_object(&DeleteObjectRequest {
                        object: ObjectVersionRequest::new(
                            trusted_bucket_name(bucket),
                            trusted_object_key(PHASE13_KEY),
                            None,
                            requester.clone(),
                            None,
                        ),
                        bypass_governance: false,
                        cond: NO_DELETE,
                    }),
                    true,
                    None,
                ),
                Phase13ExecutionProbe::Version => (
                    self.coord.delete_object(&DeleteObjectRequest {
                        object: ObjectVersionRequest::new(
                            trusted_bucket_name(bucket),
                            trusted_object_key(PHASE13_KEY),
                            Some(version_id),
                            requester.clone(),
                            None,
                        ),
                        bypass_governance: false,
                        cond: NO_DELETE,
                    }),
                    true,
                    Some(version_id),
                ),
                Phase13ExecutionProbe::VersionBypass => (
                    self.coord.delete_object(&DeleteObjectRequest {
                        object: ObjectVersionRequest::new(
                            trusted_bucket_name(bucket),
                            trusted_object_key(PHASE13_KEY),
                            Some(version_id),
                            requester.clone(),
                            None,
                        ),
                        bypass_governance: true,
                        cond: NO_DELETE,
                    }),
                    true,
                    Some(version_id),
                ),
            };

            let deleted = match delete_result {
                Ok(result) => result,
                Err(
                    ServerError::AccessDenied
                    | ServerError::ObjectLockProtectedAccessDenied
                    | ServerError::AnonymousApiAccessDenied,
                ) => {
                    return ClassifiedPhase13ExecutionResult::Denied;
                }
                Err(err) => panic!("unexpected phase 13 delete execution result: {err:?}"),
            };

            let tracked_requester = requester.clone();
            let read_result = if read_current {
                self.coord
                    .get_object(&GetObjectRequest {
                        sse_customer: None,
                        object: ObjectVersionRequest::new(
                            trusted_bucket_name(bucket),
                            trusted_object_key(PHASE13_KEY),
                            None,
                            requester,
                            None,
                        ),
                        cond: NO_READ,
                    })
                    .and_then(|result| {
                        let _ = read_all_phase13_body(result.body)?;
                        Ok(())
                    })
            } else {
                Ok(())
            };

            let current_read = to_phase13_outcome(classify_phase13(read_result));
            let tracked_version_read = read_tracked_version.map(|tracked_version_id| {
                to_phase13_outcome(classify_phase13(
                    self.coord
                        .get_object(&GetObjectRequest {
                            sse_customer: None,
                            object: ObjectVersionRequest::new(
                                trusted_bucket_name(bucket),
                                trusted_object_key(PHASE13_KEY),
                                Some(tracked_version_id),
                                tracked_requester.clone(),
                                None,
                            ),
                            cond: NO_READ,
                        })
                        .and_then(|result| {
                            let _ = read_all_phase13_body(result.body)?;
                            Ok(())
                        }),
                ))
            });

            ClassifiedPhase13ExecutionResult::Applied {
                delete_marker: deleted.delete_marker,
                current_read,
                tracked_version_read,
            }
        }

        fn owner_put_current_version(&self, bucket: &str) -> VersionId {
            test_helpers::put_object(
                &self.coord,
                &PutObjectRequest {
                    encryption: WriteEncryptionRequest::none(),
                    policy_context: PutObjectPolicyContext::default(),
                    object_lock: ObjectLockState::default(),
                    object: ObjectRequest::new(
                        trusted_bucket_name(bucket),
                        trusted_object_key(PHASE13_KEY),
                        Requester::authenticated(self.fixtures.owner_user.clone()),
                        None,
                    ),
                    data: b"phase13",
                    metadata: &MetadataBlob::new(),
                    system_metadata: &SystemMetadata::EMPTY,
                    tags: None,
                    cond: NO_WRITE,
                    acl: NO_PUT_OBJECT_ACL.into(),
                },
            )
            .unwrap_or_else(|err| panic!("failed to owner-put phase 13 object: {err:?}"))
            .version_id
        }

        fn apply_tracked_version_lock(&self, bucket: &str, lock: DeleteObjectLockShape) {
            if !self.tracked_version_present.get() {
                return;
            }
            let Some(version_id) = *self.tracked_version_id.borrow() else {
                return;
            };
            match lock {
                DeleteObjectLockShape::None => {}
                DeleteObjectLockShape::Governance | DeleteObjectLockShape::Compliance => match self
                    .coord
                    .put_object_retention(&PutObjectRetentionRequest {
                        object: ObjectVersionRequest::new(
                            trusted_bucket_name(bucket),
                            trusted_object_key(PHASE13_KEY),
                            Some(version_id),
                            Requester::authenticated(self.fixtures.owner_user.clone()),
                            None,
                        ),
                        retention: ObjectRetention {
                            mode: match lock {
                                DeleteObjectLockShape::Governance => ObjectLockMode::Governance,
                                DeleteObjectLockShape::Compliance => ObjectLockMode::Compliance,
                                DeleteObjectLockShape::None | DeleteObjectLockShape::LegalHold => {
                                    unreachable!()
                                }
                            },
                            retain_until_unix_seconds: Coordinator::current_unix_seconds()
                                .expect("phase 13 current time")
                                + 3600,
                        },
                        bypass_governance: false,
                    })
                    .map(|_| ())
                {
                    Ok(()) => {}
                    Err(
                        ServerError::AccessDenied
                        | ServerError::ObjectLockProtectedAccessDenied
                        | ServerError::AnonymousApiAccessDenied
                        | ServerError::VersionNotFound { .. }
                        | ServerError::MethodNotAllowed,
                    ) => {}
                    Err(err) => {
                        panic!("failed to apply tracked retention in phase 13: {err:?}")
                    }
                },
                DeleteObjectLockShape::LegalHold => {
                    match self
                        .coord
                        .put_object_legal_hold(&PutObjectLegalHoldRequest {
                            object: ObjectVersionRequest::new(
                                trusted_bucket_name(bucket),
                                trusted_object_key(PHASE13_KEY),
                                Some(version_id),
                                Requester::authenticated(self.fixtures.owner_user.clone()),
                                None,
                            ),
                            legal_hold: LegalHoldStatus::On,
                        }) {
                        Ok(_) => {}
                        Err(
                            ServerError::AccessDenied
                            | ServerError::ObjectLockProtectedAccessDenied
                            | ServerError::AnonymousApiAccessDenied
                            | ServerError::VersionNotFound { .. }
                            | ServerError::MethodNotAllowed,
                        ) => {}
                        Err(err) => {
                            panic!("failed to apply tracked legal hold in phase 13: {err:?}")
                        }
                    }
                }
            }
        }

        fn apply_bucket_policy(&self, bucket: &str) {
            let Some(document) = phase13_policy_document(
                &self.fixtures,
                bucket,
                self.read_policy.get(),
                self.delete_policy.get(),
            ) else {
                self.coord
                    .delete_bucket_policy(&BucketRequest::new(
                        trusted_bucket_name(bucket),
                        Requester::authenticated(self.fixtures.owner_user.clone()),
                        None,
                    ))
                    .unwrap_or_else(|err| {
                        panic!("failed to delete phase 13 bucket policy: {err:?}")
                    });
                return;
            };
            self.coord
                .put_bucket_policy(&PutBucketPolicyRequest {
                    bucket: BucketRequest::new(
                        trusted_bucket_name(bucket),
                        Requester::authenticated(self.fixtures.owner_user.clone()),
                        None,
                    ),
                    config: &document,
                    confirm_remove_self_bucket_access: false,
                })
                .unwrap_or_else(|err| panic!("failed to put phase 13 bucket policy: {err:?}"));
        }
    }

    pub(super) fn to_phase13_outcome(result: ClassifiedPhase13Result) -> Outcome {
        match result {
            ClassifiedPhase13Result::Allow => Outcome::Allow,
            ClassifiedPhase13Result::Deny => Outcome::Deny,
        }
    }

    pub(super) fn to_phase13_execution_outcome(
        result: ClassifiedPhase13ExecutionResult,
    ) -> Phase13ExecutionOutcome {
        match result {
            ClassifiedPhase13ExecutionResult::Denied => Phase13ExecutionOutcome::Denied,
            ClassifiedPhase13ExecutionResult::Applied {
                delete_marker,
                current_read,
                tracked_version_read,
            } => Phase13ExecutionOutcome::Applied {
                delete_marker,
                current_read,
                tracked_version_read,
            },
        }
    }

    fn phase13_policy_document(
        fixtures: &IdentityFixtures,
        bucket: &str,
        read_policy: Phase13ReadPolicyState,
        delete_policy: DeletePolicyShape,
    ) -> Option<String> {
        let principal = fixtures.cross_account.principal();
        let resource = format!("arn:aws:s3:::{bucket}/{PHASE13_KEY}");
        let mut statements = Vec::new();
        if read_policy == Phase13ReadPolicyState::AllowPrivate {
            statements.push(format!(
                r#"{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":["s3:GetObject","s3:GetObjectAttributes"],"Resource":"{resource}"}}"#
            ));
        }
        match delete_policy {
            DeletePolicyShape::None => {}
            DeletePolicyShape::AllowDeleteObject => statements.push(format!(
                r#"{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":"s3:DeleteObject","Resource":"{resource}"}}"#
            )),
            DeletePolicyShape::AllowDeleteVersion => statements.push(format!(
                r#"{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":"s3:DeleteObjectVersion","Resource":"{resource}"}}"#
            )),
            DeletePolicyShape::AllowDeleteVersionAndBypass => statements.push(format!(
                r#"{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":["s3:DeleteObjectVersion","s3:BypassGovernanceRetention"],"Resource":"{resource}"}}"#
            )),
            DeletePolicyShape::DenyBypass => statements.push(format!(
                r#"{{"Effect":"Deny","Principal":{{"AWS":"{principal}"}},"Action":"s3:BypassGovernanceRetention","Resource":"{resource}"}}"#
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

    fn classify_phase13(result: Result<(), ServerError>) -> ClassifiedPhase13Result {
        match result {
            Ok(()) => ClassifiedPhase13Result::Allow,
            Err(
                ServerError::AccessDenied
                | ServerError::ObjectLockProtectedAccessDenied
                | ServerError::AnonymousApiAccessDenied
                | ServerError::DeleteMarkerHit { .. }
                | ServerError::ObjectNotFound { .. }
                | ServerError::VersionNotFound { .. },
            ) => ClassifiedPhase13Result::Deny,
            Err(other) => panic!("unexpected phase 13 probe result: {other:?}"),
        }
    }

    fn read_all_phase13_body(mut body: ReadHandle) -> Result<Vec<u8>, ServerError> {
        let mut out = Vec::new();
        while let Some(chunk) = body.next_chunk(INTERNAL_SEGMENT_SIZE)? {
            out.extend_from_slice(&chunk);
        }
        Ok(out)
    }
}

mod phase14_model {
    use super::*;

    const DISABLED_VERSIONING_TARGETS: [BucketVersioningState; 2] = [
        BucketVersioningState::Disabled,
        BucketVersioningState::Enabled,
    ];
    const ENABLED_VERSIONING_TARGETS: [BucketVersioningState; 2] = [
        BucketVersioningState::Enabled,
        BucketVersioningState::Suspended,
    ];
    const SUSPENDED_VERSIONING_TARGETS: [BucketVersioningState; 2] = [
        BucketVersioningState::Enabled,
        BucketVersioningState::Suspended,
    ];

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum Phase14ReadPolicyState {
        None,
        AllowPrivate,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum Phase14DeletePolicyState {
        None,
        AllowDeleteObject,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum Phase14CurrentState {
        Missing,
        Live,
        DeleteMarker,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum Phase14Mutation {
        SetVersioning(BucketVersioningState),
        ReadPolicy(Phase14ReadPolicyState),
        DeletePolicy(Phase14DeletePolicyState),
        OwnerPutCurrent,
        OwnerDelete,
    }

    impl fmt::Display for Phase14Mutation {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::SetVersioning(state) => write!(f, "set-versioning={state:?}"),
                Self::ReadPolicy(state) => write!(f, "set-read-policy={state}"),
                Self::DeletePolicy(state) => write!(f, "set-delete-policy={state}"),
                Self::OwnerPutCurrent => f.write_str("owner-put-current"),
                Self::OwnerDelete => f.write_str("owner-delete-current"),
            }
        }
    }

    impl fmt::Display for Phase14ReadPolicyState {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::None => f.write_str("none"),
                Self::AllowPrivate => f.write_str("allow-read-private"),
            }
        }
    }

    impl fmt::Display for Phase14DeletePolicyState {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::None => f.write_str("none"),
                Self::AllowDeleteObject => f.write_str("allow-delete-object"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum Phase14Probe {
        Object,
        Attributes,
        Delete,
    }

    impl fmt::Display for Phase14Probe {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Object => f.write_str("get-object-current"),
                Self::Attributes => f.write_str("get-object-attributes-current"),
                Self::Delete => f.write_str("delete-current"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum Phase14ExecutionOutcome {
        Denied,
        Applied {
            delete_marker: bool,
            current_read: model::Outcome,
        },
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct Phase14State {
        pub(super) versioning: BucketVersioningState,
        pub(super) read_policy: Phase14ReadPolicyState,
        pub(super) delete_policy: Phase14DeletePolicyState,
        pub(super) current: Phase14CurrentState,
    }

    impl Phase14State {
        pub(super) fn new() -> Self {
            Self {
                versioning: BucketVersioningState::Disabled,
                read_policy: Phase14ReadPolicyState::None,
                delete_policy: Phase14DeletePolicyState::None,
                current: Phase14CurrentState::Missing,
            }
        }

        pub(super) fn legal_versioning_targets(&self) -> &'static [BucketVersioningState] {
            match self.versioning {
                BucketVersioningState::Disabled => &DISABLED_VERSIONING_TARGETS,
                BucketVersioningState::Enabled => &ENABLED_VERSIONING_TARGETS,
                BucketVersioningState::Suspended => &SUSPENDED_VERSIONING_TARGETS,
            }
        }

        pub(super) fn apply(&mut self, mutation: Phase14Mutation) {
            match mutation {
                Phase14Mutation::SetVersioning(state) => {
                    assert!(self.legal_versioning_targets().contains(&state));
                    self.versioning = state;
                }
                Phase14Mutation::ReadPolicy(state) => self.read_policy = state,
                Phase14Mutation::DeletePolicy(state) => self.delete_policy = state,
                Phase14Mutation::OwnerPutCurrent => self.current = Phase14CurrentState::Live,
                Phase14Mutation::OwnerDelete => {
                    self.current = match self.versioning {
                        BucketVersioningState::Disabled => Phase14CurrentState::Missing,
                        BucketVersioningState::Enabled | BucketVersioningState::Suspended => {
                            Phase14CurrentState::DeleteMarker
                        }
                    };
                }
            }
        }

        pub(super) fn expected_outcome(self, probe: Phase14Probe) -> model::Outcome {
            match probe {
                Phase14Probe::Object | Phase14Probe::Attributes => self.current_read_outcome(),
                Phase14Probe::Delete => {
                    if self.delete_policy == Phase14DeletePolicyState::AllowDeleteObject {
                        model::Outcome::Allow
                    } else {
                        model::Outcome::Deny
                    }
                }
            }
        }

        pub(super) fn expected_execution_outcome(self) -> Phase14ExecutionOutcome {
            if self.delete_policy != Phase14DeletePolicyState::AllowDeleteObject {
                return Phase14ExecutionOutcome::Denied;
            }
            let delete_marker = match self.versioning {
                BucketVersioningState::Disabled => false,
                BucketVersioningState::Enabled | BucketVersioningState::Suspended => true,
            };
            Phase14ExecutionOutcome::Applied {
                delete_marker,
                current_read: model::Outcome::Deny,
            }
        }

        fn current_read_outcome(self) -> model::Outcome {
            if self.read_policy == Phase14ReadPolicyState::AllowPrivate
                && self.current == Phase14CurrentState::Live
            {
                model::Outcome::Allow
            } else {
                model::Outcome::Deny
            }
        }
    }
}

mod phase14_harness {
    use std::cell::Cell;
    use std::thread;
    use std::time::Duration;

    use super::harness::{setup_coordinator, IdentityFixtures};
    use super::model::Outcome;
    use super::phase14_model::{
        Phase14DeletePolicyState, Phase14ExecutionOutcome, Phase14Mutation, Phase14Probe,
        Phase14ReadPolicyState,
    };
    use super::*;

    const PHASE14_KEY: &str = "phase14-key";

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum ClassifiedPhase14Result {
        Allow,
        Deny,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum ClassifiedPhase14ExecutionResult {
        Denied,
        Applied {
            delete_marker: bool,
            current_read: Outcome,
        },
    }

    pub(super) struct Phase14Harness {
        _tmp: test_util::TempDir,
        coord: Coordinator,
        fixtures: IdentityFixtures,
        read_policy: Cell<Phase14ReadPolicyState>,
        delete_policy: Cell<Phase14DeletePolicyState>,
    }

    impl Phase14Harness {
        pub(super) fn new() -> Self {
            let tmp = test_util::tempdir();
            let coord = setup_coordinator(tmp.path());
            let fixtures = IdentityFixtures::new();
            Self {
                _tmp: tmp,
                coord,
                fixtures,
                read_policy: Cell::new(Phase14ReadPolicyState::None),
                delete_policy: Cell::new(Phase14DeletePolicyState::None),
            }
        }

        pub(super) fn prepare(&self, bucket: &str) {
            self.coord
                .create_bucket(&CreateBucketRequest {
                    name: trusted_bucket_name(bucket),
                    requester: Requester::authenticated(self.fixtures.owner_user.clone()),
                    namespace: BucketNamespace::Global,
                    acl: CreateBucketAcl::DefaultPrivate,
                    ownership: BucketObjectOwnership::BucketOwnerEnforced,
                    object_lock_enabled: false,
                })
                .unwrap_or_else(|err| panic!("failed to create phase 14 bucket: {err:?}"));
            self.read_policy.set(Phase14ReadPolicyState::None);
            self.delete_policy.set(Phase14DeletePolicyState::None);
        }

        pub(super) fn apply_mutation(&self, bucket: &str, mutation: Phase14Mutation) {
            match mutation {
                Phase14Mutation::SetVersioning(state) => {
                    retry_phase14_setup_operation(
                        format!("failed to set phase 14 bucket versioning to {state:?}"),
                        || {
                            self.coord
                                .put_bucket_versioning(&PutBucketVersioningRequest {
                                    bucket: BucketRequest::new(
                                        trusted_bucket_name(bucket),
                                        Requester::authenticated(self.fixtures.owner_user.clone()),
                                        None,
                                    ),
                                    state,
                                })
                        },
                    );
                }
                Phase14Mutation::ReadPolicy(state) => {
                    self.read_policy.set(state);
                    self.apply_bucket_policy(bucket);
                }
                Phase14Mutation::DeletePolicy(state) => {
                    self.delete_policy.set(state);
                    self.apply_bucket_policy(bucket);
                }
                Phase14Mutation::OwnerPutCurrent => {
                    retry_phase14_setup_operation("failed to owner-put phase 14 object", || {
                        test_helpers::put_object(
                            &self.coord,
                            &PutObjectRequest {
                                encryption: WriteEncryptionRequest::none(),
                                policy_context: PutObjectPolicyContext::default(),
                                object_lock: ObjectLockState::default(),
                                object: ObjectRequest::new(
                                    trusted_bucket_name(bucket),
                                    trusted_object_key(PHASE14_KEY),
                                    Requester::authenticated(self.fixtures.owner_user.clone()),
                                    None,
                                ),
                                data: b"phase14",
                                metadata: &MetadataBlob::new(),
                                system_metadata: &SystemMetadata::EMPTY,
                                tags: None,
                                cond: NO_WRITE,
                                acl: NO_PUT_OBJECT_ACL.into(),
                            },
                        )
                    });
                }
                Phase14Mutation::OwnerDelete => {
                    retry_phase14_setup_operation(
                        "failed to owner-delete current object for phase 14",
                        || {
                            self.coord.delete_object(&DeleteObjectRequest {
                                object: ObjectVersionRequest::new(
                                    trusted_bucket_name(bucket),
                                    trusted_object_key(PHASE14_KEY),
                                    None,
                                    Requester::authenticated(self.fixtures.owner_user.clone()),
                                    None,
                                ),
                                bypass_governance: false,
                                cond: NO_DELETE,
                            })
                        },
                    );
                }
            }
        }

        pub(super) fn probe(&self, bucket: &str, probe: Phase14Probe) -> ClassifiedPhase14Result {
            let requester = Requester::authenticated(self.fixtures.cross_account.clone());
            let result = match probe {
                Phase14Probe::Object => self
                    .coord
                    .get_object(&GetObjectRequest {
                        sse_customer: None,
                        object: ObjectVersionRequest::new(
                            trusted_bucket_name(bucket),
                            trusted_object_key(PHASE14_KEY),
                            None,
                            requester,
                            None,
                        ),
                        cond: NO_READ,
                    })
                    .and_then(|result| {
                        let _ = read_all_phase14_body(result.body)?;
                        Ok(())
                    }),
                Phase14Probe::Attributes => self
                    .coord
                    .get_object_attributes(&GetObjectAttributesRequest {
                        object: ObjectVersionRequest::new(
                            trusted_bucket_name(bucket),
                            trusted_object_key(PHASE14_KEY),
                            None,
                            requester,
                            None,
                        ),
                        cond: NO_READ,
                        want_parts: false,
                        part_number_marker: None,
                        max_parts: 0,
                        sse_customer: None,
                    })
                    .map(|_| ()),
                Phase14Probe::Delete => self
                    .coord
                    .authorize_delete_object(&DeleteObjectRequest {
                        object: ObjectVersionRequest::new(
                            trusted_bucket_name(bucket),
                            trusted_object_key(PHASE14_KEY),
                            None,
                            requester,
                            None,
                        ),
                        bypass_governance: false,
                        cond: NO_DELETE,
                    })
                    .map(|_| ()),
            };
            classify_phase14(result)
        }

        pub(super) fn execution_probe(&self, bucket: &str) -> ClassifiedPhase14ExecutionResult {
            let requester = Requester::authenticated(self.fixtures.cross_account.clone());
            let deleted = match self.coord.delete_object(&DeleteObjectRequest {
                object: ObjectVersionRequest::new(
                    trusted_bucket_name(bucket),
                    trusted_object_key(PHASE14_KEY),
                    None,
                    requester.clone(),
                    None,
                ),
                bypass_governance: false,
                cond: NO_DELETE,
            }) {
                Ok(result) => result,
                Err(
                    ServerError::AccessDenied
                    | ServerError::ObjectLockProtectedAccessDenied
                    | ServerError::AnonymousApiAccessDenied,
                ) => {
                    return ClassifiedPhase14ExecutionResult::Denied;
                }
                Err(err) => panic!("unexpected phase 14 delete execution result: {err:?}"),
            };

            let current_read = to_phase14_outcome(classify_phase14(
                self.coord
                    .get_object(&GetObjectRequest {
                        sse_customer: None,
                        object: ObjectVersionRequest::new(
                            trusted_bucket_name(bucket),
                            trusted_object_key(PHASE14_KEY),
                            None,
                            requester,
                            None,
                        ),
                        cond: NO_READ,
                    })
                    .and_then(|result| {
                        let _ = read_all_phase14_body(result.body)?;
                        Ok(())
                    }),
            ));

            ClassifiedPhase14ExecutionResult::Applied {
                delete_marker: deleted.delete_marker,
                current_read,
            }
        }

        fn apply_bucket_policy(&self, bucket: &str) {
            let Some(document) = phase14_policy_document(
                &self.fixtures,
                bucket,
                self.read_policy.get(),
                self.delete_policy.get(),
            ) else {
                retry_phase14_setup_operation("failed to delete phase 14 bucket policy", || {
                    self.coord.delete_bucket_policy(&BucketRequest::new(
                        trusted_bucket_name(bucket),
                        Requester::authenticated(self.fixtures.owner_user.clone()),
                        None,
                    ))
                });
                return;
            };
            retry_phase14_setup_operation("failed to put phase 14 bucket policy", || {
                self.coord.put_bucket_policy(&PutBucketPolicyRequest {
                    bucket: BucketRequest::new(
                        trusted_bucket_name(bucket),
                        Requester::authenticated(self.fixtures.owner_user.clone()),
                        None,
                    ),
                    config: &document,
                    confirm_remove_self_bucket_access: false,
                })
            });
        }
    }

    fn retry_phase14_setup_operation<T>(
        context: impl AsRef<str>,
        mut operation: impl FnMut() -> Result<T, ServerError>,
    ) -> T {
        const MAX_ATTEMPTS: usize = 20;

        for attempt in 0..MAX_ATTEMPTS {
            match operation() {
                Ok(result) => return result,
                Err(ServerError::OperationAborted) if attempt + 1 < MAX_ATTEMPTS => {
                    thread::sleep(Duration::from_millis(1));
                }
                Err(err) => panic!("{}: {err:?}", context.as_ref()),
            }
        }
        unreachable!("retry loop must return on final attempt")
    }

    pub(super) fn to_phase14_outcome(result: ClassifiedPhase14Result) -> Outcome {
        match result {
            ClassifiedPhase14Result::Allow => Outcome::Allow,
            ClassifiedPhase14Result::Deny => Outcome::Deny,
        }
    }

    pub(super) fn to_phase14_execution_outcome(
        result: ClassifiedPhase14ExecutionResult,
    ) -> Phase14ExecutionOutcome {
        match result {
            ClassifiedPhase14ExecutionResult::Denied => Phase14ExecutionOutcome::Denied,
            ClassifiedPhase14ExecutionResult::Applied {
                delete_marker,
                current_read,
            } => Phase14ExecutionOutcome::Applied {
                delete_marker,
                current_read,
            },
        }
    }

    fn phase14_policy_document(
        fixtures: &IdentityFixtures,
        bucket: &str,
        read_policy: Phase14ReadPolicyState,
        delete_policy: Phase14DeletePolicyState,
    ) -> Option<String> {
        let principal = fixtures.cross_account.principal();
        let resource = format!("arn:aws:s3:::{bucket}/{PHASE14_KEY}");
        let mut statements = Vec::new();
        if read_policy == Phase14ReadPolicyState::AllowPrivate {
            statements.push(format!(
                r#"{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":["s3:GetObject","s3:GetObjectAttributes"],"Resource":"{resource}"}}"#
            ));
        }
        if delete_policy == Phase14DeletePolicyState::AllowDeleteObject {
            statements.push(format!(
                r#"{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":"s3:DeleteObject","Resource":"{resource}"}}"#
            ));
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

    fn classify_phase14(result: Result<(), ServerError>) -> ClassifiedPhase14Result {
        match result {
            Ok(()) => ClassifiedPhase14Result::Allow,
            Err(
                ServerError::AccessDenied
                | ServerError::ObjectLockProtectedAccessDenied
                | ServerError::AnonymousApiAccessDenied
                | ServerError::DeleteMarkerHit { .. }
                | ServerError::ObjectNotFound { .. },
            ) => ClassifiedPhase14Result::Deny,
            Err(err) => panic!("unexpected phase 14 result: {err:?}"),
        }
    }

    fn read_all_phase14_body(mut body: ReadHandle) -> Result<Vec<u8>, ServerError> {
        let mut out = Vec::new();
        while let Some(chunk) = body.next_chunk(INTERNAL_SEGMENT_SIZE)? {
            out.extend_from_slice(&chunk);
        }
        Ok(out)
    }
}

use harness::{bucket_name_for, to_existing_outcome, to_missing_outcome, MatrixHarness};
use model::{Action, MissingScenario, Scenario};
use phase10_harness::Phase10Harness;
use phase11_harness::{phase11_bucket_name_for, Phase11Harness};
use phase11_model::{BucketMetaAction, BucketMetaScenario};
use phase12_harness::{to_boe_trace_outcome, Phase12Harness};
use phase12_model::{
    BoeTraceBucketTagState, BoeTraceMutation, BoeTracePolicyState, BoeTraceProbe, BoeTraceState,
};
use phase13_harness::{to_phase13_execution_outcome, to_phase13_outcome, Phase13Harness};
use phase13_model::{
    Phase13ExecutionProbe, Phase13Mutation, Phase13Probe, Phase13ReadPolicyState, Phase13State,
};
use phase14_harness::{to_phase14_execution_outcome, to_phase14_outcome, Phase14Harness};
use phase14_model::{
    Phase14DeletePolicyState, Phase14Mutation, Phase14Probe, Phase14ReadPolicyState, Phase14State,
};
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
use phase7_model::{DeletePolicyShape, DeleteScenario, ObjectLockScenario};
use phase7a_harness::{
    phase7a_management_bucket_name_for, phase7a_write_bucket_name_for, Phase7aHarness,
};
use phase7a_model::{MultipartManagementScenario, MultipartWriteScenario};
use phase8_harness::Phase8Harness;
use phase9_harness::{phase9_bucket_name_for, Phase9Harness};
use phase9_model::{BucketAction, BucketActionScenario};

#[test]
fn authz_model_modern_boe_get_object_existing_matrix() {
    run_modern_boe_existing_matrix(Action::GetObject);
}

#[test]
fn authz_model_boe_get_object_fast_path_matches_snapshot_evaluator() {
    run_boe_read_fast_path_invariant(Action::GetObject);
}

#[test]
fn authz_model_boe_bucket_tag_policy_snapshot_loader_preloads_tags() {
    MatrixHarness::new()
        .run_boe_bucket_tag_snapshot_loader_invariant("authz-modern-boe-bucket-tags-snapshot");
}

#[test]
fn authz_model_boe_bucket_tag_policy_fast_path_preserves_tags() {
    MatrixHarness::new()
        .run_boe_bucket_tag_fast_path_loader_invariant("authz-modern-boe-bucket-tags-fast");
}

#[test]
fn authz_model_boe_bucket_tag_policy_put_object_path_preloads_tags() {
    MatrixHarness::new()
        .run_boe_bucket_tag_put_object_invariant("authz-modern-boe-bucket-tags-put");
}

#[test]
fn authz_model_boe_bucket_tag_policy_delete_path_preloads_tags() {
    MatrixHarness::new().run_boe_bucket_tag_delete_invariant("authz-modern-boe-bucket-tags-delete");
}

macro_rules! modern_boe_get_object_attributes_existing_matrix_shards {
    ($($name:ident => $index:expr),* $(,)?) => {
        $(
            #[test]
            fn $name() {
                run_modern_boe_existing_matrix_shard(
                    Action::GetObjectAttributes,
                    $index,
                    8,
                );
            }
        )*
    };
}

macro_rules! boe_get_object_attributes_fast_path_invariant_shards {
    ($($name:ident => $index:expr),* $(,)?) => {
        $(
            #[test]
            fn $name() {
                run_boe_read_fast_path_invariant_shard(
                    Action::GetObjectAttributes,
                    $index,
                    4,
                );
            }
        )*
    };
}

modern_boe_get_object_attributes_existing_matrix_shards! {
    authz_model_modern_boe_get_object_attributes_existing_matrix_shard_00 => 0,
    authz_model_modern_boe_get_object_attributes_existing_matrix_shard_01 => 1,
    authz_model_modern_boe_get_object_attributes_existing_matrix_shard_02 => 2,
    authz_model_modern_boe_get_object_attributes_existing_matrix_shard_03 => 3,
    authz_model_modern_boe_get_object_attributes_existing_matrix_shard_04 => 4,
    authz_model_modern_boe_get_object_attributes_existing_matrix_shard_05 => 5,
    authz_model_modern_boe_get_object_attributes_existing_matrix_shard_06 => 6,
    authz_model_modern_boe_get_object_attributes_existing_matrix_shard_07 => 7,
}

boe_get_object_attributes_fast_path_invariant_shards! {
    authz_model_boe_get_object_attributes_fast_path_matches_snapshot_evaluator_shard_00 => 0,
    authz_model_boe_get_object_attributes_fast_path_matches_snapshot_evaluator_shard_01 => 1,
    authz_model_boe_get_object_attributes_fast_path_matches_snapshot_evaluator_shard_02 => 2,
    authz_model_boe_get_object_attributes_fast_path_matches_snapshot_evaluator_shard_03 => 3,
}

#[test]
fn authz_model_phase1_get_object_existing_matrix() {
    run_existing_matrix_without_cross_account_owner("phase 1", Action::GetObject);
}

#[test]
fn authz_model_phase1_get_object_existing_foreign_owned_matrix() {
    run_existing_matrix_with_only_cross_account_owner("phase 1", Action::GetObject);
}

macro_rules! phase1_get_object_attributes_existing_matrix_shards {
    ($($name:ident => $index:expr),* $(,)?) => {
        $(
            #[test]
            fn $name() {
                run_existing_matrix_without_cross_account_owner_shard(
                    "phase 1",
                    Action::GetObjectAttributes,
                    $index,
                    8,
                );
            }
        )*
    };
}

phase1_get_object_attributes_existing_matrix_shards! {
    authz_model_phase1_get_object_attributes_existing_matrix_shard_00 => 0,
    authz_model_phase1_get_object_attributes_existing_matrix_shard_01 => 1,
    authz_model_phase1_get_object_attributes_existing_matrix_shard_02 => 2,
    authz_model_phase1_get_object_attributes_existing_matrix_shard_03 => 3,
    authz_model_phase1_get_object_attributes_existing_matrix_shard_04 => 4,
    authz_model_phase1_get_object_attributes_existing_matrix_shard_05 => 5,
    authz_model_phase1_get_object_attributes_existing_matrix_shard_06 => 6,
    authz_model_phase1_get_object_attributes_existing_matrix_shard_07 => 7,
}

#[test]
fn authz_model_phase1_get_object_attributes_existing_foreign_owned_matrix() {
    run_existing_matrix_with_only_cross_account_owner("phase 1", Action::GetObjectAttributes);
}

#[test]
fn authz_model_phase2_get_object_acl_existing_matrix() {
    run_existing_matrix_without_cross_account_owner("phase 2", Action::GetObjectAcl);
}

#[test]
fn authz_model_phase2_get_object_acl_existing_foreign_owned_matrix() {
    run_existing_matrix_with_only_cross_account_owner("phase 2", Action::GetObjectAcl);
}

#[test]
fn authz_model_phase2_get_object_tagging_existing_matrix() {
    run_existing_matrix_without_cross_account_owner("phase 2", Action::GetObjectTagging);
}

#[test]
fn authz_model_phase2_get_object_tagging_existing_foreign_owned_matrix() {
    run_existing_matrix_with_only_cross_account_owner("phase 2", Action::GetObjectTagging);
}

#[test]
fn authz_model_phase2_put_object_tagging_existing_matrix() {
    run_existing_matrix_without_cross_account_owner("phase 2", Action::PutObjectTagging);
}

#[test]
fn authz_model_phase2_put_object_tagging_existing_foreign_owned_matrix() {
    run_existing_matrix_with_only_cross_account_owner("phase 2", Action::PutObjectTagging);
}

#[test]
fn authz_model_phase2_delete_object_tagging_existing_matrix() {
    run_existing_matrix_without_cross_account_owner("phase 2", Action::DeleteObjectTagging);
}

#[test]
fn authz_model_phase2_delete_object_tagging_existing_foreign_owned_matrix() {
    run_existing_matrix_with_only_cross_account_owner("phase 2", Action::DeleteObjectTagging);
}

#[test]
fn authz_model_phase3_get_object_missing_matrix() {
    run_missing_matrix("phase 3", Action::GetObject);
}

#[test]
fn authz_model_modern_boe_get_object_missing_matrix() {
    run_boe_missing_matrix(Action::GetObject);
}

macro_rules! phase3_get_object_attributes_missing_matrix_shards {
    ($($name:ident => $index:expr),* $(,)?) => {
        $(
            #[test]
            fn $name() {
                run_missing_matrix_shard("phase 3", Action::GetObjectAttributes, $index, 32);
            }
        )*
    };
}

macro_rules! boe_get_object_attributes_missing_matrix_shards {
    ($($name:ident => $index:expr),* $(,)?) => {
        $(
            #[test]
            fn $name() {
                run_boe_missing_matrix_shard(Action::GetObjectAttributes, $index, 8);
            }
        )*
    };
}

phase3_get_object_attributes_missing_matrix_shards!(
    authz_model_phase3_get_object_attributes_missing_matrix_shard_00 => 0,
    authz_model_phase3_get_object_attributes_missing_matrix_shard_01 => 1,
    authz_model_phase3_get_object_attributes_missing_matrix_shard_02 => 2,
    authz_model_phase3_get_object_attributes_missing_matrix_shard_03 => 3,
    authz_model_phase3_get_object_attributes_missing_matrix_shard_04 => 4,
    authz_model_phase3_get_object_attributes_missing_matrix_shard_05 => 5,
    authz_model_phase3_get_object_attributes_missing_matrix_shard_06 => 6,
    authz_model_phase3_get_object_attributes_missing_matrix_shard_07 => 7,
    authz_model_phase3_get_object_attributes_missing_matrix_shard_08 => 8,
    authz_model_phase3_get_object_attributes_missing_matrix_shard_09 => 9,
    authz_model_phase3_get_object_attributes_missing_matrix_shard_10 => 10,
    authz_model_phase3_get_object_attributes_missing_matrix_shard_11 => 11,
    authz_model_phase3_get_object_attributes_missing_matrix_shard_12 => 12,
    authz_model_phase3_get_object_attributes_missing_matrix_shard_13 => 13,
    authz_model_phase3_get_object_attributes_missing_matrix_shard_14 => 14,
    authz_model_phase3_get_object_attributes_missing_matrix_shard_15 => 15,
    authz_model_phase3_get_object_attributes_missing_matrix_shard_16 => 16,
    authz_model_phase3_get_object_attributes_missing_matrix_shard_17 => 17,
    authz_model_phase3_get_object_attributes_missing_matrix_shard_18 => 18,
    authz_model_phase3_get_object_attributes_missing_matrix_shard_19 => 19,
    authz_model_phase3_get_object_attributes_missing_matrix_shard_20 => 20,
    authz_model_phase3_get_object_attributes_missing_matrix_shard_21 => 21,
    authz_model_phase3_get_object_attributes_missing_matrix_shard_22 => 22,
    authz_model_phase3_get_object_attributes_missing_matrix_shard_23 => 23,
    authz_model_phase3_get_object_attributes_missing_matrix_shard_24 => 24,
    authz_model_phase3_get_object_attributes_missing_matrix_shard_25 => 25,
    authz_model_phase3_get_object_attributes_missing_matrix_shard_26 => 26,
    authz_model_phase3_get_object_attributes_missing_matrix_shard_27 => 27,
    authz_model_phase3_get_object_attributes_missing_matrix_shard_28 => 28,
    authz_model_phase3_get_object_attributes_missing_matrix_shard_29 => 29,
    authz_model_phase3_get_object_attributes_missing_matrix_shard_30 => 30,
    authz_model_phase3_get_object_attributes_missing_matrix_shard_31 => 31,
);

boe_get_object_attributes_missing_matrix_shards!(
    authz_model_modern_boe_get_object_attributes_missing_matrix_shard_00 => 0,
    authz_model_modern_boe_get_object_attributes_missing_matrix_shard_01 => 1,
    authz_model_modern_boe_get_object_attributes_missing_matrix_shard_02 => 2,
    authz_model_modern_boe_get_object_attributes_missing_matrix_shard_03 => 3,
    authz_model_modern_boe_get_object_attributes_missing_matrix_shard_04 => 4,
    authz_model_modern_boe_get_object_attributes_missing_matrix_shard_05 => 5,
    authz_model_modern_boe_get_object_attributes_missing_matrix_shard_06 => 6,
    authz_model_modern_boe_get_object_attributes_missing_matrix_shard_07 => 7,
);

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
fn authz_model_modern_boe_put_object_write_matrix() {
    run_phase4_modern_boe_write_matrix(WriteAction::PutObject);
}

#[test]
fn authz_model_modern_boe_create_multipart_upload_write_matrix() {
    run_phase4_modern_boe_write_matrix(WriteAction::CreateMultipartUpload);
}

#[test]
fn authz_model_modern_boe_begin_stream_put_write_matrix() {
    run_phase4_modern_boe_write_matrix(WriteAction::BeginStreamPut);
}

#[test]
fn authz_model_phase4_put_object_acl_matrix_shard_00() {
    run_phase4_acl_matrix_shard(AclUpdateAction::PutObjectAcl, 0, 8);
}

#[test]
fn authz_model_phase4_put_object_acl_matrix_shard_01() {
    run_phase4_acl_matrix_shard(AclUpdateAction::PutObjectAcl, 1, 8);
}

#[test]
fn authz_model_phase4_put_object_acl_matrix_shard_02() {
    run_phase4_acl_matrix_shard(AclUpdateAction::PutObjectAcl, 2, 8);
}

#[test]
fn authz_model_phase4_put_object_acl_matrix_shard_03() {
    run_phase4_acl_matrix_shard(AclUpdateAction::PutObjectAcl, 3, 8);
}

#[test]
fn authz_model_phase4_put_object_acl_matrix_shard_04() {
    run_phase4_acl_matrix_shard(AclUpdateAction::PutObjectAcl, 4, 8);
}

#[test]
fn authz_model_phase4_put_object_acl_matrix_shard_05() {
    run_phase4_acl_matrix_shard(AclUpdateAction::PutObjectAcl, 5, 8);
}

#[test]
fn authz_model_phase4_put_object_acl_matrix_shard_06() {
    run_phase4_acl_matrix_shard(AclUpdateAction::PutObjectAcl, 6, 8);
}

#[test]
fn authz_model_phase4_put_object_acl_matrix_shard_07() {
    run_phase4_acl_matrix_shard(AclUpdateAction::PutObjectAcl, 7, 8);
}

#[test]
fn authz_model_phase4_put_object_version_acl_matrix_shard_00() {
    run_phase4_acl_matrix_shard(AclUpdateAction::PutObjectVersionAcl, 0, 8);
}

#[test]
fn authz_model_phase4_put_object_version_acl_matrix_shard_01() {
    run_phase4_acl_matrix_shard(AclUpdateAction::PutObjectVersionAcl, 1, 8);
}

#[test]
fn authz_model_phase4_put_object_version_acl_matrix_shard_02() {
    run_phase4_acl_matrix_shard(AclUpdateAction::PutObjectVersionAcl, 2, 8);
}

#[test]
fn authz_model_phase4_put_object_version_acl_matrix_shard_03() {
    run_phase4_acl_matrix_shard(AclUpdateAction::PutObjectVersionAcl, 3, 8);
}

#[test]
fn authz_model_phase4_put_object_version_acl_matrix_shard_04() {
    run_phase4_acl_matrix_shard(AclUpdateAction::PutObjectVersionAcl, 4, 8);
}

#[test]
fn authz_model_phase4_put_object_version_acl_matrix_shard_05() {
    run_phase4_acl_matrix_shard(AclUpdateAction::PutObjectVersionAcl, 5, 8);
}

#[test]
fn authz_model_phase4_put_object_version_acl_matrix_shard_06() {
    run_phase4_acl_matrix_shard(AclUpdateAction::PutObjectVersionAcl, 6, 8);
}

#[test]
fn authz_model_phase4_put_object_version_acl_matrix_shard_07() {
    run_phase4_acl_matrix_shard(AclUpdateAction::PutObjectVersionAcl, 7, 8);
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
fn authz_model_modern_boe_upload_part_copy_matrix() {
    let scenarios: Vec<_> = UploadPartCopyScenario::scenarios()
        .into_iter()
        .filter(|scenario| {
            scenario.bucket.ownership
                == crate::coordinator::authz_model_tests::model::OwnershipShape::BucketOwnerEnforced
        })
        .collect();
    assert!(
        !scenarios.is_empty(),
        "modern BOE upload-part-copy matrix unexpectedly produced no scenarios"
    );
    let harness = Phase6Harness::new();

    for (index, scenario) in scenarios.into_iter().enumerate() {
        let bucket = format!("authz-modern-boe-upc-{index:05}");
        let expected = scenario.expected_outcome();
        let actual = to_upload_part_copy_outcome(harness.run_upload_part_copy(&bucket, scenario));
        assert_eq!(
            actual, expected,
            "modern BOE upload-part-copy mismatch\nscenario: {scenario}\nexpected: {expected}\nactual: {actual}"
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
fn authz_model_modern_boe_delete_matrix() {
    let scenarios: Vec<_> = DeleteScenario::scenarios()
        .into_iter()
        .filter(|scenario| {
            scenario.ownership == phase7_model::DeleteOwnershipShape::BucketOwnerEnforced
        })
        .collect();
    assert!(
        !scenarios.is_empty(),
        "modern BOE delete matrix unexpectedly produced no scenarios"
    );
    let harness = Phase7Harness::new();

    for (index, scenario) in scenarios.into_iter().enumerate() {
        let bucket = format!("{}-boe", phase7_delete_bucket_name_for(index));
        let actual = harness.run_delete(&bucket, scenario);
        assert_eq!(
            actual, scenario.expected,
            "modern BOE delete mismatch\nscenario: {scenario}\nexpected: {}\nactual: {actual}",
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
fn authz_model_modern_boe_multipart_write_matrix() {
    let scenarios: Vec<_> = MultipartWriteScenario::scenarios()
        .into_iter()
        .filter(|scenario| {
            scenario.upload == phase7a_model::MultipartUploadShape::CrossAccountBucketOwnerEnforced
        })
        .collect();
    assert!(
        !scenarios.is_empty(),
        "modern BOE multipart write matrix unexpectedly produced no scenarios"
    );
    let harness = Phase7aHarness::new();

    for (index, scenario) in scenarios.into_iter().enumerate() {
        let bucket = format!("authz-modern-boe-mpu-write-{index:05}");
        let actual = harness.run_write(&bucket, scenario);
        assert_eq!(
            actual, scenario.expected,
            "modern BOE multipart write mismatch\nscenario: {scenario}\nexpected: {}\nactual: {actual}",
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
fn authz_model_modern_boe_multipart_management_matrix() {
    let scenarios: Vec<_> = MultipartManagementScenario::scenarios()
        .into_iter()
        .filter(|scenario| {
            scenario.upload == phase7a_model::MultipartUploadShape::CrossAccountBucketOwnerEnforced
        })
        .collect();
    assert!(
        !scenarios.is_empty(),
        "modern BOE multipart management matrix unexpectedly produced no scenarios"
    );
    let harness = Phase7aHarness::new();

    for (index, scenario) in scenarios.into_iter().enumerate() {
        let bucket = format!("authz-modern-boe-mpu-manage-{index:05}");
        let actual = harness.run_management(&bucket, scenario);
        assert_eq!(
            actual, scenario.expected,
            "modern BOE multipart management mismatch\nscenario: {scenario}\nexpected: {}\nactual: {actual}",
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

#[test]
fn authz_model_phase10_put_bucket_acl_null_treats_absent_acl_header_as_missing() {
    let harness = Phase10Harness::new();
    let bucket = "authz-phase10-bucket-acl-null";
    phase10_harness::create_bucket(&harness.coord, &harness.fixtures, bucket).unwrap();
    phase10_harness::put_bucket_acl_policy(
        &harness.coord,
        &harness.fixtures,
        bucket,
        r#"{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:user/other"},"Action":"s3:PutBucketAcl","Resource":"arn:aws:s3:::authz-phase10-bucket-acl-null"},
           {"Effect":"Deny","Principal":{"AWS":"arn:aws:iam::444455556666:user/other"},"Action":"s3:PutBucketAcl","Resource":"arn:aws:s3:::authz-phase10-bucket-acl-null","Condition":{"Null":{"s3:x-amz-acl":"true"}}}"#,
    )
    .unwrap();

    let requester = phase10_harness::cross_account_requester(&harness.fixtures);
    let absent = phase10_harness::put_canned_acl_private(
        &harness.coord,
        bucket,
        requester.clone(),
        PutObjectPolicyContext::default(),
    );
    assert!(matches!(absent, Err(ServerError::AccessDenied)));

    let exact = phase10_harness::put_canned_acl_private(
        &harness.coord,
        bucket,
        requester,
        PutObjectPolicyContext::default().with_default_canned_acl(Some("private")),
    );
    assert!(
        exact.is_ok(),
        "expected explicit private x-amz-acl to satisfy Null policy: {exact:?}"
    );
}

#[test]
fn authz_model_phase10_put_bucket_acl_string_not_equals_treats_absent_acl_header_as_not_equal() {
    let harness = Phase10Harness::new();
    let bucket = "authz-phase10-bucket-acl-sne";
    phase10_harness::create_bucket(&harness.coord, &harness.fixtures, bucket).unwrap();
    phase10_harness::put_bucket_acl_policy(
        &harness.coord,
        &harness.fixtures,
        bucket,
        r#"{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:user/other"},"Action":"s3:PutBucketAcl","Resource":"arn:aws:s3:::authz-phase10-bucket-acl-sne"},
           {"Effect":"Deny","Principal":{"AWS":"arn:aws:iam::444455556666:user/other"},"Action":"s3:PutBucketAcl","Resource":"arn:aws:s3:::authz-phase10-bucket-acl-sne","Condition":{"StringNotEquals":{"s3:x-amz-acl":"private"}}}"#,
    )
    .unwrap();

    let requester = phase10_harness::cross_account_requester(&harness.fixtures);
    let absent = phase10_harness::put_canned_acl_private(
        &harness.coord,
        bucket,
        requester.clone(),
        PutObjectPolicyContext::default(),
    );
    assert!(matches!(absent, Err(ServerError::AccessDenied)));

    let exact = phase10_harness::put_canned_acl_private(
        &harness.coord,
        bucket,
        requester,
        PutObjectPolicyContext::default().with_default_canned_acl(Some("private")),
    );
    assert!(
        exact.is_ok(),
        "expected explicit private x-amz-acl to satisfy StringNotEquals policy: {exact:?}"
    );
}

#[test]
fn authz_model_phase10_put_bucket_acl_grant_header_exactness() {
    let harness = Phase10Harness::new();
    let requester = phase10_harness::cross_account_requester(&harness.fixtures);
    let header_value = phase10_harness::cross_account_grant_header_value(&harness.fixtures);

    for grant in phase10_harness::GrantHeaderKind::ALL {
        let bucket = format!("authz-phase10-bucket-acl-{grant}");
        phase10_harness::create_bucket(&harness.coord, &harness.fixtures, &bucket).unwrap();
        let escaped = header_value.replace('"', "\\\"");
        phase10_harness::put_bucket_acl_policy(
            &harness.coord,
            &harness.fixtures,
            &bucket,
            &format!(
                r#"{{"Effect":"Allow","Principal":{{"AWS":"arn:aws:iam::444455556666:user/other"}},"Action":"s3:PutBucketAcl","Resource":"arn:aws:s3:::{bucket}","Condition":{{"StringEquals":{{"{}":"{}"}}}}}}"#,
                grant.header_name(),
                escaped
            ),
        )
        .unwrap();

        let absent = phase10_harness::put_grant_acl(
            &harness.coord,
            &harness.fixtures,
            &bucket,
            requester.clone(),
            grant,
            PutObjectPolicyContext::default(),
        );
        assert!(
            matches!(absent, Err(ServerError::AccessDenied)),
            "expected missing {grant} header to deny, got {absent:?}"
        );

        let wrong = phase10_harness::put_grant_acl(
            &harness.coord,
            &harness.fixtures,
            &bucket,
            requester.clone(),
            grant,
            grant.alternate().with_policy_context(
                PutObjectPolicyContext::default(),
                Some(header_value.as_str()),
            ),
        );
        assert!(
            wrong.is_err(),
            "expected wrong grant header provenance for {grant} to fail, got {wrong:?}"
        );

        let exact = phase10_harness::put_grant_acl(
            &harness.coord,
            &harness.fixtures,
            &bucket,
            requester.clone(),
            grant,
            grant.with_policy_context(
                PutObjectPolicyContext::default(),
                Some(header_value.as_str()),
            ),
        );
        assert!(
            exact.is_ok(),
            "expected exact {grant} header to allow PutBucketAcl: {exact:?}"
        );
    }
}

#[test]
fn authz_model_phase10_put_bucket_acl_xml_body_does_not_synthesize_grant_headers() {
    let harness = Phase10Harness::new();
    let bucket = "authz-phase10-bucket-acl-xml";
    phase10_harness::create_bucket(&harness.coord, &harness.fixtures, bucket).unwrap();
    let header_value = phase10_harness::cross_account_grant_header_value(&harness.fixtures);
    let escaped = header_value.replace('"', "\\\"");
    phase10_harness::put_bucket_acl_policy(
        &harness.coord,
        &harness.fixtures,
        bucket,
        &format!(
            r#"{{"Effect":"Allow","Principal":{{"AWS":"arn:aws:iam::444455556666:user/other"}},"Action":"s3:PutBucketAcl","Resource":"arn:aws:s3:::{bucket}","Condition":{{"StringEquals":{{"s3:x-amz-grant-read":"{escaped}"}}}}}}"#
        ),
    )
    .unwrap();

    let requester = phase10_harness::cross_account_requester(&harness.fixtures);
    let xml_body = phase10_harness::put_grant_acl(
        &harness.coord,
        &harness.fixtures,
        bucket,
        requester.clone(),
        phase10_harness::GrantHeaderKind::Read,
        PutObjectPolicyContext::default(),
    );
    assert!(
        matches!(xml_body, Err(ServerError::AccessDenied)),
        "expected XML-body grants without original header provenance to deny: {xml_body:?}"
    );

    let header_request = phase10_harness::put_grant_acl(
        &harness.coord,
        &harness.fixtures,
        bucket,
        requester,
        phase10_harness::GrantHeaderKind::Read,
        phase10_harness::GrantHeaderKind::Read.with_policy_context(
            PutObjectPolicyContext::default(),
            Some(header_value.as_str()),
        ),
    );
    assert!(
        header_request.is_ok(),
        "expected explicit grant-read header provenance to allow PutBucketAcl: {header_request:?}"
    );
}

#[test]
fn authz_model_phase11_head_bucket_matrix() {
    run_phase11_bucket_meta_matrix(BucketMetaAction::HeadBucket);
}

#[test]
fn authz_model_phase11_get_bucket_location_matrix() {
    run_phase11_bucket_meta_matrix(BucketMetaAction::GetBucketLocation);
}

fn run_existing_matrix_without_cross_account_owner(phase: &str, action: Action) {
    let scenarios = Scenario::existing_scenarios(action)
        .into_iter()
        .filter(|scenario| scenario.object.owner_kind != model::ObjectOwnerKind::CrossAccount)
        .collect();
    run_existing_matrix_scenarios(phase, action, scenarios);
}

fn run_existing_matrix_with_only_cross_account_owner(phase: &str, action: Action) {
    let scenarios = Scenario::existing_scenarios(action)
        .into_iter()
        .filter(|scenario| scenario.object.owner_kind == model::ObjectOwnerKind::CrossAccount)
        .collect();
    run_existing_matrix_scenarios(phase, action, scenarios);
}

fn run_existing_matrix_without_cross_account_owner_shard(
    phase: &str,
    action: Action,
    shard_index: usize,
    shard_count: usize,
) {
    let scenarios = Scenario::existing_scenarios(action)
        .into_iter()
        .filter(|scenario| scenario.object.owner_kind != model::ObjectOwnerKind::CrossAccount)
        .collect();
    run_existing_matrix_scenarios_shard(phase, action, scenarios, shard_index, shard_count);
}

fn run_existing_matrix_scenarios(phase: &str, action: Action, scenarios: Vec<Scenario>) {
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

fn run_existing_matrix_scenarios_shard(
    phase: &str,
    action: Action,
    scenarios: Vec<Scenario>,
    shard_index: usize,
    shard_count: usize,
) {
    assert!(
        shard_count > 0,
        "existing-matrix shard count must be non-zero"
    );
    assert!(
        shard_index < shard_count,
        "existing-matrix shard index {shard_index} out of range for shard count {shard_count}"
    );
    assert!(
        !scenarios.is_empty(),
        "{phase} matrix unexpectedly produced no scenarios for {action}"
    );
    let harness = MatrixHarness::new();
    let mut shard_len = 0usize;

    for (index, scenario) in scenarios.into_iter().enumerate() {
        if index % shard_count != shard_index {
            continue;
        }
        shard_len += 1;
        let bucket = bucket_name_for(action, index);
        let expected = scenario.expected_existing_outcome();
        let actual = to_existing_outcome(harness.run_existing(&bucket, scenario));
        assert_eq!(
            actual, expected,
            "{phase} authz model mismatch\nscenario: {scenario}\nexpected: {expected}\nactual: {actual}"
        );
    }

    assert!(
        shard_len > 0,
        "{phase} existing-matrix shard {shard_index}/{shard_count} had no scenarios for {action}"
    );
}

fn modern_bucket_name_for(action: Action, index: usize) -> String {
    let action_slug = match action {
        Action::GetObject => "go",
        Action::GetObjectAttributes => "goa",
        Action::GetObjectAcl => "goacl",
        Action::GetObjectTagging => "gotag",
        Action::PutObjectTagging => "potag",
        Action::DeleteObjectTagging => "dotag",
    };
    format!("authz-modern-{action_slug}-{index:05}")
}

fn run_modern_boe_existing_matrix(action: Action) {
    let scenarios = Scenario::existing_scenarios(action)
        .into_iter()
        .filter(|scenario| scenario.bucket.ownership == model::OwnershipShape::BucketOwnerEnforced)
        .collect();
    run_modern_existing_matrix_scenarios(action, scenarios);
}

fn run_modern_boe_existing_matrix_shard(action: Action, shard_index: usize, shard_count: usize) {
    let scenarios = Scenario::existing_scenarios(action)
        .into_iter()
        .filter(|scenario| scenario.bucket.ownership == model::OwnershipShape::BucketOwnerEnforced)
        .collect();
    run_modern_existing_matrix_scenarios_shard(action, scenarios, shard_index, shard_count);
}

fn run_modern_existing_matrix_scenarios(action: Action, scenarios: Vec<Scenario>) {
    assert!(
        !scenarios.is_empty(),
        "modern BOE auth matrix unexpectedly produced no scenarios for {action}"
    );
    let harness = MatrixHarness::new();

    for (index, scenario) in scenarios.into_iter().enumerate() {
        let bucket = modern_bucket_name_for(action, index);
        let expected = scenario.expected_modern_existing_outcome();
        let actual = to_modern_outcome(harness.run_existing_modern(&bucket, scenario));
        assert_eq!(
            actual, expected,
            "modern BOE auth model mismatch\nscenario: {scenario}\nexpected: {expected}\nactual: {actual}"
        );
    }
}

fn run_modern_existing_matrix_scenarios_shard(
    action: Action,
    scenarios: Vec<Scenario>,
    shard_index: usize,
    shard_count: usize,
) {
    assert!(
        shard_count > 0,
        "modern existing-matrix shard count must be non-zero"
    );
    assert!(
        shard_index < shard_count,
        "modern existing-matrix shard index {shard_index} out of range for shard count {shard_count}"
    );
    assert!(
        !scenarios.is_empty(),
        "modern BOE auth matrix unexpectedly produced no scenarios for {action}"
    );
    let harness = MatrixHarness::new();
    let mut shard_len = 0usize;

    for (index, scenario) in scenarios.into_iter().enumerate() {
        if index % shard_count != shard_index {
            continue;
        }
        shard_len += 1;
        let bucket = modern_bucket_name_for(action, index);
        let expected = scenario.expected_modern_existing_outcome();
        let actual = to_modern_outcome(harness.run_existing_modern(&bucket, scenario));
        assert_eq!(
            actual, expected,
            "modern BOE auth model mismatch\nscenario: {scenario}\nexpected: {expected}\nactual: {actual}"
        );
    }

    assert!(
        shard_len > 0,
        "modern existing-matrix shard {shard_index}/{shard_count} had no scenarios for {action}"
    );
}

fn run_boe_read_fast_path_invariant(action: Action) {
    let scenarios: Vec<_> = Scenario::existing_scenarios(action)
        .into_iter()
        .filter(|scenario| scenario.bucket.ownership == model::OwnershipShape::BucketOwnerEnforced)
        .collect();
    run_boe_read_fast_path_invariant_scenarios(action, scenarios);
}

fn run_boe_read_fast_path_invariant_shard(action: Action, shard_index: usize, shard_count: usize) {
    assert!(
        shard_count > 0,
        "BOE read fast-path invariant shard count must be non-zero"
    );
    assert!(
        shard_index < shard_count,
        "BOE read fast-path invariant shard index {shard_index} out of range for shard count {shard_count}"
    );
    let scenarios: Vec<_> = Scenario::existing_scenarios(action)
        .into_iter()
        .filter(|scenario| scenario.bucket.ownership == model::OwnershipShape::BucketOwnerEnforced)
        .collect();
    assert!(
        !scenarios.is_empty(),
        "BOE read fast-path invariant unexpectedly produced no scenarios for {action}"
    );
    let harness = MatrixHarness::new();
    let mut shard_len = 0usize;

    for (index, scenario) in scenarios.into_iter().enumerate() {
        if index % shard_count != shard_index {
            continue;
        }
        shard_len += 1;
        let bucket = format!("{}-boe-fast", modern_bucket_name_for(action, index));
        let (actual, modern) = harness.run_existing_boe_fast_path_invariant(&bucket, scenario);
        let actual = to_modern_existing_from_classified(actual);
        let modern = to_modern_outcome(modern);
        assert_eq!(
            actual, modern,
            "BOE read fast-path invariant mismatch\nscenario: {scenario}\nfast-path actual: {actual}\nsnapshot modern: {modern}"
        );
    }

    assert!(
        shard_len > 0,
        "BOE read fast-path invariant shard {shard_index}/{shard_count} had no scenarios for {action}"
    );
}

fn run_boe_read_fast_path_invariant_scenarios(action: Action, scenarios: Vec<Scenario>) {
    assert!(
        !scenarios.is_empty(),
        "BOE read fast-path invariant unexpectedly produced no scenarios for {action}"
    );
    let harness = MatrixHarness::new();

    for (index, scenario) in scenarios.into_iter().enumerate() {
        let bucket = format!("{}-boe-fast", modern_bucket_name_for(action, index));
        let (actual, modern) = harness.run_existing_boe_fast_path_invariant(&bucket, scenario);
        let actual = to_modern_existing_from_classified(actual);
        let modern = to_modern_outcome(modern);
        assert_eq!(
            actual, modern,
            "BOE read fast-path invariant mismatch\nscenario: {scenario}\nfast-path actual: {actual}\nsnapshot modern: {modern}"
        );
    }
}

fn to_modern_outcome(actual: ModernObjectReadAuthorization) -> model::ModernOutcome {
    match actual {
        ModernObjectReadAuthorization::Allowed => model::ModernOutcome::Allow,
        ModernObjectReadAuthorization::Denied => model::ModernOutcome::Deny,
    }
}

fn to_modern_existing_from_classified(actual: harness::ClassifiedResult) -> model::ModernOutcome {
    match actual {
        harness::ClassifiedResult::Allow => model::ModernOutcome::Allow,
        harness::ClassifiedResult::AccessDenied => model::ModernOutcome::Deny,
        harness::ClassifiedResult::NoSuchKey | harness::ClassifiedResult::VersionNotFound => {
            panic!("existing-object BOE invariant produced an impossible missing-object result")
        }
    }
}

fn to_simple_outcome(actual: ModernObjectWriteAuthorization) -> model::Outcome {
    match actual {
        ModernObjectWriteAuthorization::Allowed => model::Outcome::Allow,
        ModernObjectWriteAuthorization::Denied => model::Outcome::Deny,
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

fn run_missing_matrix_shard(phase: &str, action: Action, shard_index: usize, shard_count: usize) {
    assert!(
        shard_count > 0,
        "missing-matrix shard count must be non-zero"
    );
    assert!(
        shard_index < shard_count,
        "missing-matrix shard index {shard_index} out of range for shard count {shard_count}"
    );
    let scenarios = MissingScenario::missing_scenarios(action);
    assert!(
        !scenarios.is_empty(),
        "{phase} matrix unexpectedly produced no scenarios for {action}"
    );
    let harness = MatrixHarness::new();
    let mut shard_len = 0usize;

    for (index, scenario) in scenarios.into_iter().enumerate() {
        if index % shard_count != shard_index {
            continue;
        }
        shard_len += 1;
        let bucket = bucket_name_for(action, index);
        let expected = scenario.expected_outcome();
        let actual = to_missing_outcome(harness.run_missing(&bucket, scenario), scenario.target);
        assert_eq!(
            actual, expected,
            "{phase} authz model mismatch\nshard: {}/{}\nscenario: {scenario}\nexpected: {expected}\nactual: {actual}",
            shard_index + 1,
            shard_count
        );
    }

    assert!(
        shard_len > 0,
        "{phase} matrix shard {}/{} unexpectedly produced no scenarios for {action}",
        shard_index + 1,
        shard_count
    );
}

fn run_boe_missing_matrix(action: Action) {
    let scenarios: Vec<_> = MissingScenario::missing_scenarios(action)
        .into_iter()
        .filter(|scenario| scenario.bucket.ownership == model::OwnershipShape::BucketOwnerEnforced)
        .collect();
    run_boe_missing_matrix_scenarios(action, scenarios);
}

fn run_boe_missing_matrix_shard(action: Action, shard_index: usize, shard_count: usize) {
    assert!(
        shard_count > 0,
        "BOE missing-matrix shard count must be non-zero"
    );
    assert!(
        shard_index < shard_count,
        "BOE missing-matrix shard index {shard_index} out of range for shard count {shard_count}"
    );
    let scenarios: Vec<_> = MissingScenario::missing_scenarios(action)
        .into_iter()
        .filter(|scenario| scenario.bucket.ownership == model::OwnershipShape::BucketOwnerEnforced)
        .collect();
    assert!(
        !scenarios.is_empty(),
        "BOE missing-object matrix unexpectedly produced no scenarios for {action}"
    );
    let harness = MatrixHarness::new();
    let mut shard_len = 0usize;

    for (index, scenario) in scenarios.into_iter().enumerate() {
        if index % shard_count != shard_index {
            continue;
        }
        shard_len += 1;
        let bucket = format!("{}-boe-missing", bucket_name_for(action, index));
        let expected = scenario.expected_outcome();
        let actual = to_missing_outcome(harness.run_missing(&bucket, scenario), scenario.target);
        assert_eq!(
            actual, expected,
            "BOE missing-object authz mismatch\nshard: {}/{}\nscenario: {scenario}\nexpected: {expected}\nactual: {actual}",
            shard_index + 1,
            shard_count
        );
    }

    assert!(
        shard_len > 0,
        "BOE missing-object matrix shard {}/{} unexpectedly produced no scenarios for {action}",
        shard_index + 1,
        shard_count
    );
}

fn run_boe_missing_matrix_scenarios(action: Action, scenarios: Vec<MissingScenario>) {
    assert!(
        !scenarios.is_empty(),
        "BOE missing-object matrix unexpectedly produced no scenarios for {action}"
    );
    let harness = MatrixHarness::new();

    for (index, scenario) in scenarios.into_iter().enumerate() {
        let bucket = format!("{}-boe-missing", bucket_name_for(action, index));
        let expected = scenario.expected_outcome();
        let actual = to_missing_outcome(harness.run_missing(&bucket, scenario), scenario.target);
        assert_eq!(
            actual, expected,
            "BOE missing-object authz mismatch\nscenario: {scenario}\nexpected: {expected}\nactual: {actual}"
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

fn run_phase4_modern_boe_write_matrix(action: WriteAction) {
    let scenarios: Vec<_> = WriteScenario::scenarios(action)
        .into_iter()
        .filter(|scenario| scenario.bucket.ownership == model::OwnershipShape::BucketOwnerEnforced)
        .collect();
    assert!(
        !scenarios.is_empty(),
        "phase 4 modern BOE matrix unexpectedly produced no scenarios for {action}"
    );
    let harness = Phase4Harness::new();

    for (index, scenario) in scenarios.into_iter().enumerate() {
        let bucket = format!(
            "authz-modern-boe-{}-{index:05}",
            modern_write_action_slug(action)
        );
        let expected = scenario.expected_modern_boe_outcome();
        let actual = to_simple_outcome(harness.run_write_modern(&bucket, scenario));
        assert_eq!(
            actual, expected,
            "phase 4 modern BOE authz mismatch\nscenario: {scenario}\nexpected: {expected}\nactual: {actual}"
        );
    }
}

fn modern_write_action_slug(action: WriteAction) -> &'static str {
    match action {
        WriteAction::PutObject => "putobj",
        WriteAction::CreateMultipartUpload => "mpu",
        WriteAction::BeginStreamPut => "streamput",
    }
}

fn run_phase4_acl_matrix_shard(action: AclUpdateAction, shard_index: usize, shard_count: usize) {
    assert!(shard_count > 0, "phase 4 ACL shard count must be non-zero");
    assert!(
        shard_index < shard_count,
        "phase 4 ACL shard index {shard_index} out of range for shard count {shard_count}"
    );
    let scenarios = AclUpdateScenario::scenarios(action);
    assert!(
        !scenarios.is_empty(),
        "phase 4 matrix unexpectedly produced no scenarios for {action}"
    );
    let harness = Phase4Harness::new();
    let mut shard_len = 0usize;

    for (index, scenario) in scenarios.into_iter().enumerate() {
        if index % shard_count != shard_index {
            continue;
        }
        shard_len += 1;
        let bucket = acl_bucket_name_for(action, index);
        let expected = scenario.expected_outcome();
        let actual = to_acl_outcome(harness.run_acl_update(&bucket, scenario));
        assert_eq!(
            actual, expected,
            "phase 4 authz model mismatch\nshard: {}/{}\nscenario: {scenario}\nexpected: {expected}\nactual: {actual}",
            shard_index + 1,
            shard_count
        );
    }

    assert!(
        shard_len > 0,
        "phase 4 ACL shard {}/{} unexpectedly produced no scenarios for {action}",
        shard_index + 1,
        shard_count
    );
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

fn run_phase11_bucket_meta_matrix(action: BucketMetaAction) {
    let scenarios = BucketMetaScenario::scenarios(action);
    assert!(
        !scenarios.is_empty(),
        "phase 11 matrix unexpectedly produced no scenarios for {action}"
    );
    let harness = Phase11Harness::new();

    for (index, scenario) in scenarios.into_iter().enumerate() {
        let bucket = phase11_bucket_name_for(action, index);
        let actual = harness.run(&bucket, scenario);
        assert_eq!(
            actual, scenario.expected,
            "phase 11 bucket-meta mismatch\nscenario: {scenario}\nexpected: {}\nactual: {actual}",
            scenario.expected
        );
    }
}

fn boe_trace_policy_strategy() -> impl Strategy<Value = BoeTracePolicyState> {
    prop_oneof![
        Just(BoeTracePolicyState::None),
        Just(BoeTracePolicyState::AllowObjectOnlyPrivate),
        Just(BoeTracePolicyState::AllowBothPrivate),
        Just(BoeTracePolicyState::AllowBothPublic),
        Just(BoeTracePolicyState::DenyBothPrivate),
        Just(BoeTracePolicyState::AllowBothPrivateTagPublic),
        Just(BoeTracePolicyState::AllowWritePrivate),
        Just(BoeTracePolicyState::AllowWritePublic),
        Just(BoeTracePolicyState::DenyWritePrivate),
        Just(BoeTracePolicyState::AllowWritePrivateTagPublic),
        Just(BoeTracePolicyState::AllowDeletePrivate),
        Just(BoeTracePolicyState::AllowDeletePublic),
        Just(BoeTracePolicyState::DenyDeletePrivate),
        Just(BoeTracePolicyState::AllowDeletePrivateTagPublic),
    ]
}

fn boe_trace_bucket_tag_strategy() -> impl Strategy<Value = BoeTraceBucketTagState> {
    prop_oneof![
        Just(BoeTraceBucketTagState::Public),
        Just(BoeTraceBucketTagState::Private),
    ]
}

fn boe_trace_probe_strategy() -> impl Strategy<Value = BoeTraceProbe> {
    prop_oneof![
        Just(BoeTraceProbe::GetObject),
        Just(BoeTraceProbe::GetObjectAttributes),
        Just(BoeTraceProbe::PutObject),
        Just(BoeTraceProbe::CreateMultipartUpload),
        Just(BoeTraceProbe::DeleteObject),
    ]
}

fn boe_trace_mutation_strategy() -> impl Strategy<Value = BoeTraceMutation> {
    prop_oneof![
        boe_trace_policy_strategy().prop_map(BoeTraceMutation::Policy),
        any::<bool>().prop_map(BoeTraceMutation::RestrictPublicBuckets),
        any::<bool>().prop_map(BoeTraceMutation::BucketAbac),
        boe_trace_bucket_tag_strategy().prop_map(BoeTraceMutation::BucketTags),
    ]
}

fn boe_modern_trace_strategy() -> impl Strategy<Value = (Vec<BoeTraceMutation>, BoeTraceProbe)> {
    (
        prop::collection::vec(boe_trace_mutation_strategy(), 0..=6),
        boe_trace_probe_strategy(),
    )
}

fn phase13_read_policy_strategy() -> impl Strategy<Value = Phase13ReadPolicyState> {
    prop_oneof![
        Just(Phase13ReadPolicyState::None),
        Just(Phase13ReadPolicyState::AllowPrivate),
    ]
}

fn phase13_delete_policy_strategy() -> impl Strategy<Value = DeletePolicyShape> {
    prop_oneof![
        Just(DeletePolicyShape::None),
        Just(DeletePolicyShape::AllowDeleteObject),
        Just(DeletePolicyShape::AllowDeleteVersion),
        Just(DeletePolicyShape::AllowDeleteVersionAndBypass),
        Just(DeletePolicyShape::DenyBypass),
    ]
}

fn phase13_probe_strategy() -> impl Strategy<Value = Phase13Probe> {
    prop_oneof![
        Just(Phase13Probe::Object),
        Just(Phase13Probe::Attributes),
        Just(Phase13Probe::Delete),
        Just(Phase13Probe::DeleteTrackedVersion),
        Just(Phase13Probe::DeleteTrackedVersionBypass),
    ]
}

fn phase13_execution_probe_strategy() -> impl Strategy<Value = Phase13ExecutionProbe> {
    prop_oneof![
        Just(Phase13ExecutionProbe::Current),
        Just(Phase13ExecutionProbe::Version),
        Just(Phase13ExecutionProbe::VersionBypass),
    ]
}

fn phase13_mutation_strategy() -> impl Strategy<Value = Phase13Mutation> {
    prop_oneof![
        phase13_read_policy_strategy().prop_map(Phase13Mutation::ReadPolicy),
        phase13_delete_policy_strategy().prop_map(Phase13Mutation::DeletePolicy),
        Just(Phase13Mutation::OwnerPutCurrentVersion),
        Just(Phase13Mutation::OwnerDelete),
        Just(Phase13Mutation::OwnerDeleteTrackedVersion),
        Just(Phase13Mutation::ApplyTrackedGovernanceRetention),
        Just(Phase13Mutation::ApplyTrackedComplianceRetention),
        Just(Phase13Mutation::ApplyTrackedLegalHold),
    ]
}

fn phase13_trace_strategy() -> impl Strategy<Value = (Vec<Phase13Mutation>, Phase13Probe)> {
    (
        prop::collection::vec(phase13_mutation_strategy(), 0..=6),
        phase13_probe_strategy(),
    )
}

fn phase13_execution_trace_strategy(
) -> impl Strategy<Value = (Vec<Phase13Mutation>, Phase13ExecutionProbe)> {
    (
        prop::collection::vec(phase13_mutation_strategy(), 0..=6),
        phase13_execution_probe_strategy(),
    )
}

#[derive(Debug, Clone)]
enum Phase14TraceSeed {
    Transition { choice: u8 },
    ReadPolicy(Phase14ReadPolicyState),
    DeletePolicy(Phase14DeletePolicyState),
    OwnerPutCurrent,
    OwnerDelete,
}

fn phase14_read_policy_strategy() -> impl Strategy<Value = Phase14ReadPolicyState> {
    prop_oneof![
        Just(Phase14ReadPolicyState::None),
        Just(Phase14ReadPolicyState::AllowPrivate),
    ]
}

fn phase14_delete_policy_strategy() -> impl Strategy<Value = Phase14DeletePolicyState> {
    prop_oneof![
        Just(Phase14DeletePolicyState::None),
        Just(Phase14DeletePolicyState::AllowDeleteObject),
    ]
}

fn phase14_probe_strategy() -> impl Strategy<Value = Phase14Probe> {
    prop_oneof![
        Just(Phase14Probe::Object),
        Just(Phase14Probe::Attributes),
        Just(Phase14Probe::Delete),
    ]
}

fn phase14_trace_seed_strategy() -> impl Strategy<Value = Phase14TraceSeed> {
    prop_oneof![
        any::<u8>().prop_map(|choice| Phase14TraceSeed::Transition { choice }),
        phase14_read_policy_strategy().prop_map(Phase14TraceSeed::ReadPolicy),
        phase14_delete_policy_strategy().prop_map(Phase14TraceSeed::DeletePolicy),
        Just(Phase14TraceSeed::OwnerPutCurrent),
        Just(Phase14TraceSeed::OwnerDelete),
    ]
}

fn phase14_trace_strategy() -> impl Strategy<Value = (Vec<Phase14Mutation>, Phase14Probe)> {
    (
        prop::collection::vec(phase14_trace_seed_strategy(), 0..=6),
        phase14_probe_strategy(),
    )
        .prop_map(|(seeds, probe)| {
            let mut state = Phase14State::new();
            let mut mutations = Vec::with_capacity(seeds.len());
            for seed in seeds {
                let mutation = match seed {
                    Phase14TraceSeed::Transition { choice } => {
                        let legal_targets = state.legal_versioning_targets();
                        let next = legal_targets[(choice as usize) % legal_targets.len()];
                        Phase14Mutation::SetVersioning(next)
                    }
                    Phase14TraceSeed::ReadPolicy(policy) => Phase14Mutation::ReadPolicy(policy),
                    Phase14TraceSeed::DeletePolicy(policy) => Phase14Mutation::DeletePolicy(policy),
                    Phase14TraceSeed::OwnerPutCurrent => Phase14Mutation::OwnerPutCurrent,
                    Phase14TraceSeed::OwnerDelete => Phase14Mutation::OwnerDelete,
                };
                state.apply(mutation);
                mutations.push(mutation);
            }
            (mutations, probe)
        })
}

fn phase14_execution_trace_strategy() -> impl Strategy<Value = Vec<Phase14Mutation>> {
    prop::collection::vec(phase14_trace_seed_strategy(), 0..=6).prop_map(|seeds| {
        let mut state = Phase14State::new();
        let mut mutations = Vec::with_capacity(seeds.len());
        for seed in seeds {
            let mutation = match seed {
                Phase14TraceSeed::Transition { choice } => {
                    let legal_targets = state.legal_versioning_targets();
                    let next = legal_targets[(choice as usize) % legal_targets.len()];
                    Phase14Mutation::SetVersioning(next)
                }
                Phase14TraceSeed::ReadPolicy(policy) => Phase14Mutation::ReadPolicy(policy),
                Phase14TraceSeed::DeletePolicy(policy) => Phase14Mutation::DeletePolicy(policy),
                Phase14TraceSeed::OwnerPutCurrent => Phase14Mutation::OwnerPutCurrent,
                Phase14TraceSeed::OwnerDelete => Phase14Mutation::OwnerDelete,
            };
            state.apply(mutation);
            mutations.push(mutation);
        }
        mutations
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn prop_boe_modern_trace_matches_model(
        (mutations, probe) in boe_modern_trace_strategy()
    ) {
        let trace = mutations
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        let harness = Phase12Harness::new();
        let bucket = "authz-phase12-prop";
        harness.prepare(bucket);
        let mut state = BoeTraceState::new();

        for mutation in mutations.iter().copied() {
            harness.apply_mutation(bucket, mutation);
            state.apply(mutation);
        }

        let expected = state.expected_outcome(probe);
        let actual = to_boe_trace_outcome(harness.probe(bucket, probe));
        prop_assert_eq!(
            actual,
            expected,
            "phase 12 BOE modern trace mismatch\nprobe: {}\ntrace:\n{}",
            probe,
            trace
        );
    }

    #[test]
    fn prop_boe_versioned_delete_trace_matches_model(
        (mutations, probe) in phase13_trace_strategy()
    ) {
        let trace = mutations
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        let harness = Phase13Harness::new();
        let bucket = "authz-phase13-prop";
        harness.prepare(bucket);
        let mut state = Phase13State::new();

        for mutation in mutations.iter().copied() {
            harness.apply_mutation(bucket, mutation);
            state.apply(mutation);
        }

        let expected = state.expected_outcome(probe);
        let actual = to_phase13_outcome(harness.probe(bucket, probe));
        prop_assert_eq!(
            actual,
            expected,
            "phase 13 BOE versioned trace mismatch\nprobe: {}\ntrace:\n{}",
            probe,
            trace
        );
    }

    #[test]
    fn prop_boe_versioned_delete_execution_trace_matches_model(
        (mutations, probe) in phase13_execution_trace_strategy()
    ) {
        let trace = mutations
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        let harness = Phase13Harness::new();
        let bucket = "authz-phase13-exec-prop";
        harness.prepare(bucket);
        let mut state = Phase13State::new();

        for mutation in mutations.iter().copied() {
            harness.apply_mutation(bucket, mutation);
            state.apply(mutation);
        }

        let expected = state.expected_execution_outcome(probe);
        let actual = to_phase13_execution_outcome(harness.execution_probe(bucket, probe));
        prop_assert_eq!(
            actual,
            expected,
            "phase 13 BOE versioned execution trace mismatch\nprobe: {}\ntrace:\n{}",
            probe,
            trace
        );
    }

    #[test]
    fn prop_boe_versioning_transition_trace_matches_model(
        (mutations, probe) in phase14_trace_strategy()
    ) {
        let trace = mutations
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        let harness = Phase14Harness::new();
        let bucket = "authz-phase14-prop";
        harness.prepare(bucket);
        let mut state = Phase14State::new();

        for mutation in mutations.iter().copied() {
            harness.apply_mutation(bucket, mutation);
            state.apply(mutation);
        }

        let expected = state.expected_outcome(probe);
        let actual = to_phase14_outcome(harness.probe(bucket, probe));
        prop_assert_eq!(
            actual,
            expected,
            "phase 14 BOE versioning trace mismatch\nprobe: {}\ntrace:\n{}",
            probe,
            trace
        );
    }

    #[test]
    fn prop_boe_versioning_transition_execution_trace_matches_model(
        mutations in phase14_execution_trace_strategy()
    ) {
        let trace = mutations
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        let harness = Phase14Harness::new();
        let bucket = "authz-phase14-exec-prop";
        harness.prepare(bucket);
        let mut state = Phase14State::new();

        for mutation in mutations.iter().copied() {
            harness.apply_mutation(bucket, mutation);
            state.apply(mutation);
        }

        let expected = state.expected_execution_outcome();
        let actual = to_phase14_execution_outcome(harness.execution_probe(bucket));
        prop_assert_eq!(
            actual,
            expected,
            "phase 14 BOE versioning execution trace mismatch\ntrace:\n{}",
            trace
        );
    }
}
