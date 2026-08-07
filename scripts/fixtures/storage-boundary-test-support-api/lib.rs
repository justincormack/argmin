type CratePrivateLayout = EcShape;

pub mod test_support {
    type PayloadLayout = EcShape;
    use crate::DataPgId as RoutedPg;
    use super::CratePrivateLayout as ImportedLayout;

    pub trait RenamedPayloadScratchObservation {
        fn test_configured_payload_scratch_allocations(
            &self,
            shape: PayloadLayout,
            pg: RoutedPg,
            imported: ImportedLayout,
        ) -> usize;
    }

    mod hidden {
        pub type HiddenRoute = PgRouteSnapshot;
    }

    pub use hidden::HiddenRoute as ExportedRoute;
    pub use hidden::*;

    pub struct RawShardObservation {
        pub key: ShardKey,
    }

    pub enum RawClaimObservation {
        Claimed(LifecycleSweepClaimRecord),
    }

    pub type RawRouteObservation = PgRouteSnapshot;

    pub fn observe_raw_pg(_pg: PgId) {}
}
