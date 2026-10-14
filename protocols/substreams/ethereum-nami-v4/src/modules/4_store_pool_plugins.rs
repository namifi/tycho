use substreams::store::{StoreNew, StoreSet, StoreSetString};

use crate::pb::nami::v4::{events::pool_event::Typ, Events};

/// Tracks each pool's current plugin address (hex), keyed by pool address (hex).
#[substreams::handlers::store]
pub fn store_pool_plugins(events: Events, store: StoreSetString) {
    for event in events.pool_events {
        let Some(Typ::Plugin(plugin)) = event.typ else {
            continue;
        };
        store.set(
            event.log_ordinal,
            hex::encode(&event.pool_address),
            &hex::encode(&plugin.plugin),
        );
    }
}
