//! One live recovery context shared by scheduled, user and operator resumption.
use super::LiveAgentCatalog;
use crate::store::{RecoveryPolicy, RunRoute};
use std::collections::BTreeSet;
use std::sync::{Arc, RwLock};

type RouteResolver = dyn Fn(&str) -> Option<RunRoute> + Send + Sync;
type ReplaySafety = dyn Fn(&str) -> bool + Send + Sync;

pub(crate) struct RecoveryContext {
    max_resume_attempts: u32,
    route: Box<RouteResolver>,
    replay_safe: Box<ReplaySafety>,
}

impl RecoveryContext {
    pub fn new(
        catalog: Arc<RwLock<LiveAgentCatalog>>,
        tools: BTreeSet<String>,
        max_resume_attempts: u32,
    ) -> Self {
        Self {
            max_resume_attempts,
            route: Box::new(move |id| {
                catalog
                    .read()
                    .unwrap_or_else(|p| p.into_inner())
                    .route_of_live(id)
            }),
            replay_safe: Box::new(move |name| tools.contains(name)),
        }
    }

    pub fn policy(&self) -> RecoveryPolicy<'_> {
        RecoveryPolicy {
            max_resume_attempt: self.max_resume_attempts,
            route: &*self.route,
            replay_safe: &*self.replay_safe,
        }
    }
}
