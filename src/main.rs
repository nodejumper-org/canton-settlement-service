//! A delivery-versus-payment settlement service for the Canton Network.
//!
//! Counterparties agree a settlement off-ledger and register it here; each
//! allocates its side under the CIP-0112 token standard, naming this service
//! as executor; once every leg is authorised on both sides the service
//! settles them all in one transaction. See the README for the full flow.

mod api;
mod config;
mod demo;
mod engine;
mod ledger;
mod model;
mod store;

use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;
use tracing_subscriber::EnvFilter;

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    if std::env::var("SETTLEMENT_LOG_FORMAT").as_deref() == Ok("json") {
        builder.json().init();
    } else {
        builder.init();
    }
}

async fn shutdown_signal() {
    let interrupt = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = interrupt => {}
        () = terminate => {}
    }
    tracing::info!("shutting down");
}

#[tokio::main]
async fn main() -> ExitCode {
    init_tracing();
    let config = match config::Config::from_env() {
        Ok(config) => config,
        Err(error) => {
            tracing::error!(%error, "configuration");
            return ExitCode::from(2);
        }
    };
    tracing::info!(?config, "starting");

    let metrics = match metrics_exporter_prometheus::PrometheusBuilder::new().install_recorder() {
        Ok(handle) => Some(handle),
        Err(error) => {
            tracing::warn!(%error, "metrics are off");
            None
        }
    };

    let store = match store::Store::open(&config.database) {
        Ok(store) => store,
        Err(error) => {
            tracing::error!(%error, path = %config.database.display(), "cannot open the database");
            return ExitCode::FAILURE;
        }
    };

    // The registry and the participant may come up after this service does;
    // wait for them rather than exiting into a restart loop.
    let ledger = loop {
        match ledger::Ledger::connect(&config).await {
            Ok(ledger) => break ledger,
            Err(error) => {
                tracing::warn!(%error, "cannot reach the registry or the participant yet; retrying");
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        }
    };
    tracing::info!(executor = %ledger.executor(), registry_admin = %ledger.registry_admin(), "connected");

    let doorbell = Arc::new(Notify::new());
    let observations = engine::Observations::default();

    tokio::spawn(ledger.clone().watch(store.clone(), Arc::clone(&doorbell)));
    tokio::spawn(
        engine::Engine {
            ledger: ledger.clone(),
            store: store.clone(),
            doorbell: Arc::clone(&doorbell),
            observations: observations.clone(),
            reconcile_every: config.reconcile_every,
        }
        .run(),
    );
    if let Some(demo) = config.demo.clone() {
        tracing::info!(every = ?demo.every, "demo traders on");
        tokio::spawn(
            demo::Demo {
                ledger: ledger.clone(),
                store: store.clone(),
                doorbell: Arc::clone(&doorbell),
                config: demo,
            }
            .run(),
        );
    }

    let state = Arc::new(api::AppState {
        store,
        observations,
        doorbell,
        executor: ledger.executor().clone(),
        registry_admin: ledger.registry_admin().clone(),
        ledger: Some(ledger),
        api_key: config.api_key.clone(),
        default_ttl: config.default_ttl,
        metrics,
    });
    let listener = match tokio::net::TcpListener::bind(config.listen).await {
        Ok(listener) => listener,
        Err(error) => {
            tracing::error!(%error, listen = %config.listen, "cannot listen");
            return ExitCode::FAILURE;
        }
    };
    tracing::info!(listen = %config.listen, "serving");
    if let Err(error) = axum::serve(listener, api::router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await
    {
        tracing::error!(%error, "server");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
