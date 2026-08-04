use escapement_serve::http;
use escapement_serve::{Orchestrator, OrchestratorConfig};

fn main() {
    let addr = std::env::var("ESCAPEMENT_ADDR").unwrap_or_else(|_| "0.0.0.0:7858".to_string());
    let otlp = std::env::var("OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4318/v1/traces".to_string());
    let config = OrchestratorConfig {
        otlp_endpoint: otlp,
        ..OrchestratorConfig::default()
    };
    let orch = Orchestrator::new(config);
    println!("escapement-serve v1.1.0 starting on {addr}...");
    println!("OTLP endpoint: {}", orch.config_ref().otlp_endpoint);
    if let Err(e) = http::serve(&addr, orch) {
        eprintln!("escapement-serve failed: {e}");
        std::process::exit(1);
    }
}
