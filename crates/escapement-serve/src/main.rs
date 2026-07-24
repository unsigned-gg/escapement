use escapement_serve::http;
use escapement_serve::{Orchestrator, OrchestratorConfig};

fn main() {
    let addr = std::env::var("ESCAPEMENT_ADDR").unwrap_or_else(|_| "0.0.0.0:7858".to_string());
    let config = OrchestratorConfig::default();
    let orch = Orchestrator::new(config);
    println!("escapement-serve v1.0.0 starting on {addr}...");
    if let Err(e) = http::serve(&addr, orch) {
        eprintln!("escapement-serve failed: {e}");
        std::process::exit(1);
    }
}
