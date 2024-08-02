// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use aptos_executor_service::process_executor_service::ProcessExecutorService;
use aptos_logger::info;
use aptos_push_metrics::MetricsPusher;
use clap::Parser;
use std::net::SocketAddr;

#[derive(Debug, Parser)]
struct Args {
    #[clap(long, default_value_t = 8)]
    pub num_executor_threads: usize,

    #[clap(long)]
    pub shard_id: usize,

    #[clap(long)]
    pub num_shards: usize,

    #[clap(long, num_args = 1..)]
    pub remote_executor_addresses: Vec<SocketAddr>,

    #[clap(long)]
    pub coordinator_address: SocketAddr,
}

fn main() {
    let args = Args::parse();
    aptos_logger::Logger::new().init();

    let (tx, rx) = crossbeam_channel::unbounded();
    ctrlc::set_handler(move || {
        tx.send(()).unwrap();
    })
    .expect("Error setting Ctrl-C handler");

    aptos_node_resource_metrics::register_node_metrics_collector();
    let _mp = MetricsPusher::start_for_local_run(
        &("remote-executor-service-".to_owned() + &args.shard_id.to_string()),
    );

    let mut coordinator_address = args.coordinator_address;
    coordinator_address.set_port(coordinator_address.port() + args.shard_id as u16); // adds offset based on shard_id to the port
    let _exe_service = ProcessExecutorService::new(
        args.shard_id,
        args.num_shards,
        args.num_executor_threads,
        args.coordinator_address,
        args.remote_executor_addresses,
    );

    rx.recv()
        .expect("Could not receive Ctrl-C msg from channel.");
    info!("Process executor service shutdown successfully.");
}

#[test]
fn verify_tool() {
    use clap::CommandFactory;
    Args::command().debug_assert()
}
