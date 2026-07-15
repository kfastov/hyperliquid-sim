#![cfg(unix)]

use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    process::{Child, Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

const PROCESS_BOUND: Duration = Duration::from_secs(5);

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn unused_loopback_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve ephemeral address");
    let address = listener.local_addr().expect("local address");
    drop(listener);
    address
}

fn server_command(address: SocketAddr) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_sim-server"));
    command
        .env_clear()
        .env("SIM_BIND_ADDR", address.to_string())
        .env("SIM_ORACLE_MODE", "offline")
        .env("SIM_ACTOR_INTERVAL_MS", "10")
        .env("SIM_REPLY_TIMEOUT_MS", "500")
        .env("SIM_SHUTDOWN_TIMEOUT_MS", "1000");
    command
}

fn wait_for_response(address: SocketAddr, path: &str, expected_status: &str) -> String {
    let deadline = Instant::now() + PROCESS_BOUND;
    loop {
        match TcpStream::connect_timeout(&address, Duration::from_millis(100)) {
            Ok(mut stream) => {
                write!(
                    stream,
                    "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
                )
                .expect("write HTTP request");
                let mut response = String::new();
                stream.read_to_string(&mut response).expect("read HTTP response");
                if response.starts_with(expected_status) {
                    return response;
                }
                assert!(Instant::now() < deadline, "unexpected response: {response}");
                thread::sleep(Duration::from_millis(20));
            }
            Err(_) if Instant::now() < deadline => thread::sleep(Duration::from_millis(20)),
            Err(error) => panic!("HTTP listener did not start: {error}"),
        }
    }
}

fn wait_for_exit(child: &mut Child) -> std::process::ExitStatus {
    let deadline = Instant::now() + PROCESS_BOUND;
    loop {
        if let Some(status) = child.try_wait().expect("poll child") {
            return status;
        }
        assert!(Instant::now() < deadline, "server did not exit within bound");
        thread::sleep(Duration::from_millis(20));
    }
}

fn run_bind_failure(address: SocketAddr) -> Output {
    server_command(address).output().expect("run bind-failure process")
}

#[test]
fn health_sigterm_zero_exit_and_port_reuse() {
    let address = unused_loopback_addr();
    let child = server_command(address)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn server");
    let mut child = ChildGuard(child);

    let response = wait_for_response(address, "/healthz", "HTTP/1.1 200 OK\r\n");
    assert!(response.ends_with("\r\n\r\n{\"status\":\"alive\"}"), "{response}");
    let response = wait_for_response(address, "/readyz", "HTTP/1.1 200 OK\r\n");
    assert!(response.ends_with("\r\n\r\n{\"status\":\"ready\"}"), "{response}");

    let signal = Command::new("kill")
        .args(["-TERM", &child.0.id().to_string()])
        .status()
        .expect("send SIGTERM");
    assert!(signal.success());
    let status = wait_for_exit(&mut child.0);
    assert_eq!(status.code(), Some(0));

    let rebound = TcpListener::bind(address).expect("listener port is released after shutdown");
    drop(rebound);
    assert!(TcpStream::connect_timeout(&address, Duration::from_millis(100)).is_err());
}

#[test]
fn occupied_bind_fails_without_starting_a_service() {
    let occupied = TcpListener::bind("127.0.0.1:0").expect("occupy listener");
    let address = occupied.local_addr().expect("occupied address");
    let output = run_bind_failure(address);

    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr).expect("utf8 stderr");
    assert!(stderr.contains("sim-server listener bind failed:"), "{stderr}");
    assert!(TcpStream::connect_timeout(&address, Duration::from_millis(100)).is_ok());
}
