#![allow(dead_code)]

use anyhow::{Error, Result};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

#[derive(Clone, Default)]
struct NamespaceState {
    properties: Arc<Mutex<BTreeMap<String, String>>>,
}

pub struct MockPolarisServer {
    base_uri: String,
    warehouse: String,
    state: Arc<Mutex<BTreeMap<String, NamespaceState>>>,
    _worker: thread::JoinHandle<()>,
}

impl MockPolarisServer {
    pub fn start(warehouse: impl Into<String>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock polaris");
        let base_uri = format!("http://{}", listener.local_addr().expect("local addr"));
        let warehouse = warehouse.into();
        let state = Arc::new(Mutex::new(BTreeMap::new()));
        let state_for_thread = Arc::clone(&state);
        let warehouse_for_thread = warehouse.clone();
        let worker = thread::spawn(move || {
            for incoming in listener.incoming() {
                let mut stream = match incoming {
                    Ok(stream) => stream,
                    Err(_) => continue,
                };
                let state = Arc::clone(&state_for_thread);
                if handle_connection(&mut stream, &warehouse_for_thread, state).is_err() {
                    let _ = respond(
                        &mut stream,
                        500,
                        "{\"error\":{\"message\":\"mock failure\"}}",
                    );
                }
            }
        });

        Self {
            base_uri,
            warehouse,
            state,
            _worker: worker,
        }
    }

    pub fn catalog_uri(&self) -> String {
        format!("{}/api/catalog", self.base_uri)
    }

    pub fn warehouse(&self) -> &str {
        &self.warehouse
    }

    pub fn namespace_properties(&self, namespace: &str) -> Option<BTreeMap<String, String>> {
        self.state
            .lock()
            .ok()
            .and_then(|state| state.get(namespace).cloned())
            .and_then(|entry| entry.properties.lock().ok().map(|props| props.clone()))
    }
}

fn handle_connection(
    stream: &mut TcpStream,
    warehouse: &str,
    state: Arc<Mutex<BTreeMap<String, NamespaceState>>>,
) -> Result<()> {
    let request = read_request(stream)?;
    let mut parts = request.split("\r\n\r\n");
    let head = parts.next().unwrap_or_default();
    let body = parts.next().unwrap_or_default();
    let request_line = head.lines().next().unwrap_or_default();
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().unwrap_or_default();
    let path = request_parts.next().unwrap_or_default();
    let path_only = path.split('?').next().unwrap_or(path);

    let namespace = "orders_events";
    let namespace_get = format!("/api/catalog/v1/{warehouse}/namespaces/{namespace}");
    let namespace_create = format!("/api/catalog/v1/{warehouse}/namespaces");
    let namespace_props = format!("/api/catalog/v1/{warehouse}/namespaces/{namespace}/properties");

    match (method, path_only) {
        ("GET", "/api/catalog/v1/config") if path.contains(&format!("warehouse={warehouse}")) => {
            let json =
                format!("{{\"defaults\":{{\"prefix\":\"{warehouse}\"}},\"overrides\":{{}}}}");
            respond(stream, 200, &json)?;
        }
        ("GET", current) if current == namespace_get => {
            let state = state.lock().expect("mock state");
            if let Some(namespace_state) = state.get(namespace) {
                let properties = namespace_state
                    .properties
                    .lock()
                    .expect("namespace properties");
                let json = format!(
                    "{{\"namespace\":[\"{namespace}\"],\"properties\":{}}}",
                    json_object(&properties)
                );
                respond(stream, 200, &json)?;
            } else {
                respond(stream, 404, "{\"error\":{\"message\":\"not found\"}}")?;
            }
        }
        ("POST", current) if current == namespace_create => {
            let mut state = state.lock().expect("mock state");
            let namespace_state =
                state
                    .entry(namespace.to_string())
                    .or_insert_with(|| NamespaceState {
                        properties: Arc::new(Mutex::new(BTreeMap::new())),
                    });
            if let Some(updates) = parse_updates(body) {
                let mut properties = namespace_state
                    .properties
                    .lock()
                    .expect("namespace properties");
                properties.extend(updates);
            }
            let properties = namespace_state
                .properties
                .lock()
                .expect("namespace properties");
            let json = format!(
                "{{\"namespace\":[\"{namespace}\"],\"properties\":{}}}",
                json_object(&properties)
            );
            respond(stream, 200, &json)?;
        }
        ("POST", current) if current == namespace_props => {
            let mut state = state.lock().expect("mock state");
            let namespace_state =
                state
                    .entry(namespace.to_string())
                    .or_insert_with(|| NamespaceState {
                        properties: Arc::new(Mutex::new(BTreeMap::new())),
                    });
            let updates = parse_updates(body).unwrap_or_default();
            let mut properties = namespace_state
                .properties
                .lock()
                .expect("namespace properties");
            properties.extend(updates);
            respond(
                stream,
                200,
                "{\"updated\":[],\"removed\":[],\"missing\":[]}",
            )?;
        }
        _ => {
            respond(stream, 404, "{\"error\":{\"message\":\"unknown route\"}}")?;
        }
    }

    Ok(())
}

fn read_request(stream: &mut TcpStream) -> Result<String> {
    let mut buffer = Vec::new();
    let mut header_end = None;
    loop {
        let mut byte = [0u8; 1];
        let read = stream
            .read(&mut byte)
            .map_err(|err| Error::msg(err.to_string()))?;
        if read == 0 {
            break;
        }
        buffer.push(byte[0]);
        if buffer.len() >= 4 && &buffer[buffer.len() - 4..] == b"\r\n\r\n" {
            header_end = Some(buffer.len());
            break;
        }
    }

    let header_end = header_end.ok_or_else(|| Error::msg("missing request headers"))?;
    let headers = String::from_utf8_lossy(&buffer[..header_end]).into_owned();
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            if name.trim().eq_ignore_ascii_case("content-length") {
                value.trim().parse::<usize>().ok()
            } else {
                None
            }
        })
        .unwrap_or(0);
    let body_len = buffer.len().saturating_sub(header_end);
    if body_len < content_length {
        let mut remaining = vec![0u8; content_length - body_len];
        stream
            .read_exact(&mut remaining)
            .map_err(|err| Error::msg(err.to_string()))?;
        buffer.extend_from_slice(&remaining);
    }

    String::from_utf8(buffer).map_err(|err| Error::msg(err.to_string()))
}

fn respond(stream: &mut TcpStream, status: u16, body: &str) -> Result<()> {
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "OK",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(response.as_bytes())
        .map_err(|err| Error::msg(err.to_string()))?;
    Ok(())
}

fn parse_updates(body: &str) -> Option<BTreeMap<String, String>> {
    let value: serde_json::Value = serde_json::from_str(body).ok()?;
    let object = value.get("updates")?.as_object()?;
    let mut updates = BTreeMap::new();
    for (key, value) in object {
        updates.insert(key.clone(), value.as_str()?.to_string());
    }
    Some(updates)
}

fn json_object(map: &BTreeMap<String, String>) -> String {
    let entries = map
        .iter()
        .map(|(key, value)| format!("\"{key}\":\"{value}\""))
        .collect::<Vec<_>>();
    format!("{{{}}}", entries.join(","))
}
