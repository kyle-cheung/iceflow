#![allow(dead_code)]

use anyhow::Result;
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

#[derive(Clone, Default)]
struct NamespaceState {
    properties: Arc<Mutex<BTreeMap<String, String>>>,
}

#[derive(Clone, Default)]
struct OAuthState {
    last_body: Arc<Mutex<Option<String>>>,
}

pub struct MockPolarisServer {
    base_uri: String,
    warehouse: String,
    state: Arc<Mutex<BTreeMap<String, NamespaceState>>>,
    oauth: OAuthState,
    _worker: thread::JoinHandle<()>,
}

impl MockPolarisServer {
    pub fn start(warehouse: impl Into<String>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock polaris");
        let base_uri = format!("http://{}", listener.local_addr().expect("local addr"));
        let warehouse = warehouse.into();
        let state = Arc::new(Mutex::new(BTreeMap::new()));
        let oauth = OAuthState::default();
        let state_for_thread = Arc::clone(&state);
        let oauth_for_thread = oauth.clone();
        let warehouse_for_thread = warehouse.clone();
        let worker = thread::spawn(move || {
            for incoming in listener.incoming() {
                let mut stream = match incoming {
                    Ok(stream) => stream,
                    Err(_) => continue,
                };
                let state = Arc::clone(&state_for_thread);
                let oauth = oauth_for_thread.clone();
                if handle_connection(&mut stream, &warehouse_for_thread, state, oauth).is_err() {
                    let _ = respond(&mut stream, 500, "{}");
                }
            }
        });

        Self {
            base_uri,
            warehouse,
            state,
            oauth,
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

    pub fn last_oauth_body(&self) -> Option<String> {
        self.oauth
            .last_body
            .lock()
            .ok()
            .and_then(|body| body.clone())
    }
}

fn handle_connection(
    stream: &mut TcpStream,
    warehouse: &str,
    state: Arc<Mutex<BTreeMap<String, NamespaceState>>>,
    oauth: OAuthState,
) -> Result<()> {
    let request = read_request(stream)?;
    let mut parts = request.split("\r\n\r\n");
    let head = parts.next().unwrap_or_default();
    let body = parts.next().unwrap_or_default();
    let mut lines = head.lines();
    let request_line = lines.next().unwrap_or_default();
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().unwrap_or_default();
    let path = request_parts.next().unwrap_or_default();
    let path_only = path.split('?').next().unwrap_or(path);

    let namespace_get = format!("/api/catalog/v1/{warehouse}/namespaces/orders_events");
    let namespace_create = format!("/api/catalog/v1/{warehouse}/namespaces");
    let namespace_props =
        format!("/api/catalog/v1/{warehouse}/namespaces/orders_events/properties");

    match (method, path_only) {
        ("POST", "/api/catalog/v1/oauth/tokens") => {
            *oauth.last_body.lock().expect("oauth body") = Some(body.to_string());
            respond(stream, 200, "{\"access_token\":\"mock-token\"}")?;
        }
        ("GET", "/api/catalog/v1/config") if path.contains(&format!("warehouse={warehouse}")) => {
            let json =
                format!("{{\"defaults\":{{\"prefix\":\"{warehouse}\"}},\"overrides\":{{}}}}");
            respond(stream, 200, &json)?;
        }
        ("GET", path) if path == namespace_get => {
            let state = state.lock().expect("mock state");
            if let Some(namespace) = state.get("orders_events") {
                let properties = namespace.properties.lock().expect("namespace props");
                let json = format!(
                    "{{\"namespace\":[\"orders_events\"],\"properties\":{}}}",
                    json_object(&properties)
                );
                respond(stream, 200, &json)?;
            } else {
                respond(stream, 404, "{\"error\":{\"message\":\"not found\"}}")?;
            }
        }
        ("POST", path) if path == namespace_create => {
            let namespace = parse_namespace(body).unwrap_or_else(|| "orders_events".to_string());
            let mut state = state.lock().expect("mock state");
            let entry = state
                .entry(namespace.clone())
                .or_insert_with(|| NamespaceState {
                    properties: Arc::new(Mutex::new(BTreeMap::new())),
                });
            if let Some(props) = parse_properties(body) {
                let mut guard = entry.properties.lock().expect("namespace props");
                guard.extend(props);
            }
            let props = entry.properties.lock().expect("namespace props");
            let json = format!(
                "{{\"namespace\":[\"{}\"],\"properties\":{}}}",
                namespace,
                json_object(&props)
            );
            respond(stream, 200, &json)?;
        }
        ("POST", path) if path == namespace_props => {
            let mut state = state.lock().expect("mock state");
            let entry =
                state
                    .entry("orders_events".to_string())
                    .or_insert_with(|| NamespaceState {
                        properties: Arc::new(Mutex::new(BTreeMap::new())),
                    });
            let updates = parse_properties(body).unwrap_or_default();
            let mut guard = entry.properties.lock().expect("namespace props");
            guard.extend(updates);
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
            .map_err(|err| anyhow::Error::msg(err.to_string()))?;
        if read == 0 {
            break;
        }
        buffer.push(byte[0]);
        if buffer.len() >= 4 && &buffer[buffer.len() - 4..] == b"\r\n\r\n" {
            header_end = Some(buffer.len());
            break;
        }
    }

    let header_end = header_end.ok_or_else(|| anyhow::Error::msg("missing request headers"))?;
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
            .map_err(|err| anyhow::Error::msg(err.to_string()))?;
        buffer.extend_from_slice(&remaining);
    }

    String::from_utf8(buffer).map_err(|err| anyhow::Error::msg(err.to_string()))
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
        .map_err(|err| anyhow::Error::msg(err.to_string()))?;
    Ok(())
}

fn parse_namespace(body: &str) -> Option<String> {
    let value: serde_json_ext::Value = serde_json_ext::from_str(body).ok()?;
    value
        .get("namespace")?
        .as_array()?
        .first()?
        .as_str()
        .map(|value| value.to_string())
}

fn parse_properties(body: &str) -> Option<BTreeMap<String, String>> {
    let mut map = BTreeMap::new();
    let value: serde_json_ext::Value = serde_json_ext::from_str(body).ok()?;
    let updates = value.get("updates")?.as_object()?;
    for (key, value) in updates {
        let value = value.as_str()?.to_string();
        map.insert(key.clone(), value);
    }
    Some(map)
}

fn json_object(map: &BTreeMap<String, String>) -> String {
    let mut entries = Vec::new();
    for (key, value) in map {
        entries.push(format!("\"{}\":\"{}\"", key, value));
    }
    format!("{{{}}}", entries.join(","))
}
