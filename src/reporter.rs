//! Reporter to the [jaeger agent]
//!
//! [jaeger agent]: http://jaeger.readthedocs.io/en/latest/deployment/#agent
use std::net::{SocketAddr, UdpSocket};
use hostname;
use reqwest::Client;
use rustracing::tag::Tag;
use thrift_codec::{BinaryEncode, CompactEncode};
use thrift_codec::data::Struct;
use thrift_codec::message::Message;
use trackable::error::ErrorKindExt;
use url::{HostAndPort, Url};

use {Result, ErrorKind};
use constants;
use error;
use span::FinishedSpan;
use thrift::{agent, jaeger};

/// Reporter for the agent which accepts jaeger.thrift over compact thrift protocol.
#[derive(Debug)]
pub struct JaegerCompactReporter(JaegerReporter);
impl JaegerCompactReporter {
    /// Makes a new `JaegerCompactReporter` instance.
    ///
    /// # Errors
    ///
    /// If the UDP socket used to report spans can not be bound to `127.0.0.1:0`,
    /// it will return an error which has the kind `ErrorKind::Other`.
    pub fn new(service_name: &str) -> Result<Self> {
        let inner = track!(JaegerReporter::new(service_name, 6831))?;
        Ok(JaegerCompactReporter(inner))
    }

    /// Sets the address of the report destination agent to `addr`.
    ///
    /// The default address is `127.0.0.1:6831`.
    pub fn set_agent_addr(&mut self, addr: SocketAddr) {
        self.0.set_agent_addr(addr);
    }

    /// Adds `tag` to this service.
    pub fn add_service_tag(&mut self, tag: Tag) {
        self.0.add_service_tag(tag);
    }

    /// Reports `spans`.
    ///
    /// # Errors
    ///
    /// If it fails to encode `spans` to the thrift compact format (i.e., a bug of this crate),
    /// this method will return an error which has the kind `ErrorKind::InvalidInput`.
    ///
    /// If it fails to send the encoded binary to the jaeger agent via UDP,
    /// this method will return an error which has the kind `ErrorKind::Other`.
    pub fn report(&self, spans: &[FinishedSpan]) -> Result<()> {
        track!(self.0.report(spans, |message| {
            let mut bytes = Vec::new();
            track!(
                message
                    .compact_encode(&mut bytes,)
                    .map_err(error::from_thrift_error,)
            )?;
            Ok(bytes)
        }))
    }
}

/// Reporter for the agent which accepts jaeger.thrift over binary thrift protocol.
#[derive(Debug)]
pub struct JaegerBinaryReporter(JaegerReporter);
impl JaegerBinaryReporter {
    /// Makes a new `JaegerBinaryReporter` instance.
    ///
    /// # Errors
    ///
    /// If the UDP socket used to report spans can not be bound to `127.0.0.1:0`,
    /// it will return an error which has the kind `ErrorKind::Other`.
    pub fn new(service_name: &str) -> Result<Self> {
        let inner = track!(JaegerReporter::new(service_name, 6832))?;
        Ok(JaegerBinaryReporter(inner))
    }

    /// Sets the address of the report destination agent to `addr`.
    ///
    /// The default address is `127.0.0.1:6832`.
    pub fn set_agent_addr(&mut self, addr: SocketAddr) {
        self.0.set_agent_addr(addr);
    }

    /// Adds `tag` to this service.
    pub fn add_service_tag(&mut self, tag: Tag) {
        self.0.add_service_tag(tag);
    }

    /// Reports `spans`.
    ///
    /// # Errors
    ///
    /// If it fails to encode `spans` to the thrift binary format (i.e., a bug of this crate),
    /// this method will return an error which has the kind `ErrorKind::InvalidInput`.
    ///
    /// If it fails to send the encoded binary to the jaeger agent via UDP,
    /// this method will return an error which has the kind `ErrorKind::Other`.
    pub fn report(&self, spans: &[FinishedSpan]) -> Result<()> {
        track!(self.0.report(spans, |message| {
            let mut bytes = Vec::new();
            track!(
                message
                    .binary_encode(&mut bytes,)
                    .map_err(error::from_thrift_error,)
            )?;
            Ok(bytes)
        }))
    }
}

/// Reporter for the collector which accepts jaeger.thrift over binary thrift protocol as a HTTP
/// POST payload.
#[derive(Debug)]
pub struct JaegerHttpReporter {
    client: Client,
    url: Url,
    batcher: JaegerBatcher,
}
impl JaegerHttpReporter {
    /// Makes a new `JaegerHttpReporter` instance.
    ///
    /// # Errors
    ///
    /// Cannot currently fail, but reservers the right to do so in the future
    /// (eg if more proactive/aggressive connection pooling is used in the future).
    pub fn new(service_name: &str) -> Result<Self> {
        let client = Client::new();
        let batcher = JaegerBatcher::new(service_name);
        let url = "http://127.0.0.1:14268/api/traces?format=jaeger.thrift"
            .parse()
            .expect("failed to parse default URL");
        Ok(JaegerHttpReporter{
            client,
            url,
            batcher,
        })
    }

    /// Sets the host and port of the report destination collector to `host_and_port`.
    ///
    /// The default host and port are `127.0.0.1:14268`.
    pub fn set_collector_host_and_port(&mut self, host_and_port: HostAndPort<&str>) {
        use url::Host::*;
        use std::net::IpAddr::*;

        // shouldn't fail because existing URL is not "cannot-be-a-base" and host is already parsed
        let msg = "failed to set collector host";
        match host_and_port.host {
            Domain(s) => self.url.set_host(Some(s)).expect(msg),
            Ipv4(a) => self.url.set_ip_host(V4(a)).expect(msg),
            Ipv6(a) => self.url.set_ip_host(V6(a)).expect(msg),
        }

        // shouldn't fail because URL has a host and doesn't have the "file" scheme
        self.url.set_port(Some(host_and_port.port)).expect("failed to set collector port");
    }

    /// Adds `tag` to this service.
    pub fn add_service_tag(&mut self, tag: Tag) {
        self.batcher.add_service_tag(tag);
    }

    /// Reports `spans`.
    ///
    /// # Errors
    ///
    /// If it fails to encode `spans` to the thrift binary format (i.e., a bug of this crate),
    /// this method will return an error which has the kind `ErrorKind::InvalidInput`.
    ///
    /// If it fails to send the encoded binary to the jaeger collector via HTTP,
    /// this method will return an error which has the kind `ErrorKind::Other`.
    pub fn report(&self, spans: &[FinishedSpan]) -> Result<()> {
        let batch = self.batcher.batch(spans,);
        let mut bytes = Vec::new();
        track!(Struct::from(batch,).binary_encode(&mut bytes,)
                    .map_err(error::from_thrift_error,))?;
        let mut response = track!(
            self.client
                .post(self.url.clone(),)
                .body(bytes,)
                .send()
                .map_err(error::from_reqwest_error,)
        )?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(ErrorKind::Other.cause(format!(
                "failed to POST spans to collector with response status: '{}', body: '{}'",
                response.status(),
                response.text().unwrap_or_default()
            )).into())
        }
    }
}

#[derive(Debug)]
struct JaegerBatcher(jaeger::Process);
impl JaegerBatcher {
    fn new(service_name: &str) -> Self {
        let process = jaeger::Process {
            service_name: service_name.to_owned(),
            tags: Vec::new(),
        };
        let mut this = JaegerBatcher(process);

        this.add_service_tag(Tag::new(
            constants::JAEGER_CLIENT_VERSION_TAG_KEY,
            constants::JAEGER_CLIENT_VERSION,
        ));
        if let Some(hostname) = hostname::get_hostname() {
            this.add_service_tag(Tag::new(constants::TRACER_HOSTNAME_TAG_KEY, hostname));
        }
        this
    }
    fn add_service_tag(&mut self, tag: Tag) {
        self.0.tags.push((&tag).into());
    }
    fn batch(&self, spans: &[FinishedSpan]) -> jaeger::Batch {
        jaeger::Batch {
            process: self.0.clone(),
            spans: spans.iter().map(From::from).collect(),
        }
    }
}

#[derive(Debug)]
struct JaegerReporter {
    socket: UdpSocket,
    agent: SocketAddr,
    batcher: JaegerBatcher,
}
impl JaegerReporter {
    fn new(service_name: &str, port: u16) -> Result<Self> {
        let socket = track!(UdpSocket::bind("127.0.0.1:0").map_err(error::from_io_error))?;
        let agent = SocketAddr::from(([127, 0, 0, 1], port));
        let batcher = JaegerBatcher::new(service_name);
        Ok(JaegerReporter {
            socket,
            agent,
            batcher,
        })
    }
    fn set_agent_addr(&mut self, addr: SocketAddr) {
        self.agent = addr;
    }
    fn add_service_tag(&mut self, tag: Tag) {
        self.batcher.add_service_tag(tag);
    }
    fn report<F>(&self, spans: &[FinishedSpan], encode: F) -> Result<()>
    where
        F: FnOnce(Message) -> Result<Vec<u8>>,
    {
        let batch = self.batcher.batch(spans);
        let message = Message::from(agent::EmitBatchNotification { batch });
        let bytes = track!(encode(message))?;
        track!(
            self.socket
                .send_to(&bytes, self.agent,)
                .map_err(error::from_io_error,)
        )?;
        Ok(())
    }
}
