use anyhow::{Result, bail};
use cftp_proto::{
    Abort, ClientHello, ClientHelloV3, ClientHelloV4, ClientMessage, ClientMessageV3,
    ClientMessageV4, Complete, Done, HardwareProfile, ManifestEntry, PROTOCOL_VERSION_V2,
    PROTOCOL_VERSION_V3, PROTOCOL_VERSION_V4, ServerHello, ServerHelloV3, ServerHelloV4,
    ServerManifest, ServerMessage, ServerMessageV3, ServerMessageV4, ServerProgress, TransferMode,
};
use quinn::{Connection, RecvStream, SendStream};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum WireVersion {
    V2,
    V3,
    V4,
}

impl WireVersion {
    /// True for versions that understand hardware profiles and the v3/v4
    /// control messages.
    pub fn has_profiles(self) -> bool {
        self >= Self::V3
    }

    pub fn number(self) -> u16 {
        match self {
            Self::V2 => PROTOCOL_VERSION_V2,
            Self::V3 => PROTOCOL_VERSION_V3,
            Self::V4 => PROTOCOL_VERSION_V4,
        }
    }
}

pub fn negotiated_version(connection: &Connection) -> WireVersion {
    let protocol: Option<Vec<u8>> = connection
        .handshake_data()
        .and_then(
            |data: Box<dyn std::any::Any>| -> Option<Box<quinn::crypto::rustls::HandshakeData>> {
                data.downcast::<quinn::crypto::rustls::HandshakeData>().ok()
            },
        )
        .and_then(
            |data: Box<quinn::crypto::rustls::HandshakeData>| -> Option<Vec<u8>> { data.protocol },
        );
    match protocol.as_deref() {
        Some(cftp_proto::ALPN_V4) => WireVersion::V4,
        Some(cftp_proto::ALPN_V3) => WireVersion::V3,
        _ => WireVersion::V2,
    }
}

#[derive(Debug, Clone)]
pub struct HelloInfo {
    pub version: u16,
    pub mode: TransferMode,
    pub destination: String,
    pub entries: Vec<ManifestEntry>,
    pub profile: HardwareProfile,
}

#[derive(Debug, Clone)]
pub struct ServerHelloInfo {
    pub version: u16,
    pub accepted: bool,
    pub message: String,
    pub effective_profile: HardwareProfile,
    pub server_cap: HardwareProfile,
    pub profile_accepted: bool,
    pub allow_push: bool,
    pub allow_pull: bool,
}

#[derive(Debug, Clone)]
pub enum IncomingClient {
    Hello(HelloInfo),
    Complete(Complete),
    Abort(Abort),
    SetProfile {
        request_id: u64,
        profile: HardwareProfile,
    },
    Ready,
}

#[derive(Debug, Clone)]
pub enum IncomingServer {
    Hello(ServerHelloInfo),
    Manifest(ServerManifest),
    Done(Done),
    Error(Abort),
    Progress(ServerProgress),
    ProfileChanged {
        request_id: u64,
        accepted: bool,
        effective_profile: HardwareProfile,
        server_cap: HardwareProfile,
        message: String,
    },
}

pub async fn write_client_hello(
    send: &mut SendStream,
    wire: WireVersion,
    hello: &HelloInfo,
) -> Result<()> {
    match wire {
        WireVersion::V2 => {
            cftp_proto::write_frame(
                send,
                &ClientMessage::Hello(ClientHello {
                    version: PROTOCOL_VERSION_V2,
                    mode: hello.mode,
                    destination: hello.destination.clone(),
                    entries: hello.entries.clone(),
                }),
            )
            .await?;
        }
        WireVersion::V3 => {
            cftp_proto::write_frame(
                send,
                &ClientMessageV3::Hello(ClientHelloV3 {
                    version: PROTOCOL_VERSION_V3,
                    mode: hello.mode,
                    destination: hello.destination.clone(),
                    entries: hello.entries.clone(),
                    profile: hello.profile,
                }),
            )
            .await?;
        }
        WireVersion::V4 => {
            cftp_proto::write_frame(
                send,
                &ClientMessageV4::Hello(ClientHelloV4 {
                    version: PROTOCOL_VERSION_V4,
                    mode: hello.mode,
                    destination: hello.destination.clone(),
                    entries: hello.entries.clone(),
                    profile: hello.profile,
                }),
            )
            .await?;
        }
    }
    Ok(())
}

pub async fn read_client_hello(recv: &mut RecvStream, wire: WireVersion) -> Result<HelloInfo> {
    match wire {
        WireVersion::V2 => match cftp_proto::read_frame::<_, ClientMessage>(recv).await? {
            ClientMessage::Hello(hello) => {
                if hello.version != PROTOCOL_VERSION_V2 {
                    bail!(
                        "protocol version mismatch: client {}, server {}",
                        hello.version,
                        PROTOCOL_VERSION_V2
                    );
                }
                Ok(HelloInfo {
                    version: hello.version,
                    mode: hello.mode,
                    destination: hello.destination,
                    entries: hello.entries,
                    profile: HardwareProfile::Default,
                })
            }
            ClientMessage::Abort(abort) => bail!("client aborted: {}", abort.message),
            ClientMessage::Complete(_) => bail!("client completed before sending a manifest"),
        },
        WireVersion::V3 => match cftp_proto::read_frame::<_, ClientMessageV3>(recv).await? {
            ClientMessageV3::Hello(hello) => {
                if hello.version != PROTOCOL_VERSION_V3 {
                    bail!(
                        "protocol version mismatch: client {}, server {}",
                        hello.version,
                        PROTOCOL_VERSION_V3
                    );
                }
                Ok(HelloInfo {
                    version: hello.version,
                    mode: hello.mode,
                    destination: hello.destination,
                    entries: hello.entries,
                    profile: hello.profile,
                })
            }
            ClientMessageV3::Abort(abort) => bail!("client aborted: {}", abort.message),
            ClientMessageV3::Complete(_) => bail!("client completed before sending a manifest"),
            ClientMessageV3::SetProfile { .. } => {
                bail!("client set a profile before sending a manifest")
            }
        },
        WireVersion::V4 => match cftp_proto::read_frame::<_, ClientMessageV4>(recv).await? {
            ClientMessageV4::Hello(hello) => {
                if hello.version != PROTOCOL_VERSION_V4 {
                    bail!(
                        "protocol version mismatch: client {}, server {}",
                        hello.version,
                        PROTOCOL_VERSION_V4
                    );
                }
                Ok(HelloInfo {
                    version: hello.version,
                    mode: hello.mode,
                    destination: hello.destination,
                    entries: hello.entries,
                    profile: hello.profile,
                })
            }
            ClientMessageV4::Abort(abort) => bail!("client aborted: {}", abort.message),
            ClientMessageV4::Complete(_) => bail!("client completed before sending a manifest"),
            ClientMessageV4::SetProfile { .. } => {
                bail!("client set a profile before sending a manifest")
            }
            ClientMessageV4::Ready => bail!("client sent ready before a manifest"),
        },
    }
}

pub async fn write_server_hello(
    send: &mut SendStream,
    wire: WireVersion,
    hello: &ServerHelloInfo,
) -> Result<()> {
    match wire {
        WireVersion::V2 => {
            cftp_proto::write_frame(
                send,
                &ServerMessage::Hello(ServerHello {
                    version: PROTOCOL_VERSION_V2,
                    accepted: hello.accepted,
                    message: hello.message.clone(),
                }),
            )
            .await?;
        }
        WireVersion::V3 => {
            cftp_proto::write_frame(
                send,
                &ServerMessageV3::Hello(ServerHelloV3 {
                    version: PROTOCOL_VERSION_V3,
                    accepted: hello.accepted,
                    message: hello.message.clone(),
                    effective_profile: hello.effective_profile,
                    server_cap: hello.server_cap,
                    profile_accepted: hello.profile_accepted,
                }),
            )
            .await?;
        }
        WireVersion::V4 => {
            cftp_proto::write_frame(
                send,
                &ServerMessageV4::Hello(ServerHelloV4 {
                    version: PROTOCOL_VERSION_V4,
                    accepted: hello.accepted,
                    message: hello.message.clone(),
                    effective_profile: hello.effective_profile,
                    server_cap: hello.server_cap,
                    profile_accepted: hello.profile_accepted,
                    allow_push: hello.allow_push,
                    allow_pull: hello.allow_pull,
                }),
            )
            .await?;
        }
    }
    Ok(())
}

pub async fn read_server_hello(
    recv: &mut RecvStream,
    wire: WireVersion,
) -> Result<ServerHelloInfo> {
    match wire {
        WireVersion::V2 => match cftp_proto::read_frame::<_, ServerMessage>(recv).await? {
            ServerMessage::Hello(hello) => {
                if hello.version != PROTOCOL_VERSION_V2 {
                    bail!(
                        "protocol version mismatch: server {}, client {}",
                        hello.version,
                        PROTOCOL_VERSION_V2
                    );
                }
                Ok(ServerHelloInfo {
                    version: hello.version,
                    accepted: hello.accepted,
                    message: hello.message,
                    effective_profile: HardwareProfile::Default,
                    server_cap: HardwareProfile::Default,
                    profile_accepted: true,
                    allow_push: true,
                    allow_pull: false,
                })
            }
            ServerMessage::Error(err) => bail!("server error: {}", err.message),
            ServerMessage::Progress(_) => {
                bail!("server sent progress before accepting the transfer")
            }
            ServerMessage::Done(_) => bail!("server completed before receiving data"),
        },
        WireVersion::V3 => match cftp_proto::read_frame::<_, ServerMessageV3>(recv).await? {
            ServerMessageV3::Hello(hello) => {
                if hello.version != PROTOCOL_VERSION_V3 {
                    bail!(
                        "protocol version mismatch: server {}, client {}",
                        hello.version,
                        PROTOCOL_VERSION_V3
                    );
                }
                Ok(ServerHelloInfo {
                    version: hello.version,
                    accepted: hello.accepted,
                    message: hello.message,
                    effective_profile: hello.effective_profile,
                    server_cap: hello.server_cap,
                    profile_accepted: hello.profile_accepted,
                    allow_push: true,
                    allow_pull: false,
                })
            }
            ServerMessageV3::Error(err) => bail!("server error: {}", err.message),
            ServerMessageV3::Progress(_) => {
                bail!("server sent progress before accepting the transfer")
            }
            ServerMessageV3::Done(_) => bail!("server completed before receiving data"),
            ServerMessageV3::ProfileChanged { .. } => {
                bail!("server changed profile before accepting the transfer")
            }
        },
        WireVersion::V4 => match cftp_proto::read_frame::<_, ServerMessageV4>(recv).await? {
            ServerMessageV4::Hello(hello) => {
                if hello.version != PROTOCOL_VERSION_V4 {
                    bail!(
                        "protocol version mismatch: server {}, client {}",
                        hello.version,
                        PROTOCOL_VERSION_V4
                    );
                }
                Ok(ServerHelloInfo {
                    version: hello.version,
                    accepted: hello.accepted,
                    message: hello.message,
                    effective_profile: hello.effective_profile,
                    server_cap: hello.server_cap,
                    profile_accepted: hello.profile_accepted,
                    allow_push: hello.allow_push,
                    allow_pull: hello.allow_pull,
                })
            }
            ServerMessageV4::Error(err) => bail!("server error: {}", err.message),
            ServerMessageV4::Progress(_) => {
                bail!("server sent progress before accepting the transfer")
            }
            ServerMessageV4::Done(_) => bail!("server completed before receiving data"),
            ServerMessageV4::Manifest(_) => {
                bail!("server sent a manifest before accepting the transfer")
            }
            ServerMessageV4::ProfileChanged { .. } => {
                bail!("server changed profile before accepting the transfer")
            }
        },
    }
}

pub async fn read_client_message(
    recv: &mut RecvStream,
    wire: WireVersion,
) -> Result<IncomingClient> {
    match wire {
        WireVersion::V2 => match cftp_proto::read_frame::<_, ClientMessage>(recv).await? {
            ClientMessage::Hello(_) => bail!("client sent a second hello"),
            ClientMessage::Complete(complete) => Ok(IncomingClient::Complete(complete)),
            ClientMessage::Abort(abort) => Ok(IncomingClient::Abort(abort)),
        },
        WireVersion::V3 => match cftp_proto::read_frame::<_, ClientMessageV3>(recv).await? {
            ClientMessageV3::Hello(_) => bail!("client sent a second hello"),
            ClientMessageV3::Complete(complete) => Ok(IncomingClient::Complete(complete)),
            ClientMessageV3::Abort(abort) => Ok(IncomingClient::Abort(abort)),
            ClientMessageV3::SetProfile {
                request_id,
                profile,
            } => Ok(IncomingClient::SetProfile {
                request_id,
                profile,
            }),
        },
        WireVersion::V4 => match cftp_proto::read_frame::<_, ClientMessageV4>(recv).await? {
            ClientMessageV4::Hello(_) => bail!("client sent a second hello"),
            ClientMessageV4::Complete(complete) => Ok(IncomingClient::Complete(complete)),
            ClientMessageV4::Abort(abort) => Ok(IncomingClient::Abort(abort)),
            ClientMessageV4::SetProfile {
                request_id,
                profile,
            } => Ok(IncomingClient::SetProfile {
                request_id,
                profile,
            }),
            ClientMessageV4::Ready => Ok(IncomingClient::Ready),
        },
    }
}

pub async fn read_server_message(
    recv: &mut RecvStream,
    wire: WireVersion,
) -> Result<IncomingServer> {
    match wire {
        WireVersion::V2 => match cftp_proto::read_frame::<_, ServerMessage>(recv).await? {
            ServerMessage::Hello(_) => bail!("unexpected server hello"),
            ServerMessage::Done(done) => Ok(IncomingServer::Done(done)),
            ServerMessage::Error(err) => Ok(IncomingServer::Error(err)),
            ServerMessage::Progress(progress) => Ok(IncomingServer::Progress(progress)),
        },
        WireVersion::V3 => match cftp_proto::read_frame::<_, ServerMessageV3>(recv).await? {
            ServerMessageV3::Hello(_) => bail!("unexpected server hello"),
            ServerMessageV3::Done(done) => Ok(IncomingServer::Done(done)),
            ServerMessageV3::Error(err) => Ok(IncomingServer::Error(err)),
            ServerMessageV3::Progress(progress) => Ok(IncomingServer::Progress(progress)),
            ServerMessageV3::ProfileChanged {
                request_id,
                accepted,
                effective_profile,
                server_cap,
                message,
            } => Ok(IncomingServer::ProfileChanged {
                request_id,
                accepted,
                effective_profile,
                server_cap,
                message,
            }),
        },
        WireVersion::V4 => match cftp_proto::read_frame::<_, ServerMessageV4>(recv).await? {
            ServerMessageV4::Hello(_) => bail!("unexpected server hello"),
            ServerMessageV4::Manifest(manifest) => Ok(IncomingServer::Manifest(manifest)),
            ServerMessageV4::Done(done) => Ok(IncomingServer::Done(done)),
            ServerMessageV4::Error(err) => Ok(IncomingServer::Error(err)),
            ServerMessageV4::Progress(progress) => Ok(IncomingServer::Progress(progress)),
            ServerMessageV4::ProfileChanged {
                request_id,
                accepted,
                effective_profile,
                server_cap,
                message,
            } => Ok(IncomingServer::ProfileChanged {
                request_id,
                accepted,
                effective_profile,
                server_cap,
                message,
            }),
        },
    }
}

pub async fn write_complete(
    send: &mut SendStream,
    wire: WireVersion,
    complete: Complete,
) -> Result<()> {
    match wire {
        WireVersion::V2 => {
            cftp_proto::write_frame(send, &ClientMessage::Complete(complete)).await?
        }
        WireVersion::V3 => {
            cftp_proto::write_frame(send, &ClientMessageV3::Complete(complete)).await?
        }
        WireVersion::V4 => {
            cftp_proto::write_frame(send, &ClientMessageV4::Complete(complete)).await?
        }
    }
    Ok(())
}

pub async fn write_client_abort(
    send: &mut SendStream,
    wire: WireVersion,
    abort: Abort,
) -> Result<()> {
    match wire {
        WireVersion::V2 => cftp_proto::write_frame(send, &ClientMessage::Abort(abort)).await?,
        WireVersion::V3 => cftp_proto::write_frame(send, &ClientMessageV3::Abort(abort)).await?,
        WireVersion::V4 => cftp_proto::write_frame(send, &ClientMessageV4::Abort(abort)).await?,
    }
    Ok(())
}

pub async fn write_client_ready(send: &mut SendStream, wire: WireVersion) -> Result<()> {
    if wire != WireVersion::V4 {
        bail!("pull requires cftp/4");
    }
    cftp_proto::write_frame(send, &ClientMessageV4::Ready).await?;
    Ok(())
}

pub async fn write_set_profile(
    send: &mut SendStream,
    wire: WireVersion,
    request_id: u64,
    profile: HardwareProfile,
) -> Result<()> {
    match wire {
        WireVersion::V2 => bail!("profiles require cftp/3 or newer"),
        WireVersion::V3 => {
            cftp_proto::write_frame(
                send,
                &ClientMessageV3::SetProfile {
                    request_id,
                    profile,
                },
            )
            .await?;
        }
        WireVersion::V4 => {
            cftp_proto::write_frame(
                send,
                &ClientMessageV4::SetProfile {
                    request_id,
                    profile,
                },
            )
            .await?;
        }
    }
    Ok(())
}

pub async fn write_server_progress(
    send: &mut SendStream,
    wire: WireVersion,
    progress: ServerProgress,
) -> Result<()> {
    match wire {
        WireVersion::V2 => {
            cftp_proto::write_frame(send, &ServerMessage::Progress(progress)).await?
        }
        WireVersion::V3 => {
            cftp_proto::write_frame(send, &ServerMessageV3::Progress(progress)).await?
        }
        WireVersion::V4 => {
            cftp_proto::write_frame(send, &ServerMessageV4::Progress(progress)).await?
        }
    }
    Ok(())
}

pub async fn write_server_done(send: &mut SendStream, wire: WireVersion, done: Done) -> Result<()> {
    match wire {
        WireVersion::V2 => cftp_proto::write_frame(send, &ServerMessage::Done(done)).await?,
        WireVersion::V3 => cftp_proto::write_frame(send, &ServerMessageV3::Done(done)).await?,
        WireVersion::V4 => cftp_proto::write_frame(send, &ServerMessageV4::Done(done)).await?,
    }
    Ok(())
}

pub async fn write_server_manifest(
    send: &mut SendStream,
    wire: WireVersion,
    manifest: ServerManifest,
) -> Result<()> {
    if wire != WireVersion::V4 {
        bail!("manifests require cftp/4");
    }
    cftp_proto::write_frame(send, &ServerMessageV4::Manifest(manifest)).await?;
    Ok(())
}

pub async fn write_server_error(
    send: &mut SendStream,
    wire: WireVersion,
    abort: Abort,
) -> Result<()> {
    match wire {
        WireVersion::V2 => cftp_proto::write_frame(send, &ServerMessage::Error(abort)).await?,
        WireVersion::V3 => cftp_proto::write_frame(send, &ServerMessageV3::Error(abort)).await?,
        WireVersion::V4 => cftp_proto::write_frame(send, &ServerMessageV4::Error(abort)).await?,
    }
    Ok(())
}

pub async fn write_profile_changed(
    send: &mut SendStream,
    wire: WireVersion,
    request_id: u64,
    accepted: bool,
    effective_profile: HardwareProfile,
    server_cap: HardwareProfile,
    message: String,
) -> Result<()> {
    match wire {
        WireVersion::V2 => bail!("profiles require cftp/3 or newer"),
        WireVersion::V3 => {
            cftp_proto::write_frame(
                send,
                &ServerMessageV3::ProfileChanged {
                    request_id,
                    accepted,
                    effective_profile,
                    server_cap,
                    message,
                },
            )
            .await?;
        }
        WireVersion::V4 => {
            cftp_proto::write_frame(
                send,
                &ServerMessageV4::ProfileChanged {
                    request_id,
                    accepted,
                    effective_profile,
                    server_cap,
                    message,
                },
            )
            .await?;
        }
    }
    Ok(())
}
