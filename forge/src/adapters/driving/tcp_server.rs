use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

pub struct TcpServer;

const MAX_MESSAGE_SIZE: u32 = 100 * 1024 * 1024;
const API_VERSIONS_KEY: i16 = 18;
const UNSUPPORTED_VERSION_ERROR: i16 = 35;

impl TcpServer {
    pub async fn listen(address: &str) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(address).await?;
        tracing::info!("Server started on {}", address);
        
        let cancel_token = CancellationToken::new();
        let cancel_token_clone = cancel_token.clone();

        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.unwrap();
            tracing::info!("Ctrl+C received, shutting down...");
            cancel_token_clone.cancel();
        });

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((mut socket, _)) => {
                            tracing::info!("New connection from {}", socket.peer_addr()?);
                            let token = cancel_token.clone();
                            tokio::spawn(async move {
                                Self::handle_connection(&mut socket, token).await;
                            });
                        }
                        Err(e) => {
                            tracing::error!("Failed to accept connection: {}", e);
                        }
                    }
                }

                _ = cancel_token.cancelled();
                tracing::info!("Server shutting down...");
                break;
            }
        }

        tracing::info!("Server shut down gracefully");
        Ok(())
    }

    async fn handle_connection(socket: &mut tokio::net::TcpStream, cancel_token: CancellationToken) {
        loop {
            tokio::select! {
                read_result = Self::read_frame(socket) => {
                    match read_result {
                        Ok(Some(body)) => {
                            let mut cursor = std::io::Cursor::new(body);
                            match RequestHeader::decode(&mut cursor) {
                                Ok(header) => {
                                    tracing::info!(
                                        "Received Request - API Key: {}, Version: {}, Correlation ID: {}",
                                        header.api_key,
                                        header.api_version,
                                        header.correlation_id
                                    );

                                    let response_header = ResponseHeader {
                                        correlation_id: header.correlation_id,
                                    };

                                    let mut response_body = BytesMut::new();
                                    response_header.encode(&mut response_body);

                                    match header.api_key {
                                        API_VERSIONS_KEY => {
                                            tracing::info!("Received API Versions request");
                                            response_body.put_i16(0);
                                        }
                                        _ => {
                                            tracing::info!("Unsupported API Key: {}", header.api_key);
                                            response_body.put_i16(UNSUPPORTED_VERSION_ERROR);
                                        }
                                    }

                                    let mut final_packet = BytesMut::new();
                                    final_packet.put_i32(response_body.len() as i32);
                                    final_packet.put_slice(&response_body);
                                    
                                    if let Err(e) = socket.write_all(&final_packet).await {
                                        tracing::error!("Failed to write response: {}", e);
                                        break;
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Failed to decode message: {}", e);
                                    break;
                                }
                            }
                        }
                        Ok(None) => {
                            tracing::info!("Connection closed by client");
                            break;
                        }
                        Err(e) => {
                            tracing::error!("Failed to read frame: {}", e);
                            break;
                        }
                    }
                }

                _ = cancel_token.cancelled();
                tracing::info!("Connection shut down gracefully");
                break;
            }
        }
    }

    async fn read_frame(socket: &mut tokio::net::TcpStream) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        let mut size_buf = [0u8; 4];
        if socket.read_exact(&mut size_buf).await.is_err() {
            return Ok(None);
        }

        let size = u32::from_be_bytes(size_buf);
        if size > MAX_MESSAGE_SIZE {
            tracing::warn!("Request size {} exceeds max allowed size {}", size, MAX_MESSAGE_SIZE);
            return Err("Request size exceeds max allowed size".into());
        }

        let mut body = vec![0u8; size as usize];
        if socket.read_exact(&mut body).await.is_err() {
            return Err("Failed to read request body".into());
        }

        Ok(Some(body))
    }
}
