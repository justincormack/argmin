// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

/// Independently versioned protocol identities negotiated by internal TLS
/// transports. These bytes are private wire-format markers: changing one is a
/// protocol-version change for that transport, not a deployment setting.
macro_rules! internal_tls_protocols {
    ($($variant:ident => $alpn:literal),+ $(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub(crate) enum InternalTlsProtocol {
            $($variant),+
        }

        impl InternalTlsProtocol {
            #[cfg(test)]
            pub(crate) const ALL: [Self; internal_tls_protocols!(@count $($variant),+)] = [
                $(Self::$variant),+
            ];

            pub(crate) const fn alpn(self) -> &'static [u8] {
                match self {
                    $(Self::$variant => $alpn),+
                }
            }

            pub(crate) fn is_negotiated(self, actual: Option<&[u8]>) -> bool {
                actual == Some(self.alpn())
            }
        }

    };
    (@count $head:ident $(, $tail:ident)*) => {
        1_usize $(+ internal_tls_protocols!(@one $tail))*
    };
    (@one $variant:ident) => { 1_usize };
}

internal_tls_protocols! {
    StorageRpc => b"argmin-storage-rpc/1",
    ControlPlaneRpc => b"argmin-control-plane/1",
    RaftPeer => b"argmin-raft/1",
}

#[cfg(test)]
pub(crate) fn assert_current_and_adjacent_profile_negotiation(
    protocol: InternalTlsProtocol,
    current_client: std::sync::Arc<rustls::ClientConfig>,
    current_server: std::sync::Arc<rustls::ServerConfig>,
) {
    use rustls::pki_types::ServerName;
    use std::io;
    use std::net::{TcpListener, TcpStream};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    type NegotiatedAlpn = io::Result<Option<Vec<u8>>>;

    fn negotiate(
        client: Arc<rustls::ClientConfig>,
        server: Arc<rustls::ServerConfig>,
    ) -> (NegotiatedAlpn, NegotiatedAlpn) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(1)))?;
            socket.set_write_timeout(Some(Duration::from_secs(1)))?;
            let connection = rustls::ServerConnection::new(server)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
            let mut stream = rustls::StreamOwned::new(connection, socket);
            while stream.conn.is_handshaking() {
                stream.conn.complete_io(&mut stream.sock)?;
            }
            Ok(stream.conn.alpn_protocol().map(<[u8]>::to_vec))
        });
        let client_result = (|| {
            let socket = TcpStream::connect(address)?;
            socket.set_read_timeout(Some(Duration::from_secs(1)))?;
            socket.set_write_timeout(Some(Duration::from_secs(1)))?;
            let connection = rustls::ClientConnection::new(
                client,
                ServerName::try_from("localhost").unwrap().to_owned(),
            )
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
            let mut stream = rustls::StreamOwned::new(connection, socket);
            while stream.conn.is_handshaking() {
                stream.conn.complete_io(&mut stream.sock)?;
            }
            Ok(stream.conn.alpn_protocol().map(<[u8]>::to_vec))
        })();
        (client_result, server.join().unwrap())
    }

    fn accepted(protocol: InternalTlsProtocol, result: &io::Result<Option<Vec<u8>>>) -> bool {
        result
            .as_ref()
            .is_ok_and(|actual| protocol.is_negotiated(actual.as_deref()))
    }

    assert_eq!(current_client.alpn_protocols, [protocol.alpn()]);
    assert_eq!(current_server.alpn_protocols, [protocol.alpn()]);
    let (client_result, server_result) =
        negotiate(Arc::clone(&current_client), Arc::clone(&current_server));
    assert!(accepted(protocol, &client_result), "{client_result:?}");
    assert!(accepted(protocol, &server_result), "{server_result:?}");

    for adjacent_version in *b"02" {
        let mut adjacent = protocol.alpn().strip_suffix(b"1").unwrap().to_vec();
        adjacent.push(adjacent_version);
        let mut adjacent_server = (*current_server).clone();
        adjacent_server.alpn_protocols = vec![adjacent.clone()];
        let (client_result, _) = negotiate(Arc::clone(&current_client), Arc::new(adjacent_server));
        assert!(
            !accepted(protocol, &client_result),
            "current client accepted adjacent ALPN {:?}",
            String::from_utf8_lossy(&adjacent)
        );

        let mut adjacent_client = (*current_client).clone();
        adjacent_client.alpn_protocols = vec![adjacent.clone()];
        let (_, server_result) = negotiate(Arc::new(adjacent_client), Arc::clone(&current_server));
        assert!(
            !accepted(protocol, &server_result),
            "current server accepted adjacent ALPN {:?}",
            String::from_utf8_lossy(&adjacent)
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_internal_tls_protocol_identifier_catalogue_matches_frozen_versioned_manifest_and_requires_version_bump(
    ) {
        assert_eq!(
            InternalTlsProtocol::ALL.map(InternalTlsProtocol::alpn),
            [
                b"argmin-storage-rpc/1".as_slice(),
                b"argmin-control-plane/1".as_slice(),
                b"argmin-raft/1".as_slice(),
            ]
        );
    }
}
