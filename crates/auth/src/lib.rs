pub mod canonical;
pub mod credential;
pub mod error;
pub mod request;
pub mod sigv4;

pub use canonical::parse_amz_date;
pub use credential::{CredentialRecord, CredentialScope, CredentialStore, SecretKey};
pub use error::AuthError;
pub use request::{authenticate_request, AuthContext, AuthMode};
pub use sigv4::{parse_auth_header, verify_request, SigV4Auth};
