// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! The pinned Cobblestone-256 chunked-encryption profile.
//!
//! This implements the C2SP chunked-encryption specification at commit
//! `d757bb90e5966d92d17353a3964ca1af776b63e0`. Each message has a 24-byte
//! salt, a 32-byte key commitment, and canonical 16 KiB AES-256-GCM chunks.

use std::ops::Range;

use crate::aead::{Aes256GcmKey, AES_256_GCM_KEY_LEN, AES_GCM_NONCE_LEN, AES_GCM_TAG_LEN};
use crate::{hkdf, CryptoError};

pub const COBBLESTONE_256_INPUT_KEY_LEN: usize = AES_256_GCM_KEY_LEN;
pub const COBBLESTONE_SALT_LEN: usize = 24;
pub const COBBLESTONE_COMMITMENT_LEN: usize = 32;
pub const COBBLESTONE_HEADER_LEN: usize = COBBLESTONE_SALT_LEN + COBBLESTONE_COMMITMENT_LEN;
pub const COBBLESTONE_CHUNK_LEN: usize = 16 * 1024;
pub const COBBLESTONE_FULL_ENCRYPTED_CHUNK_LEN: usize = COBBLESTONE_CHUNK_LEN + AES_GCM_TAG_LEN;
pub const COBBLESTONE_MAX_CHUNKS: u64 = 1 << 38;

const DERIVED_MATERIAL_LEN: usize =
    AES_256_GCM_KEY_LEN + AES_GCM_NONCE_LEN + COBBLESTONE_COMMITMENT_LEN;
const HKDF_INFO_PREFIX: &[u8] = b"c2sp.org/chunked-encryption@v1+AEAD_AES_256_GCM\0";

/// A uniformly random Cobblestone-256 input key.
///
/// The key can encrypt multiple messages only when each message receives a
/// unique uniformly random salt.
pub struct Cobblestone256Key([u8; COBBLESTONE_256_INPUT_KEY_LEN]);

impl Cobblestone256Key {
    pub fn new(input_key: &[u8]) -> Result<Self, CryptoError> {
        Ok(Self(input_key.try_into().map_err(|_| CryptoError)?))
    }
}

struct DerivedMessageKey {
    key: Aes256GcmKey,
    base_nonce: [u8; AES_GCM_NONCE_LEN],
    commitment: [u8; COBBLESTONE_COMMITMENT_LEN],
}

fn derive_message_key(
    input_key: &Cobblestone256Key,
    salt: &[u8; COBBLESTONE_SALT_LEN],
    context: &[u8],
) -> Result<DerivedMessageKey, CryptoError> {
    let mut info = Vec::with_capacity(HKDF_INFO_PREFIX.len() + salt.len() + context.len());
    info.extend_from_slice(HKDF_INFO_PREFIX);
    info.extend_from_slice(salt);
    info.extend_from_slice(context);
    let material = hkdf::expand_sha512::<DERIVED_MATERIAL_LEN>(&input_key.0, &info)?;
    let key_material: [u8; AES_256_GCM_KEY_LEN] = material[..AES_256_GCM_KEY_LEN]
        .try_into()
        .map_err(|_| CryptoError)?;
    let nonce_start = AES_256_GCM_KEY_LEN;
    let commitment_start = nonce_start + AES_GCM_NONCE_LEN;
    Ok(DerivedMessageKey {
        key: Aes256GcmKey::new(&key_material)?,
        base_nonce: material[nonce_start..commitment_start]
            .try_into()
            .map_err(|_| CryptoError)?,
        commitment: material[commitment_start..]
            .try_into()
            .map_err(|_| CryptoError)?,
    })
}

fn nonce_for_chunk(
    base_nonce: [u8; AES_GCM_NONCE_LEN],
    chunk_index: u64,
) -> Result<[u8; AES_GCM_NONCE_LEN], CryptoError> {
    if chunk_index >= COBBLESTONE_MAX_CHUNKS {
        return Err(CryptoError);
    }
    let mut nonce = base_nonce;
    let counter = chunk_index.to_be_bytes();
    let nonce_counter_start = nonce.len() - 5;
    let counter_start = counter.len() - 5;
    for (nonce_byte, counter_byte) in nonce[nonce_counter_start..]
        .iter_mut()
        .zip(&counter[counter_start..])
    {
        *nonce_byte ^= counter_byte;
    }
    Ok(nonce)
}

#[cfg(feature = "openssl")]
fn commitments_equal(left: &[u8], right: &[u8]) -> bool {
    openssl::memcmp::eq(left, right)
}

#[cfg(all(feature = "ring", not(feature = "openssl")))]
#[allow(deprecated)]
fn commitments_equal(left: &[u8], right: &[u8]) -> bool {
    // Ring has not yet exposed a replacement constant-time byte comparison.
    // Keep this isolated so replacing the deprecated provider API does not
    // affect the persisted construction.
    ring::constant_time::verify_slices_are_equal(left, right).is_ok()
}

/// Canonical plaintext/ciphertext geometry for one complete message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Cobblestone256MessageLayout {
    ciphertext_len: u64,
    plaintext_len: u64,
    full_chunks: u64,
    terminal_ciphertext_len: usize,
}

impl Cobblestone256MessageLayout {
    pub fn from_ciphertext_len(ciphertext_len: u64) -> Result<Self, CryptoError> {
        let body_len = ciphertext_len
            .checked_sub(COBBLESTONE_HEADER_LEN as u64)
            .ok_or(CryptoError)?;
        let full_chunks = body_len / COBBLESTONE_FULL_ENCRYPTED_CHUNK_LEN as u64;
        let terminal_ciphertext_len =
            usize::try_from(body_len % COBBLESTONE_FULL_ENCRYPTED_CHUNK_LEN as u64)
                .map_err(|_| CryptoError)?;
        if !(AES_GCM_TAG_LEN..COBBLESTONE_FULL_ENCRYPTED_CHUNK_LEN)
            .contains(&terminal_ciphertext_len)
        {
            return Err(CryptoError);
        }
        let chunk_count = full_chunks.checked_add(1).ok_or(CryptoError)?;
        if chunk_count > COBBLESTONE_MAX_CHUNKS {
            return Err(CryptoError);
        }
        let plaintext_len = full_chunks
            .checked_mul(COBBLESTONE_CHUNK_LEN as u64)
            .and_then(|length| {
                length.checked_add((terminal_ciphertext_len - AES_GCM_TAG_LEN) as u64)
            })
            .ok_or(CryptoError)?;
        Ok(Self {
            ciphertext_len,
            plaintext_len,
            full_chunks,
            terminal_ciphertext_len,
        })
    }

    pub fn from_plaintext_len(plaintext_len: u64) -> Result<Self, CryptoError> {
        let full_chunks = plaintext_len / COBBLESTONE_CHUNK_LEN as u64;
        let final_plaintext_len = usize::try_from(plaintext_len % COBBLESTONE_CHUNK_LEN as u64)
            .map_err(|_| CryptoError)?;
        let chunk_count = full_chunks.checked_add(1).ok_or(CryptoError)?;
        if chunk_count > COBBLESTONE_MAX_CHUNKS {
            return Err(CryptoError);
        }
        let terminal_ciphertext_len = final_plaintext_len + AES_GCM_TAG_LEN;
        let ciphertext_len = (COBBLESTONE_HEADER_LEN as u64)
            .checked_add(
                full_chunks
                    .checked_mul(COBBLESTONE_FULL_ENCRYPTED_CHUNK_LEN as u64)
                    .ok_or(CryptoError)?,
            )
            .and_then(|length| length.checked_add(terminal_ciphertext_len as u64))
            .ok_or(CryptoError)?;
        Ok(Self {
            ciphertext_len,
            plaintext_len,
            full_chunks,
            terminal_ciphertext_len,
        })
    }

    #[must_use]
    pub fn ciphertext_len(self) -> u64 {
        self.ciphertext_len
    }

    #[must_use]
    pub fn plaintext_len(self) -> u64 {
        self.plaintext_len
    }

    #[must_use]
    pub fn chunk_count(self) -> u64 {
        self.full_chunks + 1
    }

    pub fn ciphertext_range_for_chunk(self, chunk_index: u64) -> Result<Range<u64>, CryptoError> {
        if chunk_index >= self.chunk_count() {
            return Err(CryptoError);
        }
        let start = (COBBLESTONE_HEADER_LEN as u64)
            .checked_add(
                chunk_index
                    .checked_mul(COBBLESTONE_FULL_ENCRYPTED_CHUNK_LEN as u64)
                    .ok_or(CryptoError)?,
            )
            .ok_or(CryptoError)?;
        let length = if chunk_index < self.full_chunks {
            COBBLESTONE_FULL_ENCRYPTED_CHUNK_LEN
        } else {
            self.terminal_ciphertext_len
        };
        Ok(start..start.checked_add(length as u64).ok_or(CryptoError)?)
    }

    pub fn plaintext_range_for_chunk(self, chunk_index: u64) -> Result<Range<u64>, CryptoError> {
        let ciphertext_range = self.ciphertext_range_for_chunk(chunk_index)?;
        let start = chunk_index
            .checked_mul(COBBLESTONE_CHUNK_LEN as u64)
            .ok_or(CryptoError)?;
        let plaintext_length = (ciphertext_range.end - ciphertext_range.start)
            .checked_sub(AES_GCM_TAG_LEN as u64)
            .ok_or(CryptoError)?;
        Ok(start..start.checked_add(plaintext_length).ok_or(CryptoError)?)
    }
}

/// Incremental Cobblestone-256 message encryption.
pub struct Cobblestone256Sealer {
    key: Aes256GcmKey,
    base_nonce: [u8; AES_GCM_NONCE_LEN],
    next_chunk: u64,
    pending: Vec<u8>,
    failed: bool,
}

impl Cobblestone256Sealer {
    pub fn start(
        input_key: &Cobblestone256Key,
        salt: [u8; COBBLESTONE_SALT_LEN],
        context: &[u8],
        ciphertext: &mut Vec<u8>,
    ) -> Result<Self, CryptoError> {
        let derived = derive_message_key(input_key, &salt, context)?;
        ciphertext.extend_from_slice(&salt);
        ciphertext.extend_from_slice(&derived.commitment);
        Ok(Self {
            key: derived.key,
            base_nonce: derived.base_nonce,
            next_chunk: 0,
            pending: Vec::with_capacity(COBBLESTONE_CHUNK_LEN),
            failed: false,
        })
    }

    pub fn update(
        &mut self,
        mut plaintext: &[u8],
        ciphertext: &mut Vec<u8>,
    ) -> Result<(), CryptoError> {
        if self.failed {
            return Err(CryptoError);
        }
        if !self.pending.is_empty() {
            let wanted = COBBLESTONE_CHUNK_LEN - self.pending.len();
            let consumed = wanted.min(plaintext.len());
            self.pending.extend_from_slice(&plaintext[..consumed]);
            plaintext = &plaintext[consumed..];
            if self.pending.len() == COBBLESTONE_CHUNK_LEN {
                let chunk =
                    std::mem::replace(&mut self.pending, Vec::with_capacity(COBBLESTONE_CHUNK_LEN));
                self.seal_chunk(chunk, ciphertext)?;
            }
        }

        while plaintext.len() >= COBBLESTONE_CHUNK_LEN {
            self.seal_chunk(plaintext[..COBBLESTONE_CHUNK_LEN].to_vec(), ciphertext)?;
            plaintext = &plaintext[COBBLESTONE_CHUNK_LEN..];
        }
        self.pending.extend_from_slice(plaintext);
        Ok(())
    }

    pub fn finish(mut self, ciphertext: &mut Vec<u8>) -> Result<(), CryptoError> {
        let final_chunk = std::mem::take(&mut self.pending);
        self.seal_chunk(final_chunk, ciphertext)
    }

    fn seal_chunk(
        &mut self,
        mut plaintext: Vec<u8>,
        ciphertext: &mut Vec<u8>,
    ) -> Result<(), CryptoError> {
        if self.failed {
            return Err(CryptoError);
        }
        let result = nonce_for_chunk(self.base_nonce, self.next_chunk).and_then(|nonce| {
            self.key
                .seal_in_place_append_tag(nonce, &[], &mut plaintext)
        });
        if result.is_err() {
            self.failed = true;
            return Err(CryptoError);
        }
        ciphertext.extend_from_slice(&plaintext);
        self.next_chunk = self.next_chunk.checked_add(1).ok_or(CryptoError)?;
        Ok(())
    }
}

/// A commitment-verified Cobblestone-256 message key.
pub struct Cobblestone256Opener {
    key: Aes256GcmKey,
    base_nonce: [u8; AES_GCM_NONCE_LEN],
}

impl Cobblestone256Opener {
    pub fn from_header(
        input_key: &Cobblestone256Key,
        context: &[u8],
        header: &[u8],
    ) -> Result<Self, CryptoError> {
        if header.len() != COBBLESTONE_HEADER_LEN {
            return Err(CryptoError);
        }
        let salt: [u8; COBBLESTONE_SALT_LEN] = header[..COBBLESTONE_SALT_LEN]
            .try_into()
            .map_err(|_| CryptoError)?;
        let derived = derive_message_key(input_key, &salt, context)?;
        if !commitments_equal(&derived.commitment, &header[COBBLESTONE_SALT_LEN..]) {
            return Err(CryptoError);
        }
        Ok(Self {
            key: derived.key,
            base_nonce: derived.base_nonce,
        })
    }

    pub fn open_chunk<'a>(
        &self,
        layout: Cobblestone256MessageLayout,
        chunk_index: u64,
        ciphertext_and_tag: &'a mut [u8],
    ) -> Result<&'a mut [u8], CryptoError> {
        let expected = layout.ciphertext_range_for_chunk(chunk_index)?;
        if ciphertext_and_tag.len() as u64 != expected.end - expected.start {
            return Err(CryptoError);
        }
        let nonce = nonce_for_chunk(self.base_nonce, chunk_index)?;
        self.key.open_in_place(nonce, &[], ciphertext_and_tag)
    }
}

/// Incremental decryption which never appends bytes from an unauthenticated
/// chunk to its output.
pub struct Cobblestone256StreamOpener {
    input_key: Option<Cobblestone256Key>,
    context: Vec<u8>,
    layout: Cobblestone256MessageLayout,
    opener: Option<Cobblestone256Opener>,
    pending: Vec<u8>,
    consumed: u64,
    next_chunk: u64,
    failed: bool,
    finished: bool,
}

impl Cobblestone256StreamOpener {
    pub fn new(
        input_key: Cobblestone256Key,
        context: &[u8],
        ciphertext_len: u64,
    ) -> Result<Self, CryptoError> {
        Ok(Self {
            input_key: Some(input_key),
            context: context.to_vec(),
            layout: Cobblestone256MessageLayout::from_ciphertext_len(ciphertext_len)?,
            opener: None,
            pending: Vec::with_capacity(COBBLESTONE_FULL_ENCRYPTED_CHUNK_LEN),
            consumed: 0,
            next_chunk: 0,
            failed: false,
            finished: false,
        })
    }

    pub fn update(
        &mut self,
        mut ciphertext: &[u8],
        plaintext: &mut Vec<u8>,
    ) -> Result<(), CryptoError> {
        if self.failed
            || self.finished
            || self
                .consumed
                .checked_add(ciphertext.len() as u64)
                .is_none_or(|length| length > self.layout.ciphertext_len())
        {
            return self.fail();
        }

        if self.opener.is_none() {
            let wanted = COBBLESTONE_HEADER_LEN - self.pending.len();
            let consumed = wanted.min(ciphertext.len());
            self.pending.extend_from_slice(&ciphertext[..consumed]);
            self.consumed += consumed as u64;
            ciphertext = &ciphertext[consumed..];
            if self.pending.len() == COBBLESTONE_HEADER_LEN {
                let input_key = self.input_key.take().ok_or(CryptoError)?;
                let opener =
                    Cobblestone256Opener::from_header(&input_key, &self.context, &self.pending);
                self.pending.clear();
                self.opener = Some(opener.map_err(|_| {
                    self.failed = true;
                    CryptoError
                })?);
            }
        }

        while !ciphertext.is_empty() {
            let range = self
                .layout
                .ciphertext_range_for_chunk(self.next_chunk)
                .map_err(|_| {
                    self.failed = true;
                    CryptoError
                })?;
            let chunk_len = usize::try_from(range.end - range.start).map_err(|_| CryptoError)?;
            let wanted = chunk_len - self.pending.len();
            let consumed = wanted.min(ciphertext.len());
            self.pending.extend_from_slice(&ciphertext[..consumed]);
            self.consumed += consumed as u64;
            ciphertext = &ciphertext[consumed..];
            if self.pending.len() == chunk_len {
                let opened = self
                    .opener
                    .as_ref()
                    .ok_or(CryptoError)?
                    .open_chunk(self.layout, self.next_chunk, &mut self.pending)
                    .map_err(|_| {
                        self.failed = true;
                        CryptoError
                    })?;
                plaintext.extend_from_slice(opened);
                self.pending.clear();
                self.next_chunk += 1;
            }
        }
        Ok(())
    }

    pub fn finish(&mut self) -> Result<(), CryptoError> {
        if self.failed
            || self.finished
            || self.consumed != self.layout.ciphertext_len()
            || self.next_chunk != self.layout.chunk_count()
            || !self.pending.is_empty()
        {
            return self.fail();
        }
        self.finished = true;
        Ok(())
    }

    fn fail<T>(&mut self) -> Result<T, CryptoError> {
        self.failed = true;
        Err(CryptoError)
    }
}

pub fn seal(
    input_key: &Cobblestone256Key,
    salt: [u8; COBBLESTONE_SALT_LEN],
    context: &[u8],
    plaintext: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    let layout = Cobblestone256MessageLayout::from_plaintext_len(plaintext.len() as u64)?;
    let capacity = usize::try_from(layout.ciphertext_len()).map_err(|_| CryptoError)?;
    let mut ciphertext = Vec::with_capacity(capacity);
    let mut sealer = Cobblestone256Sealer::start(input_key, salt, context, &mut ciphertext)?;
    sealer.update(plaintext, &mut ciphertext)?;
    sealer.finish(&mut ciphertext)?;
    debug_assert_eq!(ciphertext.len(), capacity);
    Ok(ciphertext)
}

pub fn open(
    input_key: Cobblestone256Key,
    context: &[u8],
    ciphertext: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    let layout = Cobblestone256MessageLayout::from_ciphertext_len(ciphertext.len() as u64)?;
    let capacity = usize::try_from(layout.plaintext_len()).map_err(|_| CryptoError)?;
    let mut plaintext = Vec::with_capacity(capacity);
    let mut opener = Cobblestone256StreamOpener::new(input_key, context, ciphertext.len() as u64)?;
    opener.update(ciphertext, &mut plaintext)?;
    opener.finish()?;
    debug_assert_eq!(plaintext.len(), capacity);
    Ok(plaintext)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> Cobblestone256Key {
        Cobblestone256Key::new(&[0x42; COBBLESTONE_256_INPUT_KEY_LEN]).unwrap()
    }

    #[test]
    fn canonical_lengths_include_short_terminal_chunk() {
        for plaintext_len in [
            0,
            1,
            COBBLESTONE_CHUNK_LEN - 1,
            COBBLESTONE_CHUNK_LEN,
            COBBLESTONE_CHUNK_LEN + 1,
            2 * COBBLESTONE_CHUNK_LEN,
        ] {
            let layout =
                Cobblestone256MessageLayout::from_plaintext_len(plaintext_len as u64).unwrap();
            assert_eq!(layout.plaintext_len(), plaintext_len as u64);
            assert_eq!(
                Cobblestone256MessageLayout::from_ciphertext_len(layout.ciphertext_len()).unwrap(),
                layout
            );
            let final_range = layout
                .ciphertext_range_for_chunk(layout.chunk_count() - 1)
                .unwrap();
            assert!(
                final_range.end - final_range.start < COBBLESTONE_FULL_ENCRYPTED_CHUNK_LEN as u64
            );
        }
    }

    #[test]
    fn invalid_ciphertext_lengths_are_rejected() {
        for length in [
            0,
            COBBLESTONE_HEADER_LEN as u64,
            (COBBLESTONE_HEADER_LEN + AES_GCM_TAG_LEN - 1) as u64,
            (COBBLESTONE_HEADER_LEN + COBBLESTONE_FULL_ENCRYPTED_CHUNK_LEN) as u64,
            (COBBLESTONE_HEADER_LEN + COBBLESTONE_FULL_ENCRYPTED_CHUNK_LEN + 1) as u64,
        ] {
            assert!(Cobblestone256MessageLayout::from_ciphertext_len(length).is_err());
        }
    }

    #[test]
    fn fragmented_streaming_round_trip_releases_only_complete_chunks() {
        let plaintext = vec![0xa5; COBBLESTONE_CHUNK_LEN * 2 + 17];
        let mut ciphertext = Vec::new();
        let mut sealer = Cobblestone256Sealer::start(
            &key(),
            [0x24; COBBLESTONE_SALT_LEN],
            b"context",
            &mut ciphertext,
        )
        .unwrap();
        for part in plaintext.chunks(997) {
            sealer.update(part, &mut ciphertext).unwrap();
        }
        sealer.finish(&mut ciphertext).unwrap();

        let mut opener =
            Cobblestone256StreamOpener::new(key(), b"context", ciphertext.len() as u64).unwrap();
        let mut opened = Vec::new();
        for part in ciphertext.chunks(613) {
            let before = opened.len();
            opener.update(part, &mut opened).unwrap();
            assert!(
                opened.len() == before
                    || opened.len() % COBBLESTONE_CHUNK_LEN == 0
                    || opened.len() == plaintext.len()
            );
        }
        opener.finish().unwrap();
        assert_eq!(opened, plaintext);
    }

    #[test]
    fn commitment_and_chunk_failures_release_no_failing_chunk() {
        let plaintext = vec![0x3c; COBBLESTONE_CHUNK_LEN + 7];
        let ciphertext =
            seal(&key(), [0x18; COBBLESTONE_SALT_LEN], b"subject", &plaintext).unwrap();

        let mut bad_commitment = ciphertext.clone();
        bad_commitment[COBBLESTONE_SALT_LEN] ^= 1;
        assert!(open(key(), b"subject", &bad_commitment).is_err());

        let mut bad_final_chunk = ciphertext;
        let last = bad_final_chunk.len() - 1;
        bad_final_chunk[last] ^= 1;
        let mut opened = Vec::new();
        let mut opener =
            Cobblestone256StreamOpener::new(key(), b"subject", bad_final_chunk.len() as u64)
                .unwrap();
        assert!(opener.update(&bad_final_chunk, &mut opened).is_err());
        assert_eq!(opened, &plaintext[..COBBLESTONE_CHUNK_LEN]);
        assert!(opener.update(&[], &mut opened).is_err());
    }

    #[test]
    fn selected_chunks_require_verified_header_and_exact_layout() {
        let plaintext: Vec<u8> = (0..COBBLESTONE_CHUNK_LEN * 2 + 3)
            .map(|index| index as u8)
            .collect();
        let ciphertext = seal(
            &key(),
            [0x81; COBBLESTONE_SALT_LEN],
            b"selected",
            &plaintext,
        )
        .unwrap();
        let layout =
            Cobblestone256MessageLayout::from_ciphertext_len(ciphertext.len() as u64).unwrap();
        let opener = Cobblestone256Opener::from_header(
            &key(),
            b"selected",
            &ciphertext[..COBBLESTONE_HEADER_LEN],
        )
        .unwrap();
        let mut selected_plaintext = Vec::new();
        for index in 0..layout.chunk_count() {
            let range = layout.ciphertext_range_for_chunk(index).unwrap();
            let mut chunk = ciphertext[range.start as usize..range.end as usize].to_vec();
            selected_plaintext
                .extend_from_slice(opener.open_chunk(layout, index, &mut chunk).unwrap());
        }
        assert_eq!(selected_plaintext, plaintext);

        let range = layout.ciphertext_range_for_chunk(0).unwrap();
        let mut short = ciphertext[range.start as usize..range.end as usize - 1].to_vec();
        assert!(opener.open_chunk(layout, 0, &mut short).is_err());

        let mut bad_tag = ciphertext[range.start as usize..range.end as usize].to_vec();
        *bad_tag.last_mut().unwrap() ^= 1;
        assert!(opener.open_chunk(layout, 0, &mut bad_tag).is_err());
        assert!(bad_tag.iter().all(|byte| *byte == 0));
    }

    #[test]
    fn streaming_finish_is_terminal_and_failures_are_fused() {
        let ciphertext = seal(&key(), [0x91; COBBLESTONE_SALT_LEN], b"finish", b"payload").unwrap();

        let mut complete =
            Cobblestone256StreamOpener::new(key(), b"finish", ciphertext.len() as u64).unwrap();
        let mut plaintext = Vec::new();
        complete.update(&ciphertext, &mut plaintext).unwrap();
        complete.finish().unwrap();
        assert!(complete.update(&[], &mut plaintext).is_err());
        assert!(complete.finish().is_err());

        let mut truncated =
            Cobblestone256StreamOpener::new(key(), b"finish", ciphertext.len() as u64).unwrap();
        let mut partial_plaintext = Vec::new();
        truncated
            .update(&ciphertext[..ciphertext.len() - 1], &mut partial_plaintext)
            .unwrap();
        assert!(truncated.finish().is_err());
        assert!(truncated.update(&[], &mut partial_plaintext).is_err());
        assert!(truncated.finish().is_err());
        assert!(partial_plaintext.is_empty());
    }

    #[test]
    fn streaming_sealer_is_fused_after_failure() {
        let mut ciphertext = Vec::new();
        let mut sealer = Cobblestone256Sealer::start(
            &key(),
            [0x92; COBBLESTONE_SALT_LEN],
            b"seal-failure",
            &mut ciphertext,
        )
        .unwrap();
        sealer.next_chunk = COBBLESTONE_MAX_CHUNKS;
        assert!(sealer
            .update(&[0x11; COBBLESTONE_CHUNK_LEN], &mut ciphertext)
            .is_err());
        assert_eq!(ciphertext.len(), COBBLESTONE_HEADER_LEN);
        assert!(sealer.update(&[], &mut ciphertext).is_err());
        assert!(sealer.finish(&mut ciphertext).is_err());
        assert_eq!(ciphertext.len(), COBBLESTONE_HEADER_LEN);
    }
}
