#[cfg(any(
    feature = "bench-select",
    all(target_arch = "riscv64", target_os = "linux")
))]
use std::sync::OnceLock;

#[cfg(target_arch = "riscv64")]
const BLOCK_LEN: usize = 64;
const DIGEST_LEN: usize = 32;
#[cfg(target_arch = "riscv64")]
const INITIAL_STATE: [u32; 8] = [
    0x6a09_e667,
    0xbb67_ae85,
    0x3c6e_f372,
    0xa54f_f53a,
    0x510e_527f,
    0x9b05_688c,
    0x1f83_d9ab,
    0x5be0_cd19,
];

/// Incremental SHA-256 hasher.
///
/// Ring remains the implementation on existing targets. On Linux RISC-V, a
/// `Zvknha`/`Zvknhb` and `Zvkb` implementation is selected automatically when
/// the hardware and calling thread permit vector instructions.
pub struct Sha256 {
    inner: Inner,
}

enum Inner {
    Ring(ring::digest::Context),
    #[cfg(target_arch = "riscv64")]
    Riscv64(Riscv64Sha256),
}

impl Sha256 {
    #[must_use]
    pub fn new() -> Self {
        #[cfg(target_arch = "riscv64")]
        if selected_backend() == Backend::Riscv64Zvknh {
            return Self {
                inner: Inner::Riscv64(Riscv64Sha256::new()),
            };
        }

        Self {
            inner: Inner::Ring(ring::digest::Context::new(&ring::digest::SHA256)),
        }
    }

    pub fn update(&mut self, data: &[u8]) {
        match &mut self.inner {
            Inner::Ring(context) => context.update(data),
            #[cfg(target_arch = "riscv64")]
            Inner::Riscv64(hasher) => hasher.update(data),
        }
    }

    #[must_use]
    pub fn finalize(self) -> [u8; DIGEST_LEN] {
        match self.inner {
            Inner::Ring(context) => context
                .finish()
                .as_ref()
                .try_into()
                .expect("SHA-256 produces a 32-byte digest"),
            #[cfg(target_arch = "riscv64")]
            Inner::Riscv64(hasher) => hasher.finalize(),
        }
    }
}

impl Default for Sha256 {
    fn default() -> Self {
        Self::new()
    }
}

#[must_use]
pub fn digest(data: &[u8]) -> [u8; DIGEST_LEN] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize()
}

/// Returns the SHA-256 implementation selected for a newly created hasher on
/// the calling thread.
#[must_use]
pub fn backend_name() -> &'static str {
    match selected_backend() {
        Backend::Ring => "ring",
        #[cfg(target_arch = "riscv64")]
        Backend::Riscv64Zvknh => "riscv64-zvknh",
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Backend {
    Ring,
    #[cfg(target_arch = "riscv64")]
    Riscv64Zvknh,
}

fn selected_backend() -> Backend {
    #[cfg(feature = "bench-select")]
    if let Some(backend) = bench_override_backend() {
        return backend;
    }

    #[cfg(target_arch = "riscv64")]
    if riscv64_vector_sha256_available() {
        return Backend::Riscv64Zvknh;
    }

    Backend::Ring
}

#[cfg(feature = "bench-select")]
fn bench_override_backend() -> Option<Backend> {
    static OVERRIDE: OnceLock<Option<String>> = OnceLock::new();
    let name = OVERRIDE.get_or_init(|| {
        std::env::var("ARGMIN_SHA256_BENCH_BACKEND")
            .ok()
            .map(|value| value.trim().to_ascii_lowercase())
    });
    match name.as_deref() {
        Some("ring") => Some(Backend::Ring),
        #[cfg(target_arch = "riscv64")]
        Some("zvknh") if riscv64_vector_sha256_available() => Some(Backend::Riscv64Zvknh),
        _ => None,
    }
}

#[cfg(target_arch = "riscv64")]
#[repr(align(4))]
struct AlignedBlock([u8; BLOCK_LEN]);

#[cfg(target_arch = "riscv64")]
struct Riscv64Sha256 {
    state: [u32; 8],
    block: AlignedBlock,
    block_len: usize,
    message_len: u64,
}

#[cfg(target_arch = "riscv64")]
impl Riscv64Sha256 {
    const fn new() -> Self {
        Self {
            state: INITIAL_STATE,
            block: AlignedBlock([0; BLOCK_LEN]),
            block_len: 0,
            message_len: 0,
        }
    }

    fn update(&mut self, mut data: &[u8]) {
        self.message_len = self.message_len.wrapping_add(data.len() as u64);

        if self.block_len != 0 {
            let copied = (BLOCK_LEN - self.block_len).min(data.len());
            self.block.0[self.block_len..self.block_len + copied].copy_from_slice(&data[..copied]);
            self.block_len += copied;
            data = &data[copied..];
            if self.block_len != BLOCK_LEN {
                return;
            }
            compress_blocks(&mut self.state, &self.block.0);
            self.block_len = 0;
        }

        let direct_len = data.len() / BLOCK_LEN * BLOCK_LEN;
        if direct_len != 0 {
            let (blocks, tail) = data.split_at(direct_len);
            if blocks.as_ptr().align_offset(align_of::<u32>()) == 0 {
                compress_blocks(&mut self.state, blocks);
            } else {
                for block in blocks.chunks_exact(BLOCK_LEN) {
                    self.block.0.copy_from_slice(block);
                    compress_blocks(&mut self.state, &self.block.0);
                }
            }
            data = tail;
        }

        self.block.0[..data.len()].copy_from_slice(data);
        self.block_len = data.len();
    }

    fn finalize(mut self) -> [u8; DIGEST_LEN] {
        let bit_len = self.message_len.wrapping_mul(8);
        self.block.0[self.block_len] = 0x80;
        self.block_len += 1;

        if self.block_len > BLOCK_LEN - size_of::<u64>() {
            self.block.0[self.block_len..].fill(0);
            compress_blocks(&mut self.state, &self.block.0);
            self.block.0.fill(0);
        } else {
            self.block.0[self.block_len..BLOCK_LEN - size_of::<u64>()].fill(0);
        }
        self.block.0[BLOCK_LEN - size_of::<u64>()..].copy_from_slice(&bit_len.to_be_bytes());
        compress_blocks(&mut self.state, &self.block.0);

        let mut result = [0; DIGEST_LEN];
        for (output, word) in result.chunks_exact_mut(4).zip(self.state) {
            output.copy_from_slice(&word.to_be_bytes());
        }
        result
    }
}

#[cfg(target_arch = "riscv64")]
#[inline]
fn compress_blocks(state: &mut [u32; 8], blocks: &[u8]) {
    debug_assert!(!blocks.is_empty());
    debug_assert_eq!(blocks.len() % BLOCK_LEN, 0);
    debug_assert_eq!(blocks.as_ptr().align_offset(align_of::<u32>()), 0);

    if riscv64_vector_sha256_available() {
        // SAFETY: feature detection verifies Zvkb, Zvknha/Zvknhb, VLEN >= 128,
        // and vector permission for the calling thread. The state is eight
        // aligned u32 words, and blocks is a nonempty sequence of aligned
        // 64-byte blocks.
        unsafe {
            riscv64::compress_zvknh(
                state.as_mut_ptr(),
                blocks.as_ptr(),
                blocks.len() / BLOCK_LEN,
            );
        }
    } else {
        compress_scalar(state, blocks);
    }
}

#[cfg(target_arch = "riscv64")]
fn compress_scalar(state: &mut [u32; 8], blocks: &[u8]) {
    const K: [u32; 64] = [
        0x428a_2f98,
        0x7137_4491,
        0xb5c0_fbcf,
        0xe9b5_dba5,
        0x3956_c25b,
        0x59f1_11f1,
        0x923f_82a4,
        0xab1c_5ed5,
        0xd807_aa98,
        0x1283_5b01,
        0x2431_85be,
        0x550c_7dc3,
        0x72be_5d74,
        0x80de_b1fe,
        0x9bdc_06a7,
        0xc19b_f174,
        0xe49b_69c1,
        0xefbe_4786,
        0x0fc1_9dc6,
        0x240c_a1cc,
        0x2de9_2c6f,
        0x4a74_84aa,
        0x5cb0_a9dc,
        0x76f9_88da,
        0x983e_5152,
        0xa831_c66d,
        0xb003_27c8,
        0xbf59_7fc7,
        0xc6e0_0bf3,
        0xd5a7_9147,
        0x06ca_6351,
        0x1429_2967,
        0x27b7_0a85,
        0x2e1b_2138,
        0x4d2c_6dfc,
        0x5338_0d13,
        0x650a_7354,
        0x766a_0abb,
        0x81c2_c92e,
        0x9272_2c85,
        0xa2bf_e8a1,
        0xa81a_664b,
        0xc24b_8b70,
        0xc76c_51a3,
        0xd192_e819,
        0xd699_0624,
        0xf40e_3585,
        0x106a_a070,
        0x19a4_c116,
        0x1e37_6c08,
        0x2748_774c,
        0x34b0_bcb5,
        0x391c_0cb3,
        0x4ed8_aa4a,
        0x5b9c_ca4f,
        0x682e_6ff3,
        0x748f_82ee,
        0x78a5_636f,
        0x84c8_7814,
        0x8cc7_0208,
        0x90be_fffa,
        0xa450_6ceb,
        0xbef9_a3f7,
        0xc671_78f2,
    ];

    for block in blocks.chunks_exact(BLOCK_LEN) {
        let mut schedule = [0_u32; 64];
        for (word, bytes) in schedule[..16].iter_mut().zip(block.chunks_exact(4)) {
            *word = u32::from_be_bytes(bytes.try_into().expect("chunk has four bytes"));
        }
        for index in 16..64 {
            let s0 = schedule[index - 15].rotate_right(7)
                ^ schedule[index - 15].rotate_right(18)
                ^ (schedule[index - 15] >> 3);
            let s1 = schedule[index - 2].rotate_right(17)
                ^ schedule[index - 2].rotate_right(19)
                ^ (schedule[index - 2] >> 10);
            schedule[index] = schedule[index - 16]
                .wrapping_add(s0)
                .wrapping_add(schedule[index - 7])
                .wrapping_add(s1);
        }

        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = *state;
        for (&word, &constant) in schedule.iter().zip(&K) {
            let sum1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let choice = (e & f) ^ (!e & g);
            let temporary1 = h
                .wrapping_add(sum1)
                .wrapping_add(choice)
                .wrapping_add(constant)
                .wrapping_add(word);
            let sum0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let majority = (a & b) ^ (a & c) ^ (b & c);
            let temporary2 = sum0.wrapping_add(majority);

            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(temporary1);
            d = c;
            c = b;
            b = a;
            a = temporary1.wrapping_add(temporary2);
        }

        state[0] = state[0].wrapping_add(a);
        state[1] = state[1].wrapping_add(b);
        state[2] = state[2].wrapping_add(c);
        state[3] = state[3].wrapping_add(d);
        state[4] = state[4].wrapping_add(e);
        state[5] = state[5].wrapping_add(f);
        state[6] = state[6].wrapping_add(g);
        state[7] = state[7].wrapping_add(h);
    }
}

#[cfg(all(target_arch = "riscv64", target_os = "linux"))]
fn riscv64_vector_sha256_available() -> bool {
    static HARDWARE_AVAILABLE: OnceLock<bool> = OnceLock::new();
    static VLEN_SUFFICIENT: OnceLock<bool> = OnceLock::new();

    if !*HARDWARE_AVAILABLE.get_or_init(riscv64_vector_sha256_hardware_available)
        || !riscv64_vector_enabled_for_thread()
    {
        return false;
    }

    *VLEN_SUFFICIENT.get_or_init(|| {
        // SAFETY: hardware probing and the per-thread control check establish
        // that vector state is available, so reading vlenb is permitted.
        unsafe { riscv64::vector_len_bytes() >= 16 }
    })
}

#[cfg(all(target_arch = "riscv64", not(target_os = "linux")))]
const fn riscv64_vector_sha256_available() -> bool {
    false
}

#[cfg(all(target_arch = "riscv64", target_os = "linux"))]
fn riscv64_vector_sha256_hardware_available() -> bool {
    use core::arch::asm;

    #[repr(C)]
    struct HwprobePair {
        key: i64,
        value: u64,
    }

    const RISCV_HWPROBE_SYSCALL: usize = 258;
    const RISCV_HWPROBE_KEY_IMA_EXT_0: i64 = 4;
    const RISCV_HWPROBE_EXT_ZVKB: u64 = 1 << 19;
    const RISCV_HWPROBE_EXT_ZVKNHA: u64 = 1 << 22;
    const RISCV_HWPROBE_EXT_ZVKNHB: u64 = 1 << 23;

    let mut pair = HwprobePair {
        key: RISCV_HWPROBE_KEY_IMA_EXT_0,
        value: 0,
    };
    let mut result = (&raw mut pair).addr() as isize;
    // SAFETY: this invokes Linux's riscv_hwprobe syscall with one writable
    // pair and no CPU mask. The kernel validates the arguments before writing.
    unsafe {
        asm!(
            "ecall",
            inlateout("a0") result,
            in("a1") 1_usize,
            in("a2") 0_usize,
            in("a3") 0_usize,
            in("a4") 0_usize,
            in("a7") RISCV_HWPROBE_SYSCALL,
            options(nostack),
        );
    }

    result == 0
        && pair.key == RISCV_HWPROBE_KEY_IMA_EXT_0
        && pair.value & RISCV_HWPROBE_EXT_ZVKB != 0
        && pair.value & (RISCV_HWPROBE_EXT_ZVKNHA | RISCV_HWPROBE_EXT_ZVKNHB) != 0
}

#[cfg(all(target_arch = "riscv64", target_os = "linux"))]
std::thread_local! {
    static RISCV64_VECTOR_ENABLED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(all(target_arch = "riscv64", target_os = "linux"))]
fn riscv64_vector_enabled_for_thread() -> bool {
    unsafe extern "C" {
        fn prctl(option: i32, ...) -> i32;
    }

    const EINVAL: i32 = 22;
    const PR_RISCV_V_GET_CONTROL: i32 = 70;
    const PR_RISCV_V_VSTATE_CTRL_CUR_MASK: i32 = 3;
    const PR_RISCV_V_VSTATE_CTRL_ON: i32 = 2;

    RISCV64_VECTOR_ENABLED.with(|enabled| {
        if enabled.get() {
            return true;
        }

        // Vector enablement is thread-local. A positive result may be cached
        // because Linux does not allow a thread to disable vector state once
        // enabled. Negative results are not cached because it may be enabled
        // later.
        // SAFETY: PR_RISCV_V_GET_CONTROL takes no pointer arguments; the zero
        // variadic arguments are ignored.
        let vector_control =
            unsafe { prctl(PR_RISCV_V_GET_CONTROL, 0_usize, 0_usize, 0_usize, 0_usize) };
        let is_enabled = if vector_control >= 0 {
            vector_control & PR_RISCV_V_VSTATE_CTRL_CUR_MASK == PR_RISCV_V_VSTATE_CTRL_ON
        } else {
            // Kernels predating this interface report EINVAL. Other failures,
            // including seccomp EPERM, conservatively disable the backend.
            std::io::Error::last_os_error().raw_os_error() == Some(EINVAL)
        };
        if is_enabled {
            enabled.set(true);
        }
        is_enabled
    })
}

#[cfg(target_arch = "riscv64")]
mod riscv64 {
    use core::arch::global_asm;

    // The following compression sequence is derived from Linux/OpenSSL's
    // sha256-riscv64-zvknha_or_zvknhb-zvkb.S. The upstream code is
    // dual-licensed under Apache-2.0 OR BSD-2-Clause, at our option.
    // SPDX-License-Identifier: Apache-2.0 OR BSD-2-Clause
    //
    // Copyright 2023 The OpenSSL Project Authors.
    // Copyright (c) 2023, Christoph Müllner <christoph.muellner@vrull.eu>
    // Copyright (c) 2023, Phoebe Chen <phoebe.chen@sifive.com>
    // Copyright 2024 Google LLC.
    // All rights reserved.
    //
    // Redistribution and use in source and binary forms, with or without
    // modification, are permitted provided that the following conditions are
    // met:
    //
    // 1. Redistributions of source code must retain the above copyright
    //    notice, this list of conditions and the following disclaimer.
    // 2. Redistributions in binary form must reproduce the above copyright
    //    notice, this list of conditions and the following disclaimer in the
    //    documentation and/or other materials provided with the distribution.
    //
    // THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
    // IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
    // TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
    // PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
    // HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
    // SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
    // LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
    // DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
    // THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
    // (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
    // THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
    //
    // Alternatively, this code may be used under the Apache License 2.0. A
    // copy is available at https://www.apache.org/licenses/LICENSE-2.0.
    global_asm!(
        r#"
        .pushsection .text.argmin_crypto_sha256_zvknh, "ax", @progbits
        .balign 4
        .globl argmin_crypto_sha256_zvknh
        .hidden argmin_crypto_sha256_zvknh
        .type argmin_crypto_sha256_zvknh, @function
        .option push
        .option arch, +zvknha, +zvkb

        .macro argmin_sha256_4rounds last, k, w0, w1, w2, w3
        vadd.vv v6, \k, \w0
        vsha2cl.vv v8, v7, v6
        vsha2ch.vv v7, v8, v6
        .if !\last
        vmerge.vvm v6, \w2, \w1, v0
        vsha2ms.vv \w0, v6, \w3
        .endif
        .endm

        .macro argmin_sha256_16rounds last, k0, k1, k2, k3
        argmin_sha256_4rounds \last, \k0, v2, v3, v4, v5
        argmin_sha256_4rounds \last, \k1, v3, v4, v5, v2
        argmin_sha256_4rounds \last, \k2, v4, v5, v2, v3
        argmin_sha256_4rounds \last, \k3, v5, v2, v3, v4
        .endm

argmin_crypto_sha256_zvknh:
        vsetivli zero, 4, e32, m1, ta, ma
        la t0, .Largmin_sha256_k256
        vle32.v v10, (t0)
        addi t0, t0, 16
        vle32.v v11, (t0)
        addi t0, t0, 16
        vle32.v v12, (t0)
        addi t0, t0, 16
        vle32.v v13, (t0)
        addi t0, t0, 16
        vle32.v v14, (t0)
        addi t0, t0, 16
        vle32.v v15, (t0)
        addi t0, t0, 16
        vle32.v v16, (t0)
        addi t0, t0, 16
        vle32.v v17, (t0)
        addi t0, t0, 16
        vle32.v v18, (t0)
        addi t0, t0, 16
        vle32.v v19, (t0)
        addi t0, t0, 16
        vle32.v v20, (t0)
        addi t0, t0, 16
        vle32.v v21, (t0)
        addi t0, t0, 16
        vle32.v v22, (t0)
        addi t0, t0, 16
        vle32.v v23, (t0)
        addi t0, t0, 16
        vle32.v v24, (t0)
        addi t0, t0, 16
        vle32.v v25, (t0)

        vsetivli zero, 1, e8, m1, ta, ma
        vmv.v.i v0, 1

        li t0, 0x00041014
        vsetivli zero, 1, e32, m1, ta, ma
        vmv.v.x v1, t0
        addi a3, a0, 8
        vsetivli zero, 4, e32, m1, ta, ma
        vluxei8.v v7, (a0), v1
        vluxei8.v v8, (a3), v1

.Largmin_sha256_next_block:
        addi a2, a2, -1
        vmv.v.v v26, v7
        vmv.v.v v27, v8

        vle32.v v2, (a1)
        vrev8.v v2, v2
        addi a1, a1, 16
        vle32.v v3, (a1)
        vrev8.v v3, v3
        addi a1, a1, 16
        vle32.v v4, (a1)
        vrev8.v v4, v4
        addi a1, a1, 16
        vle32.v v5, (a1)
        vrev8.v v5, v5
        addi a1, a1, 16

        argmin_sha256_16rounds 0, v10, v11, v12, v13
        argmin_sha256_16rounds 0, v14, v15, v16, v17
        argmin_sha256_16rounds 0, v18, v19, v20, v21
        argmin_sha256_16rounds 1, v22, v23, v24, v25

        vadd.vv v7, v7, v26
        vadd.vv v8, v8, v27
        bnez a2, .Largmin_sha256_next_block

        vsuxei8.v v7, (a0), v1
        vsuxei8.v v8, (a3), v1
        ret
        .purgem argmin_sha256_4rounds
        .purgem argmin_sha256_16rounds
        .option pop
        .size argmin_crypto_sha256_zvknh, .-argmin_crypto_sha256_zvknh
        .popsection

        .pushsection .text.argmin_crypto_riscv64_vector_len_bytes, "ax", @progbits
        .balign 4
        .globl argmin_crypto_riscv64_vector_len_bytes
        .hidden argmin_crypto_riscv64_vector_len_bytes
        .type argmin_crypto_riscv64_vector_len_bytes, @function
        .option push
        .option arch, +v
argmin_crypto_riscv64_vector_len_bytes:
        csrr a0, vlenb
        ret
        .option pop
        .size argmin_crypto_riscv64_vector_len_bytes, .-argmin_crypto_riscv64_vector_len_bytes
        .popsection

        .pushsection .rodata.argmin_crypto_sha256_k256, "a", @progbits
        .balign 4
.Largmin_sha256_k256:
        .word 0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5
        .word 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5
        .word 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3
        .word 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174
        .word 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc
        .word 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da
        .word 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7
        .word 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967
        .word 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13
        .word 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85
        .word 0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3
        .word 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070
        .word 0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5
        .word 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3
        .word 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208
        .word 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2
        .popsection
        "#,
    );

    unsafe extern "C" {
        fn argmin_crypto_sha256_zvknh(state: *mut u32, data: *const u8, blocks: usize);
        fn argmin_crypto_riscv64_vector_len_bytes() -> usize;
    }

    /// Compresses one or more SHA-256 blocks using Zvknh.
    ///
    /// # Safety
    ///
    /// The calling thread must have vector state enabled and the CPU must
    /// support V with VLEN >= 128, Zvkb, and Zvknha or Zvknhb. `state` must
    /// address eight writable u32 values, `data` must be four-byte aligned and
    /// address `blocks * 64` readable bytes, and `blocks` must be nonzero.
    #[inline]
    pub(super) unsafe fn compress_zvknh(state: *mut u32, data: *const u8, blocks: usize) {
        assert!(!state.is_null());
        assert!(!data.is_null());
        assert_ne!(blocks, 0);
        assert_eq!(data.align_offset(align_of::<u32>()), 0);
        // SAFETY: the caller establishes the documented feature, permission,
        // pointer, and length requirements.
        unsafe { argmin_crypto_sha256_zvknh(state, data, blocks) };
    }

    /// Returns the architectural vector register length in bytes.
    ///
    /// # Safety
    ///
    /// The calling thread must have vector state enabled.
    #[inline]
    pub(super) unsafe fn vector_len_bytes() -> usize {
        // SAFETY: the caller establishes vector availability.
        unsafe { argmin_crypto_riscv64_vector_len_bytes() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(target_arch = "riscv64")]
    use proptest::prelude::*;

    fn ring_digest(data: &[u8]) -> [u8; DIGEST_LEN] {
        ring::digest::digest(&ring::digest::SHA256, data)
            .as_ref()
            .try_into()
            .unwrap()
    }

    #[test]
    fn standard_vectors() {
        for input in [
            b"".as_slice(),
            b"abc".as_slice(),
            b"The quick brown fox jumps over the lazy dog".as_slice(),
        ] {
            assert_eq!(digest(input), ring_digest(input));
        }
    }

    #[test]
    fn incremental_boundaries_match_ring() {
        let data: Vec<u8> = (0..4099).map(|index| (index * 131) as u8).collect();
        let expected = ring_digest(&data);
        for chunk_size in [1, 2, 3, 7, 31, 63, 64, 65, 127, 128, 257, 1024] {
            let mut hasher = Sha256::new();
            for chunk in data.chunks(chunk_size) {
                hasher.update(chunk);
            }
            assert_eq!(hasher.finalize(), expected, "chunk size {chunk_size}");
        }
    }

    #[cfg(target_arch = "riscv64")]
    #[test]
    fn scalar_compressor_matches_ring_for_offsets_and_chunks() {
        for len in 0..512 {
            let storage: Vec<u8> = (0..len + 8).map(|index| (index * 73) as u8).collect();
            for offset in 0..8 {
                let input = &storage[offset..offset + len];
                let mut hasher = Riscv64Sha256::new();
                for chunk in input.chunks((len % 67).max(1)) {
                    hasher.update(chunk);
                }
                assert_eq!(hasher.finalize(), ring_digest(input));
            }
        }
    }

    #[cfg(target_arch = "riscv64")]
    #[test]
    fn vector_compressor_matches_scalar_when_available() {
        if !riscv64_vector_sha256_available() {
            return;
        }

        let mut blocks = AlignedBlock([0; BLOCK_LEN]);
        for (index, byte) in blocks.0.iter_mut().enumerate() {
            *byte = (index * 197) as u8;
        }
        let mut vector_state = INITIAL_STATE;
        let mut scalar_state = INITIAL_STATE;
        // SAFETY: availability was checked above and the aligned block and
        // state meet the compression entry point's contract.
        unsafe {
            riscv64::compress_zvknh(vector_state.as_mut_ptr(), blocks.0.as_ptr(), 1);
        }
        compress_scalar(&mut scalar_state, &blocks.0);
        assert_eq!(vector_state, scalar_state);
    }

    #[cfg(target_arch = "riscv64")]
    proptest! {
        #[test]
        fn native_incremental_matches_ring_for_arbitrary_offsets_and_chunks(
            prefix in 0_usize..16,
            data in proptest::collection::vec(any::<u8>(), 0..16_384),
            boundaries in proptest::collection::vec(0_usize..16_384, 0..64),
        ) {
            let mut storage = vec![0xa5; prefix];
            storage.extend_from_slice(&data);
            let input = &storage[prefix..];
            let mut hasher = Riscv64Sha256::new();
            let mut start = 0;
            for end in boundaries {
                let end = end.min(input.len()).max(start);
                hasher.update(&input[start..end]);
                start = end;
            }
            hasher.update(&input[start..]);
            prop_assert_eq!(hasher.finalize(), ring_digest(input));
        }
    }
}
