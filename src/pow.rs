// src/pow.rs

use primitive_types::U256;

pub fn u256_from_compact_target_bits(bits: u32) -> U256 {
    let (mant, expt) = {
        let unshifted_expt = bits >> 24;
        if unshifted_expt <= 3 {
            ((bits & 0xFFFFFF) >> (8 * (3 - unshifted_expt as usize)), 0)
        } else {
            (bits & 0xFFFFFF, 8 * ((bits >> 24) - 3))
        }
    };

    // The mantissa is signed but may not be negative
    if mant > 0x7FFFFF {
        U256::zero()
    } else {
        U256::from(mant as u64) << (expt as usize)
    }
}

pub fn difficulty(value: U256) -> u64 {
    if value.is_zero() {
        return u64::MAX;
    }
    // D = 2^256 / (value + 1)
    let mut adjusted = value;
    adjusted += U256::one();
    let full = U256::MAX / adjusted;
    full.low_u64()
}