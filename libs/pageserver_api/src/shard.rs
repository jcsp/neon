use std::{ops::RangeInclusive, str::FromStr};

use crate::key::Key;
use hex::FromHex;
use serde::{Deserialize, Serialize};
use utils::id::TenantId;

#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Copy, Serialize, Deserialize, Debug)]
pub struct ShardNumber(pub u8);

#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Copy, Serialize, Deserialize, Debug)]
pub struct ShardCount(pub u8);

impl ShardCount {
    pub const MAX: Self = Self(u8::MAX);
}

impl ShardNumber {
    pub const MAX: Self = Self(u8::MAX);
}

/// TenantShardId identify the units of work for the Pageserver.
///
/// These are written as `<tenant_id>-<shard number><shard-count>`, for example:
///
///   # The second shard in a two-shard tenant
///   072f1291a5310026820b2fe4b2968934-0102
///
/// Historically, tenants could not have multiple shards, and were identified
/// by TenantId.  To support this, TenantShardId has a special legacy
/// mode where `shard_count` is equal to zero: this represents a single-sharded
/// tenant which should be written as a TenantId with no suffix.
///
/// The human-readable encoding of TenantShardId, such as used in API URLs,
/// is both forward and backward compatible: a legacy TenantId can be
/// decoded as a TenantShardId, and when re-encoded it will be parseable
/// as a TenantId.
///
/// Note that the binary encoding is _not_ backward compatible, because
/// at the time sharding is introduced, there are no existing binary structures
/// containing TenantId that we need to handle.
#[derive(Eq, PartialEq, PartialOrd, Ord, Clone, Copy)]
pub struct TenantShardId {
    pub tenant_id: TenantId,
    pub shard_number: ShardNumber,
    pub shard_count: ShardCount,
}

impl TenantShardId {
    pub fn unsharded(tenant_id: TenantId) -> Self {
        Self {
            tenant_id,
            shard_number: ShardNumber(0),
            shard_count: ShardCount(0),
        }
    }

    /// The range of all TenantShardId that belong to a particular TenantId.  This is useful when
    /// you have a BTreeMap of TenantShardId, and are querying by TenantId.
    pub fn tenant_range(tenant_id: TenantId) -> RangeInclusive<Self> {
        RangeInclusive::new(
            Self {
                tenant_id,
                shard_number: ShardNumber(0),
                shard_count: ShardCount(0),
            },
            Self {
                tenant_id,
                shard_number: ShardNumber::MAX,
                shard_count: ShardCount::MAX,
            },
        )
    }

    pub fn shard_slug(&self) -> String {
        format!("{:02x}{:02x}", self.shard_number.0, self.shard_count.0)
    }
}

impl std::fmt::Display for TenantShardId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.shard_count != ShardCount(0) {
            write!(
                f,
                "{}-{:02x}{:02x}",
                self.tenant_id, self.shard_number.0, self.shard_count.0
            )
        } else {
            // Legacy case (shard_count == 0) -- format as just the tenant id.  Note that this
            // is distinct from the normal single shard case (shard count == 1).
            self.tenant_id.fmt(f)
        }
    }
}

impl std::fmt::Debug for TenantShardId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Debug is the same as Display: the compact hex representation
        write!(f, "{}", self)
    }
}

impl std::str::FromStr for TenantShardId {
    type Err = hex::FromHexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Expect format: 16 byte TenantId, '-', 1 byte shard number, 1 byte shard count
        if s.len() == 32 {
            // Legacy case: no shard specified
            Ok(Self {
                tenant_id: TenantId::from_str(s)?,
                shard_number: ShardNumber(0),
                shard_count: ShardCount(0),
            })
        } else if s.len() == 37 {
            let bytes = s.as_bytes();
            let tenant_id = TenantId::from_hex(&bytes[0..32])?;
            let mut shard_parts: [u8; 2] = [0u8; 2];
            hex::decode_to_slice(&bytes[33..37], &mut shard_parts)?;
            Ok(Self {
                tenant_id,
                shard_number: ShardNumber(shard_parts[0]),
                shard_count: ShardCount(shard_parts[1]),
            })
        } else {
            Err(hex::FromHexError::InvalidStringLength)
        }
    }
}

impl From<[u8; 18]> for TenantShardId {
    fn from(b: [u8; 18]) -> Self {
        let tenant_id_bytes: [u8; 16] = b[0..16].try_into().unwrap();

        Self {
            tenant_id: TenantId::from(tenant_id_bytes),
            shard_number: ShardNumber(b[16]),
            shard_count: ShardCount(b[17]),
        }
    }
}

impl ShardNumber {}

/// Stripe size in number of pages
#[derive(Clone, Copy, Serialize, Deserialize, Eq, PartialEq, Debug)]
pub struct ShardStripeSize(pub u32);

/// Layout version: for future upgrades where we might change how the key->shard mapping works
#[derive(Clone, Copy, Serialize, Deserialize, Eq, PartialEq, Debug)]
pub struct ShardLayout(u8);

const LAYOUT_V1: ShardLayout = ShardLayout(1);

/// Default stripe size in pages: 256MiB divided by 8kiB page size.
const DEFAULT_STRIPE_SIZE: ShardStripeSize = ShardStripeSize(256 * 1024 / 8);

/// The ShardIdentity contains the information needed for one member of map
/// to resolve a key to a shard, and then check whether that shard is ==self.
#[derive(Clone, Copy, Serialize, Deserialize, Eq, PartialEq, Debug)]
pub struct ShardIdentity {
    pub layout: ShardLayout,
    pub number: ShardNumber,
    pub count: ShardCount,
    pub stripe_size: ShardStripeSize,
}

impl ShardIdentity {
    /// An identity with number=0 count=0 is a "none" identity, which represents legacy
    /// tenants.  Modern single-shard tenants should not use this: they should
    /// have number=0 count=1.
    pub fn none() -> Self {
        Self {
            number: ShardNumber(0),
            count: ShardCount(0),
            layout: LAYOUT_V1,
            stripe_size: DEFAULT_STRIPE_SIZE,
        }
    }

    pub fn new(number: ShardNumber, count: ShardCount, stripe_size: ShardStripeSize) -> Self {
        Self {
            number,
            count,
            layout: LAYOUT_V1,
            stripe_size,
        }
    }

    pub fn get_shard_number(&self, key: &Key) -> ShardNumber {
        key_to_shard_number(self.count, self.stripe_size, key)
    }

    /// Return true if the key should be ingested by this shard
    pub fn is_key_local(&self, key: &Key) -> bool {
        if self.count < ShardCount(2) || key_is_broadcast(key) {
            true
        } else {
            key_to_shard_number(self.count, self.stripe_size, key) == self.number
        }
    }

    pub fn slug(&self) -> String {
        if self.count > ShardCount(0) {
            format!("-{:02x}{:02x}", self.number.0, self.count.0)
        } else {
            String::new()
        }
    }
}

impl Serialize for TenantShardId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            serializer.collect_str(self)
        } else {
            let mut packed: [u8; 18] = [0; 18];
            packed[0..16].clone_from_slice(&self.tenant_id.as_arr());
            packed[16] = self.shard_number.0;
            packed[17] = self.shard_count.0;

            packed.serialize(serializer)
        }
    }
}

impl Default for ShardIdentity {
    /// The default identity is to be the only shard for a tenant, i.e. the legacy
    /// pre-sharding case.
    fn default() -> Self {
        ShardIdentity {
            layout: LAYOUT_V1,
            number: ShardNumber(0),
            count: ShardCount(1),
            stripe_size: DEFAULT_STRIPE_SIZE,
        }
    }
}

impl<'de> Deserialize<'de> for TenantShardId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct IdVisitor {
            is_human_readable_deserializer: bool,
        }

        impl<'de> serde::de::Visitor<'de> for IdVisitor {
            type Value = TenantShardId;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                if self.is_human_readable_deserializer {
                    formatter.write_str("value in form of hex string")
                } else {
                    formatter.write_str("value in form of integer array([u8; 18])")
                }
            }

            fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let s = serde::de::value::SeqAccessDeserializer::new(seq);
                let id: [u8; 18] = Deserialize::deserialize(s)?;
                Ok(TenantShardId::from(id))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                TenantShardId::from_str(v).map_err(E::custom)
            }
        }

        if deserializer.is_human_readable() {
            deserializer.deserialize_str(IdVisitor {
                is_human_readable_deserializer: true,
            })
        } else {
            deserializer.deserialize_tuple(
                18,
                IdVisitor {
                    is_human_readable_deserializer: false,
                },
            )
        }
    }
}

fn key_is_broadcast(key: &Key) -> bool {
    // TODO: deduplicate wrt pgdatadir_mapping.rs
    fn is_rel_block_key(key: &Key) -> bool {
        key.field1 == 0x00 && key.field4 != 0
    }

    // TODO: can we be less conservative?  Starting point is to broadcast everything
    // except for rel block keys
    !is_rel_block_key(key)
}

/// Provide the same result as the function in postgres `hashfn.h` with the same name
fn murmurhash32(mut h: u32) -> u32 {
    h ^= h >> 16;
    h = h.wrapping_mul(0x85ebca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2ae35);
    h ^= h >> 16;
    h
}

/// Provide the same result as the function in postgres `hashfn.h` with the same name
fn hash_combine(mut a: u32, mut b: u32) -> u32 {
    b = b.wrapping_add(0x9e3779b9);
    b = b.wrapping_add(a << 6);
    b = b.wrapping_add(a >> 2);

    a ^= b;
    a
}

/// Where a Key is to be distributed across shards, select the shard.  This function
/// does not account for keys that should be broadcast across shards.
///
/// The hashing in this function must exactly match what we do in postgres smgr
/// code.  The resulting distribution of pages is intended to preserve locality within
/// `stripe_size` ranges of contiguous block numbers in the same relation, while otherwise
/// distributing data pseudo-randomly.
///
/// The mapping of key to shard is not stable across changes to ShardCount: this is intentional
/// and will be handled at higher levels when shards are split.
fn key_to_shard_number(count: ShardCount, stripe_size: ShardStripeSize, key: &Key) -> ShardNumber {
    // Fast path for un-sharded tenants or broadcast keys
    if count < ShardCount(2) || key_is_broadcast(key) {
        return ShardNumber(0);
    }

    // spcNode
    let mut hash = murmurhash32(key.field2);
    // dbNode
    hash = hash_combine(hash, murmurhash32(key.field3));
    // relNode
    hash = hash_combine(hash, murmurhash32(key.field4));
    // blockNum/stripe size
    hash = hash_combine(hash, murmurhash32(key.field6 / stripe_size.0));

    ShardNumber((hash % count.0 as u32) as u8)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use bincode;
    use utils::{id::TenantId, Hex};

    use super::*;

    const EXAMPLE_TENANT_ID: &str = "1f359dd625e519a1a4e8d7509690f6fc";

    #[test]
    fn tenant_shard_id_string() -> Result<(), hex::FromHexError> {
        let example = TenantShardId {
            tenant_id: TenantId::from_str(EXAMPLE_TENANT_ID).unwrap(),
            shard_count: ShardCount(10),
            shard_number: ShardNumber(7),
        };

        let encoded = format!("{example}");

        let expected = format!("{EXAMPLE_TENANT_ID}-070a");
        assert_eq!(&encoded, &expected);

        let decoded = TenantShardId::from_str(&encoded)?;

        assert_eq!(example, decoded);

        Ok(())
    }

    #[test]
    fn tenant_shard_id_binary() -> Result<(), hex::FromHexError> {
        let example = TenantShardId {
            tenant_id: TenantId::from_str(EXAMPLE_TENANT_ID).unwrap(),
            shard_count: ShardCount(10),
            shard_number: ShardNumber(7),
        };

        let encoded = bincode::serialize(&example).unwrap();
        let expected: [u8; 18] = [
            0x1f, 0x35, 0x9d, 0xd6, 0x25, 0xe5, 0x19, 0xa1, 0xa4, 0xe8, 0xd7, 0x50, 0x96, 0x90,
            0xf6, 0xfc, 0x07, 0x0a,
        ];
        assert_eq!(Hex(&encoded), Hex(&expected));

        let decoded = bincode::deserialize(&encoded).unwrap();

        assert_eq!(example, decoded);

        Ok(())
    }

    #[test]
    fn tenant_shard_id_backward_compat() -> Result<(), hex::FromHexError> {
        // Test that TenantShardId can decode a TenantId in human
        // readable form
        let example = TenantId::from_str(EXAMPLE_TENANT_ID).unwrap();
        let encoded = format!("{example}");

        assert_eq!(&encoded, EXAMPLE_TENANT_ID);

        let decoded = TenantShardId::from_str(&encoded)?;

        assert_eq!(example, decoded.tenant_id);
        assert_eq!(decoded.shard_count, ShardCount(0));
        assert_eq!(decoded.shard_number, ShardNumber(0));

        Ok(())
    }

    #[test]
    fn tenant_shard_id_forward_compat() -> Result<(), hex::FromHexError> {
        // Test that a legacy TenantShardId encodes into a form that
        // can be decoded as TenantId
        let example_tenant_id = TenantId::from_str(EXAMPLE_TENANT_ID).unwrap();
        let example = TenantShardId::unsharded(example_tenant_id);
        let encoded = format!("{example}");

        assert_eq!(&encoded, EXAMPLE_TENANT_ID);

        let decoded = TenantId::from_str(&encoded)?;

        assert_eq!(example_tenant_id, decoded);

        Ok(())
    }

    #[test]
    fn tenant_shard_id_legacy_binary() -> Result<(), hex::FromHexError> {
        // Unlike in human readable encoding, binary encoding does not
        // do any special handling of legacy unsharded TenantIds: this test
        // is equivalent to the main test for binary encoding, just verifying
        // that the same behavior applies when we have used `unsharded()` to
        // construct a TenantShardId.
        let example = TenantShardId::unsharded(TenantId::from_str(EXAMPLE_TENANT_ID).unwrap());
        let encoded = bincode::serialize(&example).unwrap();

        let expected: [u8; 18] = [
            0x1f, 0x35, 0x9d, 0xd6, 0x25, 0xe5, 0x19, 0xa1, 0xa4, 0xe8, 0xd7, 0x50, 0x96, 0x90,
            0xf6, 0xfc, 0x00, 0x00,
        ];
        assert_eq!(Hex(&encoded), Hex(&expected));

        let decoded = bincode::deserialize::<TenantShardId>(&encoded).unwrap();
        assert_eq!(example, decoded);

        Ok(())
    }

    // These are only smoke tests to spot check that our implementation doesn't
    // deviate from a few examples values: not aiming to validate the overall
    // hashing algorithm.
    #[test]
    fn murmur_hash() {
        assert_eq!(murmurhash32(0), 0);

        assert_eq!(hash_combine(0xb1ff3b40, 0), 0xfb7923c9);
    }

    #[test]
    fn shard_mapping() {
        let key = Key {
            field1: 0x00,
            field2: 0x67f,
            field3: 0x5,
            field4: 0x400c,
            field5: 0x00,
            field6: 0x7d06,
        };

        let shard = key_to_shard_number(ShardCount(10), DEFAULT_STRIPE_SIZE, &key);
        assert_eq!(shard, ShardNumber(3));
    }
}
