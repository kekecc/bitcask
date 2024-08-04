use anyhow::{Ok, Result};

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

#[inline(always)]
pub fn check_key_valid(key: &Key) -> Result<()> {
    if key.is_empty() {
        return Err(anyhow::Error::msg("key is not valid!"));
    }

    Ok(())
}


