use {
    bonbon::assemble as bb,
    postgres_types::*,
    solana_sdk::pubkey::Pubkey,
};

#[derive(Debug, ToSql)]
#[postgres(name = "edition_status")]
pub enum EditionStatus {
    // Edition has not been created. This state is used temporarily for every NFT we encounter
    // since the metadata must be created before the edition, but it could also be an...
    // - SFT
    // - NFT where mint auth is held by e.g cardinal
    #[postgres(name = "none")]
    None,

    #[postgres(name = "master")]
    Master,

    #[postgres(name = "limited")]
    Limited,
}

impl From<bb::EditionStatus> for EditionStatus {
    fn from(e: bb::EditionStatus) -> Self {
        match e {
            bb::EditionStatus::None => Self::None,
            bb::EditionStatus::Master => Self::Master,
            bb::EditionStatus::Limited => Self::Limited,
        }
    }
}


#[derive(Debug)]
pub struct SqlPubkey(pub Pubkey);

impl ToSql for SqlPubkey {
    fn to_sql(
        &self,
        _ty: &Type,
        w: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        use bytes::BufMut;
        w.put_slice(self.0.as_ref());
        Ok(IsNull::No)
    }

    postgres_types::accepts!(BYTEA);

    postgres_types::to_sql_checked!();
}

impl<'a> FromSql<'a> for SqlPubkey {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let fixed: [u8; 32] = raw.try_into()?;
        Ok(Self(Pubkey::new_from_array(fixed)))
    }

    postgres_types::accepts!(BYTEA);
}


#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "limited_edition")]
pub struct LimitedEdition {
    master_key: SqlPubkey,

    edition_num: Option<i64>,
}

impl From<bb::LimitedEdition> for LimitedEdition {
    fn from(e: bb::LimitedEdition) -> Self {
        Self {
            master_key: SqlPubkey(e.master_key),
            edition_num: e.edition_num,
        }
    }
}

