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
pub struct LimitedEdition {
    master_key: Pubkey,

    edition_num: Option<i64>,
}

impl ToSql for LimitedEdition {
    fn to_sql(
        &self,
        ty: &postgres_types::Type,
        w: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        use bytes::BufMut;
        w.put_slice(self.master_key.as_ref());
        self.edition_num.to_sql(ty, w)?;
        Ok(IsNull::No)
    }

      fn accepts(ty: &Type) -> bool {
        return ty.name() == "limited_edition";
      }

    postgres_types::to_sql_checked!();
}

impl From<bb::LimitedEdition> for LimitedEdition {
    fn from(e: bb::LimitedEdition) -> Self {
        Self {
            master_key: e.master_key,
            edition_num: e.edition_num,
        }
    }
}

