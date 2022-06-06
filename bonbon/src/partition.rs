use {
    borsh::de::BorshDeserialize,
    mpl_token_metadata::instruction::MetadataInstruction,
    spl_token::instruction::{AuthorityType, TokenInstruction},
    solana_account_decoder::StringAmount,
    solana_sdk::{
        instruction::CompiledInstruction,
        message::{VersionedMessage, AccountKeys},
        pubkey::Pubkey,
    },
    solana_transaction_status::{
        TransactionWithStatusMeta,
        TransactionTokenBalance,
    },
    std::collections::HashMap,
};

#[derive(Debug)]
pub struct TransactionTokenMeta {
    pub account_index: u8,

    pub decimals: u8,

    pub pre_amount: Option<StringAmount>,

    pub post_amount: Option<StringAmount>,

    pub mint_key: Pubkey,
}

pub struct InstructionContext<'a, 'k> {
    instruction: &'a CompiledInstruction,

    account_keys: &'a AccountKeys<'k>,

    token_metas: &'a [TransactionTokenMeta],

    transient_metas: &'a mut Vec<TransactionTokenMeta>,
}

pub struct InstructionPartitioner {
    pub program_id: Pubkey,

    pub partitioner: fn (
        instruction_context: InstructionContext,
    ) -> Result<Option<Pubkey>, ErrorCode>,
}

// NB: only returns a value for instructions that are 'likely' to contain an NFT-related token
// instruction (i.e heuristic based on mint, amount, etc)
pub fn partition_token_instruction(
    InstructionContext {
        instruction, account_keys, token_metas, transient_metas,
    }: InstructionContext,
) -> Result<Option<Pubkey>, ErrorCode> {
    let get_account_key = |index: usize| account_keys.get(
        instruction.accounts[index].into()
    ).ok_or(ErrorCode::BadAccountKeyIndex);
    let get_token_meta_for = |index: usize| {
        let index = instruction.accounts[index];
        if let Some(v) = token_metas.iter().find(|m| m.account_index == index) {
            Some(v)
        } else {
            transient_metas.iter().find(|m| m.account_index == index)
        }
    };

    // TODO: less jank. filter/parse all these upfront?
    let heuristic_token_meta_ok = |meta: &TransactionTokenMeta| {
        let amount_ok = |amount: &Option<StringAmount>| {
            match amount {
                Some(amount) => amount.len() == 1
                    && (amount.as_bytes()[0] == 0x30 // 0
                        || amount.as_bytes()[0] == 0x31), // or 1
                None => true,
            }
        };
        meta.decimals == 0 && amount_ok(&meta.pre_amount) && amount_ok(&meta.post_amount)
    };

    let token_account_mint_key = |index| -> Result<Option<Pubkey>, ErrorCode> {
        let token_meta = get_token_meta_for(index)
            .ok_or(ErrorCode::BadTokenMetaAccountIndex)?;
        Ok(heuristic_token_meta_ok(token_meta)
            .then(|| token_meta.mint_key))
    };

    let add_transient_token_meta = |
        transient_metas: &mut Vec<TransactionTokenMeta>,
    | -> Result<(), ErrorCode> {
        // didn't show up in pre- or post- balances so could be transient...
        transient_metas.push(TransactionTokenMeta {
            account_index: instruction.accounts[0],
            decimals: 1, // shouldn't matter...
            pre_amount: None,
            post_amount: None,
            mint_key: *get_account_key(1)?,
        });
        Ok(())
    };

    let token_instruction = TokenInstruction::unpack(&instruction.data)
        .map_err(|_| ErrorCode::FailedInstructionDeserialization)?;

    match token_instruction {
        TokenInstruction::InitializeMint { decimals, .. } => {
            if decimals != 0 {
                Ok(None)
            } else {
                Ok(Some(*get_account_key(0)?))
            }
        },
        TokenInstruction::InitializeAccount { .. } => {
            Ok(match get_token_meta_for(0) {
                Some(token_meta) => heuristic_token_meta_ok(token_meta)
                    .then(|| token_meta.mint_key),
                None => {
                    add_transient_token_meta(transient_metas)?;
                    None
                }
            })
        },
        TokenInstruction::InitializeAccount2 { .. } => {
            Ok(match get_token_meta_for(0) {
                Some(token_meta) => heuristic_token_meta_ok(token_meta)
                    .then(|| token_meta.mint_key),
                None => {
                    add_transient_token_meta(transient_metas)?;
                    None
                }
            })
        },
        TokenInstruction::InitializeMultisig { .. } => {
            Ok(None)
        }
        TokenInstruction::Transfer { amount } => {
            if amount > 1 {
                return Ok(None);
            }
            token_account_mint_key(0)
        }
        TokenInstruction::Approve { amount } => {
            if amount > 1 {
                return Ok(None);
            }
            token_account_mint_key(0)
        }
        TokenInstruction::Revoke => {
            token_account_mint_key(0)
        }
        TokenInstruction::SetAuthority { authority_type, .. } => {
            match authority_type {
                // TODO: we probably don't care about this case?
                // might be related to nft mint but shouldn't impact our handling...
                AuthorityType::MintTokens => {
                    Ok(None)
                }
                // here we could be changing ownership (aka transfer) so do handle this one...
                _ => token_account_mint_key(0)
            }
        }
        TokenInstruction::MintTo { amount } => {
            if amount > 1 {
                return Ok(None);
            }
            token_account_mint_key(1)
        }
        TokenInstruction::Burn { amount } => {
            if amount > 1 {
                return Ok(None);
            }
            token_account_mint_key(0)
        }
        TokenInstruction::CloseAccount => {
            // mints can't be closed and a token account must have zero balance to be closed so...
            if let Some(index) = transient_metas.iter().position(
                    |m| m.account_index == instruction.accounts[0]) {
                transient_metas.swap_remove(index);
            }
            Ok(None)
        }
        TokenInstruction::FreezeAccount => {
            // not really important...
            token_account_mint_key(0)
        }
        TokenInstruction::ThawAccount => {
            // not really important...
            token_account_mint_key(0)
        }
        TokenInstruction::TransferChecked { amount, decimals } => {
            if decimals != 0 || amount > 1 {
                return Ok(None);
            }
            token_account_mint_key(0)
        }
        TokenInstruction::ApproveChecked { amount, decimals } => {
            if decimals != 0 || amount > 1 {
                return Ok(None);
            }
            token_account_mint_key(0)
        }
        TokenInstruction::MintToChecked { amount, decimals } => {
            if decimals != 0 || amount > 1 {
                return Ok(None);
            }
            token_account_mint_key(1)
        }
        TokenInstruction::BurnChecked { amount, decimals } => {
            if decimals != 0 || amount > 1 {
                return Ok(None);
            }
            token_account_mint_key(0)
        }
        // InitializeAccount2 above
        TokenInstruction::SyncNative => {
            Ok(None)
        }
    }
}

pub fn partition_metadata_instruction(
    InstructionContext {
        instruction, account_keys, ..
    }: InstructionContext,
) -> Result<Option<Pubkey>, ErrorCode> {
    let get_account_key = |index: usize| account_keys.get(
        instruction.accounts[index].into()
    ).ok_or(ErrorCode::BadAccountKeyIndex);
    // TODO: skip check for SetReservationList:
    // metaplex-foundation/metaplex/commit/3e26b6b208900181a9c42362f206690544467be9,
    // this instruction's arguments change. we don't actually care about this instruction atm so
    // just ignore it early...

    let metadata_instruction = MetadataInstruction::try_from_slice(&instruction.data)
        .map_err(|_| ErrorCode::FailedInstructionDeserialization)?;

    let partition_key = match metadata_instruction {
        MetadataInstruction::CreateMetadataAccount(_) => {
            // OG create metadata
            get_account_key(0)?
        },
        MetadataInstruction::CreateMetadataAccountV2(_) => {
            // create metadata with datav2 (adds collection info, etc)
            get_account_key(0)?
        },
        MetadataInstruction::UpdateMetadataAccount(_) => {
            get_account_key(0)?
        },
        MetadataInstruction::UpdateMetadataAccountV2(_) => {
            get_account_key(0)?
        },
        MetadataInstruction::DeprecatedCreateMasterEdition(_) => {
            // master edition with printing tokens (and reservation list?)
            get_account_key(7)?
        }
        MetadataInstruction::CreateMasterEdition(_) => {
            // edition v2 w/ bitvec directly
            get_account_key(5)?
        }
        MetadataInstruction::CreateMasterEditionV3(_) => {
            // not sure why this exists
            get_account_key(5)?
        }
        MetadataInstruction::DeprecatedMintNewEditionFromMasterEditionViaPrintingToken => {
            // TODO: we need to track downstream that this parsing new-edition nfts instructions
            // depends on the master edition

            // in metaplex-foundation/metaplex/commit/a29aa4cfd5c75307892254ee5ee311ca64101ea0,
            // the master metadata account goes from index 10 to index 11. before, this commit, the
            // token program was 11
            let pivot_key = get_account_key(11)?;
            let _master_key = if pivot_key == &spl_token::id() {
                get_account_key(10)?
            } else {
                pivot_key
            };

            get_account_key(0)?
        }
        MetadataInstruction::MintNewEditionFromMasterEditionViaToken(_)=> {
            let _master_key = get_account_key(10)?;
            get_account_key(0)?
        }
        MetadataInstruction::MintNewEditionFromMasterEditionViaVaultProxy(_)=> {
            let _master_key = get_account_key(12)?;
            get_account_key(0)?
        }
        MetadataInstruction::SignMetadata => {
            get_account_key(0)?
        }
        MetadataInstruction::RemoveCreatorVerification => {
            get_account_key(0)?
        }
        MetadataInstruction::VerifyCollection => {
            get_account_key(0)?
        }
        MetadataInstruction::SetAndVerifyCollection => {
            get_account_key(0)?
        }
        MetadataInstruction::UnverifyCollection => {
            get_account_key(0)?
        }
        MetadataInstruction::UpdatePrimarySaleHappenedViaToken => {
            get_account_key(0)?
        }
        MetadataInstruction::DeprecatedSetReservationList(_) => {
            // see note above
            return Ok(None);
        }
        MetadataInstruction::DeprecatedCreateReservationList => {
            get_account_key(5)?
        }
        MetadataInstruction::DeprecatedMintPrintingTokensViaToken(_) => {
            get_account_key(5)?
        }
        MetadataInstruction::DeprecatedMintPrintingTokens(_) => {
            get_account_key(3)?
        }
        MetadataInstruction::ConvertMasterEditionV1ToV2 => {
            // TODO
            return Ok(None);
        }
        MetadataInstruction::PuffMetadata => {
            get_account_key(0)?
        }
        MetadataInstruction::Utilize(_) => {
            get_account_key(0)?
        }
        MetadataInstruction::ApproveUseAuthority(_) => {
            get_account_key(5)?
        }
        MetadataInstruction::RevokeUseAuthority => {
            get_account_key(5)?
        }
        MetadataInstruction::ApproveCollectionAuthority => {
            // this only changes authority for the collection nft...
            get_account_key(4)?
        }
        MetadataInstruction::RevokeCollectionAuthority => {
            // this only changes authority for the collection nft...
            get_account_key(3)?
        }
        MetadataInstruction::FreezeDelegatedAccount => {
            // TODO
            return Ok(None);
        }
        MetadataInstruction::ThawDelegatedAccount => {
            // TODO
            return Ok(None);
        }
    };

    Ok(Some(*partition_key))
}

pub fn partition_transaction(
    transaction: TransactionWithStatusMeta,
    partitioners: &[InstructionPartitioner]
) -> Result<Vec<PartitionedInstruction>, ErrorCode> {
    let status_meta = transaction.get_status_meta()
        .ok_or(ErrorCode::MissingTransactionStatusMeta)?;

    let account_keys = &transaction.account_keys();

    let meta_from_balance = |b: &TransactionTokenBalance| Ok(TransactionTokenMeta {
        account_index: b.account_index,
        decimals: b.ui_token_amount.decimals,
        pre_amount: None,
        post_amount: None,
        mint_key: Pubkey::new(bs58::decode(b.mint.clone()).into_vec()
            .map_err(|_| ErrorCode::BadPubkeyString)?.as_slice()),
    });

    let mut token_metas = HashMap::new();
    for balance in status_meta.pre_token_balances.into_iter().flatten() {
        let token_meta = meta_from_balance(&balance)?;
        let meta = token_metas.entry(balance.account_index).or_insert(token_meta);
        meta.pre_amount = Some(balance.ui_token_amount.amount);
    }

    for balance in status_meta.post_token_balances.into_iter().flatten() {
        let token_meta = meta_from_balance(&balance)?;
        let meta = token_metas.entry(balance.account_index).or_insert(token_meta);
        meta.post_amount = Some(balance.ui_token_amount.amount);
    }

    let token_metas = &token_metas.into_values().collect::<Vec<_>>();
    let mut transient_metas = vec![];

    let mut partitioned = vec![];
    let mut try_partition_instruction = |
        instruction: CompiledInstruction,
        outer_index: usize,
        inner_index: Option<usize>,
    | -> Result<(), ErrorCode> {
        let program_id = account_keys.get(usize::from(instruction.program_id_index))
            .ok_or(ErrorCode::BadAccountKeyIndex)?;

        if let Some(InstructionPartitioner { partitioner, .. }) = partitioners.iter().find(
            |p| &p.program_id == program_id) {
            let partition_key = partitioner(InstructionContext {
                instruction: &instruction,
                account_keys,
                token_metas,
                transient_metas: &mut transient_metas,
            })?;
            if partition_key.is_none() { return Ok(()); }
            partitioned.push(PartitionedInstruction {
                instruction,
                partition_key: partition_key.unwrap(),
                program_key: *program_id,
                outer_index: outer_index as i64,
                inner_index: inner_index.map(|v| v as i64),
            });
        }
        Ok(())
    };

    let message = transaction.get_transaction().message;

    let outer_instructions = match message {
        VersionedMessage::Legacy(message) => message.instructions,
        VersionedMessage::V0(message) => message.instructions,
    };

    let inner_instructions = status_meta.inner_instructions.unwrap_or(vec![]);
    let mut inner_instructions_iter = inner_instructions.into_iter().peekable();

    for (outer_index, instruction) in outer_instructions.into_iter().enumerate() {
        if let Some(inner) = &inner_instructions_iter.peek() {
            if usize::from(inner.index) == outer_index {
                let inner = inner_instructions_iter.next().unwrap();

                for (inner_index, instruction) in inner.instructions.into_iter().enumerate() {
                    try_partition_instruction(instruction, outer_index, Some(inner_index))?;
                }
            }
        }
        try_partition_instruction(instruction, outer_index, None)?;
    }

    if transient_metas.len() != 0 {
        return Err(ErrorCode::FailedTransientTokenAccountMatching);
    }

    Ok(partitioned)
}

pub struct PartitionedInstruction {
    pub instruction: CompiledInstruction,

    pub partition_key: Pubkey,

    pub program_key: Pubkey,

    pub outer_index: i64,

    pub inner_index: Option<i64>,
}

#[derive(Debug)]
pub enum ErrorCode {
    MissingTransactionStatusMeta,

    BadAccountKeyIndex,

    BadTokenMetaAccountIndex,

    BadPubkeyString,

    FailedInstructionDeserialization,

    FailedTransientTokenAccountMatching,
}

