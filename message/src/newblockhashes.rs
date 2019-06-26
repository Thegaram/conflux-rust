// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

use crate::{Message, MsgId};
use cfx_types::H256;
use rlp::{Decodable, DecoderError, Encodable, Rlp, RlpStream};

#[derive(Debug, PartialEq)]
pub struct NewBlockHashes {
    pub block_hashes: Vec<H256>,
    pub best_epoch: u64,
}

impl Message for NewBlockHashes {
    fn msg_id(&self) -> MsgId { MsgId::NEW_BLOCK_HASHES }
}

impl Encodable for NewBlockHashes {
    fn rlp_append(&self, stream: &mut RlpStream) {
        stream
            .begin_list(2)
            .append_list(&self.block_hashes)
            .append(&self.best_epoch);
    }
}

impl Decodable for NewBlockHashes {
    fn decode(rlp: &Rlp) -> Result<Self, DecoderError> {
        if rlp.item_count()? != 2 {
            return Err(DecoderError::RlpIncorrectListLen);
        }

        Ok(NewBlockHashes {
            block_hashes: rlp.list_at(0)?,
            best_epoch: rlp.val_at(1)?,
        })
    }
}
