// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

mod ledger_info;
mod ledger_proof;
mod peers;
mod unique_id;
mod validate;

pub use ledger_info::LedgerInfo;
pub use ledger_proof::LedgerProof;
pub use peers::Peers;
pub use unique_id::UniqueId;
pub use validate::Validate;

use std::cmp;

pub fn max_of_collection<I, T: Ord>(collection: I) -> Option<T>
where I: Iterator<Item = T> {
    collection.fold(None, |max_so_far, x| match max_so_far {
        None => Some(x),
        Some(max_so_far) => Some(cmp::max(max_so_far, x)),
    })
}

extern crate futures;
use futures::{Async, Future, Stream};

use crate::parameters::light::{MAX_POLL_TIME_MS, POLL_PERIOD_MS};

pub fn poll_next<T: Stream>(stream: &mut T) -> Option<T::Item>
where T::Item: std::fmt::Debug {
    // poll result
    // TODO(thegaram): come up with something better
    // we can consider returning a future if it is
    // compatible with our current event loop
    let max_poll_num = MAX_POLL_TIME_MS / POLL_PERIOD_MS;

    for ii in 0..max_poll_num {
        info!("poll number {}", ii);
        match stream.poll() {
            Ok(Async::Ready(resp)) => {
                info!("poll result: {:?}", resp);
                return resp;
            }
            Ok(Async::NotReady) => {
                info!("poll result: NotReady");
                ()
            }
            Err(_) => {
                info!("poll result: Error");
                return None; // TODO
            }
        }

        info!("sleeping...");
        let d = std::time::Duration::from_millis(POLL_PERIOD_MS);
        std::thread::sleep(d);
    }

    None // TODO
}

pub fn poll<T: Future>(future: &mut T) -> Option<T::Item>
where T::Item: std::fmt::Debug {
    // poll result
    // TODO(thegaram): come up with something better
    // we can consider returning a future if it is
    // compatible with our current event loop
    let max_poll_num = MAX_POLL_TIME_MS / POLL_PERIOD_MS;

    for ii in 0..max_poll_num {
        info!("poll number {}", ii);
        match future.poll() {
            Ok(Async::Ready(resp)) => {
                info!("poll result: {:?}", resp);
                return Some(resp);
            }
            Ok(Async::NotReady) => {
                info!("poll result: NotReady");
                ()
            }
            Err(_) => {
                info!("poll result: Error");
                return None; // TODO
            }
        }

        info!("sleeping...");
        let d = std::time::Duration::from_millis(POLL_PERIOD_MS);
        std::thread::sleep(d);
    }

    None // TODO
}
