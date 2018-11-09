extern crate exonum_rocksdb;
extern crate bincode;

use std::result::Result;
use self::exonum_rocksdb::{DB, WriteBatch};

//to do use result as return code sets for better code.
pub fn init_super_account(db: &DB, super_id: &String, total_supply: usize) -> Result<(), &'static str> {
    if super_id.len() == 0 {
        return Err("Invalid account id!");
    }

    if total_supply == 0 {
        return Err("Invalid total supply!");
    }

    println!("Going to init token id system with super account '{}' and totall supply:{}", super_id, total_supply);

    let mut batch = WriteBatch::default();
    let _ = batch.put(b"SuperAccount", &super_id.as_bytes());
    let totals = bincode::serde::serialize(&total_supply, bincode::SizeLimit::Infinite).unwrap();
    let _ = batch.put(b"TotalSupply", &totals);
    let _ = batch.put(b"SuperBalance", &totals);
    //atomic commit batch.

    match db.write(batch) {
        Ok(_) => {
            println!("You have super account '{}' initailized with total supply: {}.", super_id, total_supply);
        },
        Err(e) => {
            println!("operational problem encountered: {}", e);
            return Err("operational problem encountered!");
        }
    }

    Ok(())
}
