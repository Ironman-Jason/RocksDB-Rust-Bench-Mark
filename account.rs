extern crate exonum_rocksdb;
extern crate bincode;

use std::result::Result;
use self::exonum_rocksdb::{DB, WriteBatch};

//get balance for account.
pub fn get_balance(db: &DB, account: &String) -> Result<usize, &'static str> {
    if account.len() == 0 {
        return Err("invalide account id.");
    }
	
    if get_super_account_id(db).unwrap() == *account {
        match get_super_account_value(db, "SuperBalance") {
            Ok(value) => {
                return Ok(value);
            },
            Err(e) => {
                println!("Got error: {:?}", e);
                return Err("get_super_account_value error.");
            },
        }
    }

    unsafe {
    	let mut iter = db.raw_iterator();
    	iter.seek(&account.as_bytes());
    	if iter.valid() {
            let len: usize = bincode::serde::deserialize(&iter.value_inner().unwrap()[0..8]).unwrap();
            println!("get '{}' balance: {}", account,len);
            return Ok(len);
        }
        return Err("Account not found!");
    }
}

//to do use result as return code sets for better code.
pub fn transfer(db: &DB, from: &String, to: &String, amount: usize) -> Result<(), &'static str> {
    
    if from.len() == 0 {
        return Err("Wrong sender id!");
    }

    if to.len() == 0 {
        return Err("Wrong receiver id!");
    }

    if amount == 0 {
        return Err("Invalid amount to transfer!");
    }

    //try to write below nested match more readable in rust.
    match get_super_account_id(db) {
        Ok(super_id) => {
            if super_id == *to {
                return Err("transfer to super account is not allowed.");
            }

            if super_id == *from {
                match rollout_tokens(db, to, amount) {
                    Ok(()) => {
                        return Ok(());
                    },
                    Err(e) => {
                        println!("Got erro: {:?}", e);
                        return Err("rollout_tokens failed.");
                    },
                }
            }

            match end_user_transfer(db, from, to, amount) {
                Ok(()) => {
                    return Ok(());
                },
                Err(e) => {
                    println!("Got error: {:?}", e);
                    return Err("end_user_transfer failed");
                },
            }

        },
        Err(e) => {
            println!("Got error: {:?}", e);
            return Err("Get super account id failed.");
        },
    }
}

fn get_super_account_id(db: &DB) -> Result<String, &'static str> {
    match db.get(b"SuperAccount") {
        Ok(Some(value)) => {
            let super_id = value.to_utf8().unwrap().to_string();
            Ok(super_id)
        },
        Ok(None) => {
            return Err("the database haven't initailized with super account and total supply.");
        },
        Err(e) => {
            println!("Got error: {:?}", e);
            return Err("operational problem encountered.");
        },
    }
}

fn get_super_account_value(db: &DB, key: &str) -> Result<usize, &'static str>  {
	
    match db.get(key.as_bytes()) {
        Ok(Some(bytes)) =>{
            let mut value: usize= bincode::serde::deserialize(&bytes).unwrap();
            println!("get {}: {}", key, value);
            Ok(value)
        },
        Ok(None) => {
            return Err("super account meta is not found from db!");   
        }
        Err(e) => {
            println!("Got error: {:?}", e);
            return Err("rollout_tokens operational problem encountered.");
        },
    }
}

//roll out tokens from super account to end user account.
fn rollout_tokens(db: &DB, to: &String, amount: usize) -> Result<(), &'static str> {
	
    //get super account meta from db.
    let super_balance: usize = get_super_account_value(db, "SuperBalance").unwrap();
    let super_total_supply: usize = get_super_account_value(db, "TotalSupply").unwrap();

    if super_balance < amount {
        return Err("super does not have enough balance to roll out.");
    }
	
    //to do roll out:
    let start_id = super_total_supply - super_balance;
    let end_id = start_id + amount;
	
    //below could be improved since the heap could not hold a big array.
    let mut token_ids: Vec<usize> = Vec::new();
    token_ids.reserve(amount);

    for id in start_id .. end_id {
        token_ids.push(id);
    }

    let bytes = bincode::serde::serialize(&token_ids, bincode::SizeLimit::Infinite).unwrap();

    println!("Going to roll out {} from super account to end user: '{}'", amount, to);
    //atomic commit: update of super balance, end-user's balance array.
    let mut batch = WriteBatch::default();
    let new_balance = super_balance - amount;
    let balance_bytes = bincode::serde::serialize(&new_balance, bincode::SizeLimit::Infinite).unwrap();
    let _ = batch.put(b"SuperBalance", &balance_bytes);
    let _ = batch.merge(&to.as_bytes(), &bytes);

    match db.write(batch) {
        Ok(_) => {
            println!("Roll out tokens done.");
        },
        Err(e) => {
            println!("Got error: {:?}", e);
            return Err("operational problem encountered.");
        },
    }
    Ok(())
}

//transfer tokens between end-user's accounts.
fn end_user_transfer(db: &DB, from: &String, to: &String, amount: usize) -> Result<(), &'static str> {
    
    let mut sender_balance: Vec<usize>;
    //in production, below get and update should be controlled as atomic operation.
    //get sender's account balance.
    match db.get(&from.as_bytes()) {
        Ok(Some(value)) =>{
            sender_balance = bincode::serde::deserialize(&value).unwrap();
        },
        Ok(None) => {
            return Err("tansfer token, cannot find sender account from db.");
        },
        Err(e) => {
            println!("Got error: {:?}", e);
            return Err("operational problem encountered.");
        },
    }

    if sender_balance.len() < amount {
        return Err("The sender does not have enough token!");
    }

    //split into to 2 vectors.
    let index = sender_balance.len() - amount;
    let tx_tokens = sender_balance.split_off(index);

    println!("Transfering from '{}' to '{}' with {} tokens", from, to, amount);

    let sender_balance_bytes = bincode::serde::serialize(&sender_balance, bincode::SizeLimit::Infinite).unwrap();
    let tx_tokens_bytes = bincode::serde::serialize(&tx_tokens, bincode::SizeLimit::Infinite).unwrap();

    //commit to db.
    let mut batch = WriteBatch::default();
    let _ = batch.put(&from.as_bytes(), &sender_balance_bytes);
    let _ = batch.merge(&to.as_bytes(), &tx_tokens_bytes);
    
    match db.write(batch) {
        Ok(_) => {
            println!("'{}' have {} tokens left, {} tokens to be transfered.", from, sender_balance.len(), tx_tokens.len());
        },
        Err(e) => {
            println!("Got error: {:?}", e);
            return Err("operational problem encountered");
        },
    }

    Ok(())
}


