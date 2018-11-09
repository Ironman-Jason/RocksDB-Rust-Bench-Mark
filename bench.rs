extern crate exonum_rocksdb;

use std::time::Instant;
use std::result::Result;
use self::exonum_rocksdb::DB;
use super::bootstrap::init_super_account;
use super::account::{get_balance, transfer};


pub fn bench_test(db: &mut DB, io: &str, num_of_tokens: usize, num_of_accounts: usize) -> Result<(), &'static str> {
    if io.len() == 0 {
        return Err("Invalid IO command!");
    }

    if num_of_tokens == 0 {
        return Err("Invalid amount of tokens");
    }

    if num_of_accounts ==  0 {
        return Err("Invalid amount of tokens");
    }

    if num_of_tokens * num_of_accounts > 50000000000 {
        println!("{} * {} > 50,000,000,000!", num_of_tokens, num_of_accounts);
        return Err("Your input: num_of_tokens * num_of_account exceed limitation: 50B tokens.");
    }

    match io {
        "I" => {
            match bench_test_write(db, num_of_tokens, num_of_accounts) {
	            Ok(()) => println!("Bench test done!"),
                Err(e) => {
                    println!("Got error: {:?}", e);
                    return Err("Bench test write failed.");   
                }
            }
        }
        "O" => {
            match bench_test_read(db, num_of_tokens, num_of_accounts) {
                Ok(()) => println!("Bench test done!"),
                Err(e) => {
                    println!("Got error: {:?}", e);
                    return Err("Bench test read failed.");   
                }
            }
        }
        "IO"=> {
            match bench_test_rw(db, num_of_tokens, num_of_accounts) {
                Ok(()) => println!("Bench test done!"),
                Err(e) => {
                    println!("Got error: {:?}", e);
                    return Err("Bench test rw failed.");   
                }
            }
        }
        _ => {
	        return Err("Invalid IO command");
        }
    }

    Ok(())
}

fn bench_test_write(db: &mut DB, num_of_tokens: usize, num_of_accounts: usize) -> Result<(), &'static str> {
    // before write test, drop the old data.
    match db.drop_cf("default") {
        Ok(()) => println!("Old data droped."),
        Err(_e) => println!("default cf does not existed, skip to drop cf."),
    }

    match init_super_account(db, &String::from("SUPER"), 50_000_000_000){
        Ok(()) => println!("Token id system is ready to use."),
        Err(e) => {
            println!("Got error: {:?}", e);
            return Err("Cannot init super account.");
        },
    }

    let start = Instant::now();
    let mut last_time_ms: u64 = 0;

    for id in 0 .. num_of_accounts {
        // hard code super account only for bench test.
        match transfer(db, &String::from("SUPER"), &format!("{}", id), num_of_tokens) {
            Ok(()) => {
                let elapsed = start.elapsed();
                let now_ms = (elapsed.as_secs() * 1_000) + (elapsed.subsec_nanos() / 1_000_000) as u64;
                println!("Last write cost: {} ms", now_ms - last_time_ms);
                last_time_ms = now_ms;
            }
            Err(e) => {
                println!("Got error {:?}", e);
                return Err("Transfer failed during bench test.");
            }
        }
    }

    let total_elapsed = start.elapsed();
    println!("Total test elapsed: {} ms",
             (total_elapsed.as_secs() * 1_000) + (total_elapsed.subsec_nanos() / 1_000_000) as u64);

    Ok(())
}

fn bench_test_read(db: &DB, num_of_tokens: usize, num_of_accounts: usize) -> Result<(), &'static str> {
    if num_of_tokens == 0 {
        return Err("Invalid num of tokens.");
    }
	
    if num_of_accounts == 0 {
        return Err("Invalid num of accounts.");
    }
	
    // before read test, check if the db have enough account for reading.
    let max_account_id = num_of_accounts - 1;
    match get_balance(db, &format!("{}", max_account_id)) {
        Ok(value) => println!("Account ID: {} have {} tokens.", max_account_id, value),
        Err(e) => {
            println!("Got error: {:?}", e);
            println!("You might need start bench writing test to prepare the data before reading test.");
            return Err("You might need start bench writing test to prepare the data before reading test");
        }
    }

    let start = Instant::now();
    let mut last_time_ms: u64 = 0;
    for id in 0 .. num_of_accounts {
        match get_balance(db, &format!("{}", id)) {
            Ok(_value) => {
                let elapsed = start.elapsed();
                let now_ms = (elapsed.as_secs() * 1_000) + (elapsed.subsec_nanos() / 1_000_000) as u64;
                println!("Last read cost: {} ms", now_ms - last_time_ms);
                last_time_ms = now_ms;
            }
            Err(e) => {
                println!("Got error {:?}", e);
                return Err("Get balance failed during bench test.");
            }
        }
    }

    let total_elapsed = start.elapsed();
    println!("Reading {} accounts with each balance {}, total test elapsed: {} ms", num_of_accounts, num_of_tokens,
             (total_elapsed.as_secs() * 1_000) + (total_elapsed.subsec_nanos() / 1_000_000) as u64);

    Ok(())
}

fn bench_test_rw(db: &mut DB, num_of_tokens: usize, num_of_accounts: usize) -> Result<(), &'static str> {
    // before write test, drop the old data.
    match db.drop_cf("default") {
        Ok(()) => println!("Old data droped."),
        Err(_e) => println!("default cf does not existed, skip to drop cf."),
    }

    match init_super_account(db, &String::from("SUPER"), 50000000000){
        Ok(()) => println!("Token id system is ready to use."),
        Err(e) => {
            println!("Got error: {:?}", e);
            return Err("Cannot init super account.");
        },
    }

    let start = Instant::now();
    let mut last_time_ms: u64 = 0;
    for id in 0 .. num_of_accounts {
        // hard code super account only for bench test.
        match transfer(db, &String::from("SUPER"), &format!("{}", id), num_of_tokens) {
            Ok(()) => {
                let elapsed = start.elapsed();
                let now_ms = (elapsed.as_secs() * 1_000) + (elapsed.subsec_nanos() / 1_000_000) as u64;
                println!("Last write cost: {} ms", now_ms - last_time_ms);
                last_time_ms = now_ms;
            }
            Err(e) => {
                println!("Got error {:?}", e);
                return Err("Transfer failed happens during bench test.");
            }
        }

        // read balance here might be faster since the buffer hit happens usually right after the data insertion.
        match get_balance(db, &format!("{}", id)) {
            Ok(_value) => {
                let elapsed = start.elapsed();
                let now_ms = (elapsed.as_secs() * 1_000) + (elapsed.subsec_nanos() / 1_000_000) as u64;
                println!("After write operation, immediately read cost: {} ms", now_ms - last_time_ms);
                last_time_ms = now_ms;
            }
            Err(e) => {
                println!("Got error {:?}", e);
                return Err("Get balance failed during bench test.");
            }
        }
    }

    let total_elapsed = start.elapsed();
    println!("Total test elapsed: {} ms",
             (total_elapsed.as_secs() * 1_000) + (total_elapsed.subsec_nanos() / 1_000_000) as u64);

    Ok(())
}
