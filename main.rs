extern crate exonum_rocksdb;
extern crate tempdir;
extern crate bincode;

extern crate token_id_poc;

use std::env;
use std::time::Instant;
use exonum_rocksdb::{DB, Options, MergeOperands};
use token_id_poc::bootstrap::init_super_account;
use token_id_poc::account::{get_balance, transfer};
use token_id_poc::bench::bench_test;

// adding tokens into account by using merge operation for a better performance.
fn balance_array_merge_handler(_: &[u8], existing_val: Option<&[u8]>, operands: &mut MergeOperands)-> Vec<u8> {
    //to do estimate and reserve heap for vec for better perf.
    let mut result: Vec<u8> = Vec::with_capacity(operands.size_hint().0);

    //if account does not existed, just serialized 64bits to hold the lengh of its token array.
    if existing_val.is_none() {
        let length: usize = 0;
        let mut bytes = bincode::serde::serialize(&length, bincode::SizeLimit::Infinite).unwrap();
        result.append(&mut bytes);
    } else {
        result.extend_from_slice(existing_val.unwrap());
    }

    let mut balance_increased: usize = 0;

    //calculate total increasement from the merge operation list, merge arrays by per merge operation.
    for op in operands {
       //to do better for hard coded length.
       let mut patch_balance: usize = bincode::serde::deserialize(&op[0..8]).unwrap();
       balance_increased += patch_balance;
       result.extend_from_slice(&op[8..]);
    }

    //increate balance array with balance_increased.
    let mut balance: usize = bincode::serde::deserialize(&result[0..8]).unwrap();
    balance += balance_increased;
    let bytes = bincode::serde::serialize(&balance, bincode::SizeLimit::Infinite).unwrap();
    result.splice(..8, bytes.iter().cloned());
    result
}

fn get_db(dir: &str) -> DB {

    let mut opts = Options::default();
    //let mut block_opts = BlockBasedOptions::default();
    //double the block cach, 4096 as default.
    //block_opts.set_block_size(4096);
    //8MB as default, ext to 512 MB.
    //block_opts.set_lru_cache(512*1024*1024);
    //opts.set_compression_type(DBCompressionType::Lz4);

    opts.increase_parallelism(8);
    opts.set_max_background_flushes(6);
    opts.set_max_background_compactions(6);
    //opts.set_block_based_table_factory(&block_opts);
    opts.create_if_missing(true);

    //default write buffer for cf is 64MB, ext it to 128MB;
    opts.set_write_buffer_size(128 * 1024 * 1024);
    opts.set_max_write_buffer_number(6);
    //opts.set_disable_auto_compactions(true);
    opts.set_bytes_per_sync(2*1024*1024);

    opts.set_merge_operator("balance array merge handler", balance_array_merge_handler);

    //to do better for configuable dir for db storage.
    let db = DB::open(&opts, dir).unwrap();
    db
}

fn help() {
    println!(
        "Usage Example:
    $ Init token id system by executing:
    $ > cargo run init-super [your_super_account_id] [total_supply]
    $ Transfer tokens from one to another accounts by executing:
    $ > cargo run transfer [from] [to] [amount]
    $ Get balance of an anccount by executing:
    $ > cargo run balance [address]
    $ Start bench mark by rolling out amount of tokens to amount of accounts by executing:
    $ > cargo run bench [I/O] [amount of account] [amount of tokens per account]
    $ Note: for better performance, please build binary in release mode.
where:
    [your_super_account_id]\t\t Hash id of your account adrress, can be anything in string.
    [total_supply]\t\t\t The total supply of your tokens, for example: 50000000000
    [from]\t\t\t\t Sender's address, for example super account's address.
    [to]\t\t\t\t Receiver's address, for any end user's account address.
    [amount]\t\t\t\t Number of tokens need to be transfered in the transaction.
    [address]\t\t\t\t Account address in string.
    [I/O]\t\t\t\t I | O | IO, command to write | read | write & read balance during the bench test."
    );
}


fn main() {
    let args: Vec<String> = env::args().collect();

    let start = Instant::now();
    let db = get_db("./token_storage");
    let mut bench_db = get_db("./bench_token_storage");
    
    let elapsed = start.elapsed();
    println!("On starup, get db loaded. It cost: {} ms",
             (elapsed.as_secs() * 1_000) + (elapsed.subsec_nanos() / 1_000_000) as u64);

    match args.len() {
        1 => {
            // no args
            help();
        }
        2 => {
            // one arg passed
            help();
        }
        3 => {
            // two args passed
            // to do get balance
            let command = &args[1];
            let address = &args[2];

            match &command[..] {
                "balance" => {
                    let start = Instant::now();
                    match get_balance(&db, &address) {
                        Ok(value) => {
                            println!("{} have {} tokens", address, value);
                            let elapsed = start.elapsed();
                            println!("get_balance cost: {} ms",
                                    (elapsed.as_secs() * 1_000) + (elapsed.subsec_nanos() / 1_000_000) as u64);
                        },
                        Err(e) => {
                            println!("Got error: {:?}", e);
                            return;
                        },
                    }
                }
                _ => {
                    eprintln!("error: invalid command.");
                    help();
                }
            }
        }
        4 => {
            //three args passed
            //to do the init super account.
            let command = &args[1];
            let super_account_id = &args[2];
            let total = &args[3];
            let total_supply: usize = match total.parse() {
	            Ok(n) => n,
                Err(_) => {
                    eprintln!("error: <total supply> must be an number");
                    help();
                    return;
                }
            };

            match &command[..] {
                "init-super" => {
                    let start = Instant::now();
                    match init_super_account(&db, &super_account_id, total_supply){
                        Ok(()) => {
                            println!("Token id system is ready to use.");
                            let elapsed = start.elapsed();
                            println!("init_super_account cost: {} ms",
                                    (elapsed.as_secs() * 1_000) + (elapsed.subsec_nanos() / 1_000_000) as u64);
                        },
                        Err(e) => {
                            println!("Got error: {:?}", e);
                            return;
                        },
                    }
                }
                _ => {
                    eprintln!("error: invalid command.");
                    help();
                }
            }
        }
        5 => {
            //four args passed
            let command = &args[1];
            match &command[..] {
                //to do the transfer.
                "transfer" => {
                    let from = &args[2];
                    let to = &args[3];
                    let amount = &args[4];
                    let num_of_tokens: usize = match amount.parse() {
                        Ok(n) => n,
                        Err(_) => {
                            eprintln!("error: <amount> must be an number");
                            help();
                            return;
                        }
                    };
                    let start = Instant::now();              
                    match transfer(&db, &from, &to, num_of_tokens) {
                        Ok(()) => {
                            println!("Transfer done!");
                            let elapsed = start.elapsed();
                            println!("Transfer cost: {} ms",
                                    (elapsed.as_secs() * 1_000) + (elapsed.subsec_nanos() / 1_000_000) as u64);
                        },
                        Err(e) => {
                            println!("Got error: {:?}", e);
                            return;
                        },
                    }
                }
                //to do the bench mark test.
                "bench" => {
                    let io = &args[2];
                    let token_amount = &args[3];
                    let account_amount = &args[4];
                    let num_of_tokens: usize = match token_amount.parse() {
                        Ok(n) => n,
                        Err(_) => {
                            eprintln!("error: <amount> must be an number");
                            help();
                            return;
                        }
                    };
                    let num_of_accounts: usize = match account_amount.parse() {
                        Ok(n) => n,
                        Err(_) => {
                            eprintln!("error: <amount> must be an number");
                            help();
                            return;
                        }
                    };

                    match bench_test(&mut bench_db, &io, num_of_tokens, num_of_accounts) {
                        Ok(()) => println!("Bench test done!"),
                        Err(e) => {
                            println!("Got error: {:?}", e);
                            return;
                        },
                    }
                }
                _ => {
                    eprintln!("error: invalid command.");
                    help();
                }
            }
        }
        _ => {
            // all the other cases
            help();
        }
    }
}
