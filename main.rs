use rand::Rng;
use std::collections::HashMap;
use std::collections::HashSet;
use nohash_hasher;
use nohash_hasher::IntSet;
use std::error::Error;
use csv;
use serde::{Serialize, Deserialize};
use std::process;
use std::convert::TryFrom;
use std::time::{Duration, SystemTime};
use std::fs::File;
use std::io::prelude::*;

static mut banks: Vec<bank> = Vec::new();


#[derive(Debug, Deserialize, PartialEq)]
struct Record{
    TX_ID:u64,
    SENDER_ACCOUNT_ID:u32,
    RECEIVER_ACCOUNT_ID:u32,
    TX_TYPE:String,
    TX_AMOUNT:f64,
    TIMESTAMP:u64,
    IS_FRAUD:String,
    ALERT_ID:i64,
}
pub struct bank{
    id: u32,
    accounts:HashMap<u32, account, nohash_hasher::BuildNoHashHasher<u32>>,
    // accounts:HashMap<u32, account>,
    cycleIds:HashSet<u128>,
    key:u128,
}
impl bank {
    pub fn detectCycle(&mut self,id: u32) -> Result<(u128), Box<dyn Error>>{
        let mut rng = rand::thread_rng();
        let cycleId: u128 = rng.gen();
        let mut ret:u128 = 0;

        self.cycleIds.insert(cycleId);

        for transaction in self.accounts[&id].outGoingTransactions.iter(){
            let newCycleId = cycleId^transaction.txHash;
            unsafe{
                ret = ret + &banks[usize::try_from(transaction.receiverBank)?].continueCycle(transaction.receiver, newCycleId,9).unwrap();
            }
        }
        self.cycleIds.remove(&cycleId);
        Ok(ret)
    }
    pub fn continueCycle(&mut self,id:u32,cycleId:u128,counter:u8) -> Result<(u128), Box<dyn Error>> {


        if counter == (0 as u8) || self.cycleIds.contains(&cycleId){
            if self.cycleIds.contains(&cycleId){
                // println!("Cycle");
            }
            return Ok(256);
        }

        let mut ret:u128 = 0;
        for transaction in self.accounts[&id].outGoingTransactions.iter(){
            let newCycleId = cycleId^transaction.txHash;
            let newCounter = counter -1;
            unsafe{
                ret = ret + &banks[usize::try_from(transaction.receiverBank)?].continueCycle(transaction.receiver, newCycleId,newCounter).unwrap();
            }
        }
        Ok(ret)
    }
}

pub struct account<>{
    id: u32,
    bank: u32,
    outGoingTransactions:Vec<transaction>,
    outgoingHash:IntSet<u32>,
}
pub struct transaction<>{
    sender:u32,
    senderBank:u32,
    receiver:u32,
    receiverBank:u32,
    txHash:u128,
}



fn run(path:&str) -> Result<(),Box<dyn Error>>{
    let accountsize:u32 = 1000000;
    let mut rng = rand::thread_rng();
    let mut accounts: Vec<account> = Vec::with_capacity(usize::try_from(accountsize)?);
    unsafe{
        banks.push(bank{id:0,accounts:HashMap::default(),cycleIds:HashSet::default(),key:rng.gen()});
        banks.push(bank{id:1,accounts:HashMap::default(),cycleIds:HashSet::default(),key:rng.gen()});
        banks.push(bank{id:2,accounts:HashMap::default(),cycleIds:HashSet::default(),key:rng.gen()});
    }

    let mut reader = csv::Reader::from_path(path)?;
    for i in 0..accountsize{
        let account = account{
            id:i,
            bank:i%3,
            outGoingTransactions:Vec::new(),
            outgoingHash:IntSet::default(),
        };
        accounts.push(account);
    }

    for (i, result) in reader.deserialize::<Record>().enumerate(){
        let record: Record = result?;
        if !accounts[usize::try_from(record.SENDER_ACCOUNT_ID)?].outgoingHash.contains(&record.RECEIVER_ACCOUNT_ID){
            let newTransaction = transaction{
            sender:record.SENDER_ACCOUNT_ID,
            senderBank:record.SENDER_ACCOUNT_ID%3,
            receiver:record.RECEIVER_ACCOUNT_ID,
            receiverBank:record.RECEIVER_ACCOUNT_ID%3,
            txHash:rng.gen(),
            };
            accounts[usize::try_from(record.SENDER_ACCOUNT_ID)?].outGoingTransactions.push(newTransaction);
            accounts[usize::try_from(record.SENDER_ACCOUNT_ID)?].outgoingHash.insert(record.RECEIVER_ACCOUNT_ID);
        }

        if i%1000000 == 0{
            println!("{}",i);
        }
    }
    for (i,account) in accounts.into_iter().enumerate(){
        unsafe{
            banks[i%3].accounts.insert(account.id, account);
        }
        if i%10000 == 0{
            println!("Accounts added to banks: {}",i);
        }
    }



    unsafe {
        for i in 0..accountsize{

            banks[usize::try_from(i%3)?].detectCycle(i).unwrap();

        }

    }

    Ok(())
}
fn main() {
    if let Err(err) = run("transactions.csv") {
        println!("{}", err);
        process::exit(1);
    }
}
