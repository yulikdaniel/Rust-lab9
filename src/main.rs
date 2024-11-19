use std::cmp::max;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead};
use std::sync::mpsc::channel;
use std::thread;
use std::time::{Duration, Instant};
use clap::{arg, command, value_parser, ArgAction, Command};

fn load_file(name: &str) -> Result<Vec<String>, io::Error> {
    io::BufReader::new(File::open(name)?).lines().collect()
}

fn count_chars<S: AsRef<str>>(input: &[S]) -> HashMap<char, usize> {
    let mut counter = HashMap::<char, usize>::new();
    for text in input {
        for c in text.as_ref().chars() {
            *counter.entry(c).or_default() += 1;
        }
    }
    counter
}

fn count_chars_parallel<S: AsRef<str> + Sync>(input: &[S], n: usize) -> HashMap<char, usize> {
    let (sender, receiver) = channel();
    let BLCKSZ = (input.len() + n - 1) / n;
    let mut counter = HashMap::<char, usize>::new();
    thread::scope(|s| {
        for chunk in input.chunks(BLCKSZ) {
            let sender = sender.clone();
            s.spawn(move || {
                let counter = count_chars(chunk);
                sender.send(counter).unwrap();
            });
        }
    });
    std::mem::drop(sender);
    while let Ok(counter_part) = receiver.recv() {
        for (key, value) in counter_part.iter() {
            *counter.entry(*key).or_default() += value;
        }
    }
    counter
}

fn benchmark<S: AsRef<str> + Sync>(input: &[S], n: usize, reruns: u32) -> (Duration, HashMap<char, usize>) {
    let start = Instant::now();
    let mut counter = None;
    for _ in 0..reruns {   // Here reruns is a u32
        counter = Some(count_chars_parallel(input, n));
    }
    (Instant::elapsed(&start) / reruns, counter.unwrap())
}

fn benchmark_all<S: AsRef<str> + Sync>(input: &[S], max: usize, reruns: u32) -> HashMap<char, usize> {
    let mut counter = None;
    for par_level in 1..max+1 {
        let (time, counter1) = benchmark(input, par_level, reruns);
        counter = Some(counter1);
        println!("Average time with {par_level} threads: {:?}", time);
    }
    counter.unwrap()
}

fn main() -> Result<(), io::Error> {
    let matches = command!() // requires `cargo` feature
    .arg(arg!(<FILE> "File to operate on"))
    .arg(arg!(-m --max <MAX> "Maximum number of threads to benchmark").required(false).value_parser(value_parser!(usize)).default_value("8"))
    .arg(arg!(-r --reruns <RERUNS>  "The number of reruns to run each test").value_parser(value_parser!(u32)).default_value("100"))
    .arg(arg!(-s --stats <rank>   "Display statistics").value_parser(value_parser!(usize)))
    .get_matches();

    let mut max_threads = *matches.get_one::<usize>("max").unwrap();
    if max_threads == 0 {
        println!("Max thread argument is equal to zero, setting to 1.");
        max_threads = 1;
    }
    let mut reruns = *matches.get_one::<u32>("reruns").unwrap();
    if reruns == 0 {
        println!("Reruns argument is equal to zero, setting to 1.");
        reruns = 1;
    }

    let lines = load_file(matches.get_one::<String>("FILE").unwrap())?;

    let stats = benchmark_all(lines.as_slice(), max_threads, reruns);

    if let Some(rank) = matches.get_one::<usize>("stats") {
        if *rank == 0 {
            println!("Stats argument is used, but rank set to 0. Setting to 1.");
        }
        let mut freq : Vec<_> = stats.into_iter().collect();
        freq.sort_unstable_by_key(|&(_, n)| n);
        println!("Most frequent characters:");
        for (c, n) in freq.into_iter().rev().take(max(*rank, 1)) {
            println!(" - '{c}': {n} occurrences");
        }
    }
    Ok(())
}