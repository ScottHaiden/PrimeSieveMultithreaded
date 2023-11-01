use std::io::BufWriter;
use std::io::Write;
use std::sync::Arc;
use std::sync::mpsc;

use threadpool::ThreadPool;

fn is_prime(i:u32, primes: &Vec<u32>) -> bool {
    let limit = (i as f64).sqrt() as u32;
    return primes.iter()
                 .take_while(|&&prime| prime <= limit)
                 .all(|&prime| i % prime != 0);
}

fn find_prime_vec(max_prime: u32) -> Vec<u32> {
    let mut ret = Vec::new();

    for i in (3..max_prime).step_by(2) {
        if !is_prime(i, &ret) { continue; }
        ret.push(i)
    }

    return ret;
}

fn check_block(sender: mpsc::Sender<u32>,
               primes: Arc<Vec<u32>>,
               cands: Box<dyn Iterator<Item = u32>>) {
    cands.filter(|&cand| is_prime(cand, &primes))
         .for_each(|prime| sender.send(prime).expect("sending failed"));
}

fn main() {
    let (sender, receiver) = mpsc::channel::<u32>();

    // Find out the highest prime we need to find in a single thread, in order to not miss any in
    // the multi threaded scenario.
    let max = u32::MAX;
    let max_track = (max as f64).sqrt() as u32;

    eprintln!("Starting printing thread...");
    let printer = std::thread::Builder::new().name("collector".into()).spawn(move || {
        eprintln!("Collecting primes...");
        let mut all_primes = receiver.iter().collect::<Vec<u32>>();
        eprintln!("Got {} primes; sorting...", all_primes.len());
        all_primes.sort();

        eprintln!("Sorted!");
        let mut out = BufWriter::new(std::io::stdout());
        all_primes.into_iter()
                  .map(|prime| prime.to_be_bytes())
                  .for_each(|bytes| out.write_all(&bytes).expect("write failed"));
    }).unwrap();

    eprintln!("Finding seed primes...");
    let primes = Arc::new(find_prime_vec(max_track));

    eprintln!("Enqueueing tasks...");
    let threadpool = ThreadPool::with_name("search_threads".into(),
                                           std::thread::available_parallelism().unwrap().get());
    for i in 1.. {
        // Point returns a multiple of the square root of the maximum number we're checking to, if
        // it's less than max, otherwise max itself.
        let point = |i: u32| {
            return match max_track.checked_mul(i) {
                Some(n) => n,
                None => u32::MAX,
            };
        };
        // Set 'low' to the next odd number greater than the current multiple of the square root of
        // the maximum number we're checking to.
        let low = match point(i).checked_add(1) {
            Some(n) => n | 1,
            None => point(i),
        };
        let hi = point(i + 1);
        let interval = Box::new((low..hi).step_by(2));

        let primes = primes.clone();
        let sender = sender.clone();
        threadpool.execute(move || { check_block(sender, primes, interval); });
        if hi == max { break; }
    }
    // We put the primes in *after* we've started all the tasks so that we don't block on sending
    // all the low primes first; this is fine, since the output thread needs to sort the list
    // anyway.
    (move || primes.iter().for_each(|&prime| sender.send(prime).expect("send failed")))();

    eprintln!("Awaiting tasks...");
    (move || threadpool.join())();
    printer.join().expect("Failed to join printer");
    eprintln!("Done!");
}
