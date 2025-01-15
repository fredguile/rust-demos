use std::sync::{Arc, Condvar, Mutex};
use std::thread;

fn main() {
    let outer = Arc::new((Mutex::new(0), Condvar::new()));
    let inner = outer.clone();

    thread::spawn(move || {
        let (lock, cvar) = &*inner;

        let mut guard = lock.lock().unwrap();
        *guard = 42;
        println!("Inner guard: {}", *guard);
        cvar.notify_one();
    });

    let (lock, cvar) = &*outer;
    let mut guard = lock.lock().unwrap();
    println!("Outer before wait guard: {}", *guard);
    while *guard == 0 {
        guard = cvar.wait(guard).unwrap();
    }
    println!("Outer after wait guard: {}", *guard);
}
