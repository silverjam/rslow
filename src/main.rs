#![allow(unused_variables)]
#![allow(unused_imports)]

#[macro_use]
extern crate log;

extern crate env_logger;

use std::io::prelude::*;

use std::io::{self, Read, Write, Error};
use std::net::TcpListener;
use std::{thread, time};
use std::net::{Shutdown, TcpStream};

use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::collections::VecDeque;

use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};

struct Buffer {
    buffer: Box<[u8;4096]>,
    length: usize,
    start: usize,
}

fn main() {

    env_logger::init().unwrap();

    let listener = TcpListener::bind("127.0.0.1:9123").unwrap();
    info!("listening started, ready to accept");

    let host = "www.example.com";
    let port = 80;

    for client_stream in listener.incoming() {
        thread::spawn(move || {

            let mut client_stream = client_stream.unwrap();

            let upstream_stream = TcpStream::connect((host, port)).unwrap_or_else(|error| {
                panic!(error.to_string());
            });

            let mut upstream_stream = upstream_stream.try_clone().unwrap();

            let queue : VecDeque<Buffer> = VecDeque::new();
            let queue_lock = Arc::new(Mutex::new(queue));

            let rate = 10000;

            let token_bucket = Arc::new(AtomicUsize::new(0));
            let token_threads_quit = Arc::new(AtomicBool::new(false));

            let sleep_ms : usize = 100;
            let tokens_per_sleep = rate / sleep_ms;

            let token_bucket_feed = token_bucket.clone();
            let feed_thread_sleep = time::Duration::from_millis(sleep_ms as u64);

            let consume_thread_sleep = time::Duration::from_millis((sleep_ms/10) as u64);

            {
                let token_bucket = token_bucket.clone();
                let token_threads_quit = token_threads_quit.clone();

                let func = move || {
                    loop {
                        if token_threads_quit.load(Ordering::SeqCst) {
                            info!("Token producer thread quitting...");
                            return;
                        }

                        thread::sleep(feed_thread_sleep);

                        let tokens = token_bucket.fetch_add(0, Ordering::SeqCst);

                        if tokens >= tokens_per_sleep*2 {
                            continue;
                        }

                        token_bucket.fetch_add(tokens_per_sleep, Ordering::SeqCst);
                    }
                };

                thread::spawn(func);
            }


            {
                let token_bucket = token_bucket.clone();
                let queue_lock = queue_lock.clone();
                let token_threads_quit = token_threads_quit.clone();

                let mut client_stream = client_stream.try_clone().unwrap();

                let thread_func = move || {

                    loop {

                        if token_threads_quit.load(Ordering::SeqCst) {
                            info!("Token consumer thread exiting...");
                            return;
                        }

                        thread::sleep(consume_thread_sleep);

                        let tokens = token_bucket.fetch_add(0, Ordering::SeqCst);
                        let sleep_ms : usize = 50;

                        if tokens > 0 {

                            match queue_lock.try_lock() {
                                Ok(mut queue) => {

                                    if queue.len() == 0 {
                                        continue;
                                    }

                                    let mut bufobj = queue.pop_front().unwrap();

                                    let slice_len = if tokens < bufobj.length
                                        { tokens } else { bufobj.length };

                                    {
                                        let slice = &bufobj.buffer[bufobj.start..(bufobj.start+slice_len)];

                                        client_stream.write_all(slice).unwrap_or_else(|error| {
                                            warn!("write_all: {}", error); 
                                            token_threads_quit.store(true, Ordering::SeqCst);
                                        });
                                    }

                                    if slice_len != bufobj.length {

                                        bufobj.length -= slice_len;
                                        bufobj.start += slice_len;

                                        queue.push_back(bufobj);
                                    }

                                    let sub_tokens = token_bucket.fetch_sub(slice_len, Ordering::SeqCst);
                                }

                                Err(error) => {
                                    continue;
                                }
                            }
                        }
                    }
                };

                thread::spawn(thread_func);
            }

            {
                let mut upstream_stream = upstream_stream.try_clone().unwrap();
                let queue_lock = queue_lock.clone();
                let client_stream = client_stream.try_clone().unwrap();

                let thread_func = move || {
                    
                    let mut buf_in = [0u8; 4096];

                    loop {
                        match upstream_stream.read(&mut buf_in) {
     
                            Ok(length) => {

                                if length == 0 {

                                    info!("Zero read (upstream), closing...");

                                    client_stream.shutdown(Shutdown::Both)
                                        .unwrap_or_else(|error| {
                                            warn!("shutdown error: {}", error);
                                        });

                                    return;

                                } else {

                                    let mut buffer = Box::new([0u8; 4096]);
                                    buffer.copy_from_slice(&buf_in);

                                    let buffer_obj = Buffer{buffer, length, start: 0};
                                    let mut queue = queue_lock.lock().unwrap();

                                    queue.push_back(buffer_obj);
                                }

                            }

                            Err(error) => {
                                warn!("read error: {}", error.to_string());
                                return;
                            },
                        }
                    }
                };

                thread::spawn(thread_func);
            }

            {
                let token_threads_quit = token_threads_quit.clone();

                let thread_func = move || {

                    let mut client_buffer = [0u8; 4096];
                    let mut exit = false;

                    loop {
                        if exit { return }

                        match client_stream.read(&mut client_buffer) {

                            Ok(n) => {

                                if n == 0 {
                                    info!("Zero read (client), closing...");

                                    upstream_stream.shutdown(Shutdown::Both)
                                    .unwrap_or_else(|error| {
                                        warn!("shutdown error: {}", error);
                                    });

                                    token_threads_quit.store(true, Ordering::SeqCst);

                                    return;

                                } else {
                                    let slice = &client_buffer[..n];

                                    upstream_stream.write_all(slice).unwrap_or_else(|error| {
                                        warn!("write failed: {}", error);
                                        exit = true;
                                    });
                                }
                            }

                            Err(error) => {
                                warn!("client read error: {}", error);
                            }
                        }
                    }
                };

                thread::spawn(thread_func);
            }
        });
    }
}
