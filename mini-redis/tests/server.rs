use std::{net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time,
};

use mini_redis::server;

mod integration_tests {
    use super::*;

    #[tokio::test]
    async fn key_value_get_set() {
        let (addr, _) = start_server().await;
        let mut stream = TcpStream::connect(addr).await.unwrap();

        // Get a key, data is missing
        stream
            .write_all(b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n")
            .await
            .unwrap();

        // Read empty response
        let mut response = [0; 5];
        stream.read_exact(&mut response).await.unwrap();
        assert_eq!(b"$-1\r\n", &response);

        // Set a key
        stream
            .write_all(b"*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
            .await
            .unwrap();

        let mut response = [0; 5];
        stream.read_exact(&mut response).await.unwrap();
        assert_eq!(b"+OK\r\n", &response);

        // Get the key, data is present
        stream
            .write_all(b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n")
            .await
            .unwrap();

        let mut response = [0; 11];
        stream.read_exact(&mut response).await.unwrap();
        assert_eq!(b"$5\r\nworld\r\n", &response);

        // Shutdown the write half
        stream.shutdown().await.unwrap();

        // Receives `None`
        assert_eq!(0, stream.read(&mut response).await.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn key_value_timeout() {
        let (addr, _) = start_server().await;
        let mut stream = TcpStream::connect(addr).await.unwrap();

        // Set a key
        stream
            .write_all(b"*5\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n+EX\r\n:1\r\n")
            .await
            .unwrap();

        let mut response = [0; 5];

        // Read OK
        stream.read_exact(&mut response).await.unwrap();
        assert_eq!(b"+OK\r\n", &response);

        // Get the key, data is present
        stream
            .write_all(b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n")
            .await
            .unwrap();

        let mut response = [0; 11];
        stream.read_exact(&mut response).await.unwrap();
        assert_eq!(b"$5\r\nworld\r\n", &response);

        // wait for the key to expire
        time::advance(Duration::from_secs(1)).await;

        // Get the key, data is missing
        stream
            .write_all(b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n")
            .await
            .unwrap();

        let mut response = [0; 5];
        stream.read_exact(&mut response).await.unwrap();
        assert_eq!(b"$-1\r\n", &response);
    }

    #[tokio::test]
    async fn pub_sub() {
        let (addr, _) = start_server().await;
        let mut publisher = TcpStream::connect(addr).await.unwrap();

        // Publish a message, there are no subscribers yet so the server will return `0`
        publisher
            .write_all(b"*3\r\n$7\r\nPUBLISH\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
            .await
            .unwrap();

        let mut response = [0; 4];
        publisher.read_exact(&mut response).await.unwrap();
        assert_eq!(b":0\r\n", &response);

        // Create a subscriber. This subscriber will only subscribe to the `hello` channel
        let mut sub1 = TcpStream::connect(addr).await.unwrap();
        sub1.write_all(b"*2\r\n$9\r\nSUBSCRIBE\r\n$5\r\nhello\r\n")
            .await
            .unwrap();

        let mut response = [0; 34];
        sub1.read_exact(&mut response).await.unwrap();
        assert_eq!(
            &b"*3\r\n$9\r\nsubscribe\r\n$5\r\nhello\r\n:1\r\n"[..],
            &response[..],
        );

        // Publish a message, there now is a subscriber
        publisher
            .write_all(b"*3\r\n$7\r\nPUBLISH\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
            .await
            .unwrap();

        let mut response = [0; 4];
        publisher.read_exact(&mut response).await.unwrap();
        assert_eq!(b":1\r\n", &response);

        // The first subscriber received the message
        let mut response = [0; 39];
        sub1.read_exact(&mut response).await.unwrap();
        assert_eq!(
            &b"*3\r\n$7\r\nmessage\r\n$5\r\nhello\r\n$5\r\nworld\r\n"[..],
            &response[..]
        );

        // Create another subscriber. This subscriber will subscribe to `hello` and `foo` channel
        let mut sub2 = TcpStream::connect(addr).await.unwrap();
        sub2.write_all(b"*3\r\n$9\r\nSUBSCRIBE\r\n$5\r\nhello\r\n$3\r\nfoo\r\n")
            .await
            .unwrap();

        // Read the subscribe response
        let mut response = [0; 34];
        sub2.read_exact(&mut response).await.unwrap();
        assert_eq!(
            &b"*3\r\n$9\r\nsubscribe\r\n$5\r\nhello\r\n:1\r\n"[..],
            &response[..]
        );
        let mut response = [0; 32];
        sub2.read_exact(&mut response).await.unwrap();
        assert_eq!(
            &b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:2\r\n"[..],
            &response[..]
        );

        // Publish another message on `hello`, there are two subscribers
        publisher
            .write_all(b"*3\r\n$7\r\nPUBLISH\r\n$5\r\nhello\r\n$5\r\njazzy\r\n")
            .await
            .unwrap();
        let mut response = [0; 4];
        publisher.read_exact(&mut response).await.unwrap();
        assert_eq!(b":2\r\n", &response);

        // Publish a message on `foo`, there is only one subscriber
        publisher
            .write_all(b"*3\r\n$7\r\nPUBLISH\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
            .await
            .unwrap();

        let mut response = [0; 4];
        publisher.read_exact(&mut response).await.unwrap();
        assert_eq!(b":1\r\n", &response);

        // The first subscriber received the message
        let mut response = [0; 39];
        sub1.read_exact(&mut response).await.unwrap();
        assert_eq!(
            &b"*3\r\n$7\r\nmessage\r\n$5\r\nhello\r\n$5\r\njazzy\r\n"[..],
            &response[..]
        );

        // The second subscriber received the message
        let mut response = [0; 39];
        sub2.read_exact(&mut response).await.unwrap();
        assert_eq!(
            &b"*3\r\n$7\r\nmessage\r\n$5\r\nhello\r\n$5\r\njazzy\r\n"[..],
            &response[..]
        );

        // The first subscriber did not receive the second message
        let mut response = [0; 1];
        time::timeout(Duration::from_millis(100), sub1.read(&mut response))
            .await
            .unwrap_err();

        // The 2nd subscriber did receive the second message
        let mut response = [0; 35];
        sub2.read_exact(&mut response).await.unwrap();
        assert_eq!(
            &b"*3\r\n$7\r\nmessage\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"[..],
            &response[..],
        );
    }

    #[tokio::test]
    async fn send_error_unknown_command() {
        let (addr, _) = start_server().await;
        let mut stream = TcpStream::connect(addr).await.unwrap();

        // Get a key, data is missing
        stream
            .write_all(b"*2\r\n$7\r\nUNKNOWN\r\n$5\r\nhello\r\n")
            .await
            .unwrap();

        let mut response = [0; 32];
        stream.read_exact(&mut response).await.unwrap();

        assert_eq!(b"-ERR unknown command \'unknown\'\r\n", &response);
    }

    async fn start_server() -> (SocketAddr, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = tokio::spawn(async move {
            server::run(listener, tokio::signal::ctrl_c()).await;
        });

        (addr, handle)
    }
}
