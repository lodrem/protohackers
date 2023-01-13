use std::net::SocketAddr;

use anyhow::{anyhow, Result};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::info;

use crate::tcp;

const CIPHER_END: u8 = 0x00;
const CIPHER_REVERSE_BITS: u8 = 0x01;
const CIPHER_XOR: u8 = 0x02;
const CIPHER_XOR_POS: u8 = 0x03;
const CIPHER_ADD: u8 = 0x04;
const CIPHER_ADD_POS: u8 = 0x05;

fn reverse_bits(mut v: u8) -> u8 {
    let mut rv = 0;
    let mut p = 7;

    while v != 0 {
        rv += (v & 1) << p;
        v >>= 1;
        p -= 1;
    }

    rv
}

#[derive(Copy, Clone, Debug)]
enum Cipher {
    ReverseBits,
    Xor(u8),
    XorPos,
    Add(u8),
    AddPos,
}

impl Cipher {
    #[inline]
    pub fn encode(&self, i: usize, v: u8) -> u8 {
        match self {
            Self::ReverseBits => reverse_bits(v),
            Self::Xor(n) => v ^ n,
            Self::XorPos => v ^ (i as u8),
            Self::Add(n) => ((v as u16 + *n as u16) % 256) as u8,
            Self::AddPos => ((v as u16 + i as u16) % 256) as u8,
        }
    }

    #[inline]
    pub fn decode(&self, i: usize, v: u8) -> u8 {
        match self {
            Self::ReverseBits => reverse_bits(v),
            Self::Xor(n) => v ^ n,
            Self::XorPos => v ^ (i as u8),
            Self::Add(n) => ((v as i16 - *n as i16).rem_euclid(256)) as u8,
            Self::AddPos => ((v as i16 - i as i16).rem_euclid(256)) as u8,
        }
    }
}

#[derive(Debug, Clone)]
struct Ciphers(Vec<Cipher>);

impl Ciphers {
    pub fn new(cs: Vec<Cipher>) -> Self {
        Self(cs)
    }

    #[inline]
    pub fn encode(&self, i: usize, mut v: u8) -> u8 {
        for cipher in self.0.iter() {
            v = cipher.encode(i, v);
        }

        v
    }

    #[inline]
    pub fn decode(&self, i: usize, mut v: u8) -> u8 {
        for j in (0..self.0.len()).rev() {
            v = self.0[j].decode(i, v);
        }

        v
    }

    pub fn to_data(&self) -> Vec<u8> {
        let mut buf = vec![];
        for cipher in self.0.iter() {
            match cipher {
                Cipher::Add(n) => {
                    buf.push(CIPHER_ADD);
                    buf.push(*n);
                }
                Cipher::AddPos => buf.push(CIPHER_ADD_POS),
                Cipher::Xor(n) => {
                    buf.push(CIPHER_XOR);
                    buf.push(*n);
                }
                Cipher::XorPos => buf.push(CIPHER_XOR_POS),
                Cipher::ReverseBits => buf.push(CIPHER_REVERSE_BITS),
            }
        }
        buf.push(CIPHER_END);
        buf
    }

    pub fn is_noop(&self) -> bool {
        let buf = {
            let mut buf = [0; 255];
            for i in 0..buf.len() {
                buf[i] = i as u8;
            }
            buf
        };
        for i in 0..buf.len() {
            if self.encode(i, buf[i]) != buf[i] {
                return false;
            }
        }
        true
    }
}

struct Context<R, W> {
    reader: R,
    writer: W,
    ciphers: Ciphers,
    read_pos: usize,
    write_pos: usize,
}

impl<R, W> Context<R, W>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    pub fn new(r: R, w: W) -> Self {
        Self {
            reader: r,
            writer: w,
            ciphers: Ciphers(vec![]),
            read_pos: 0,
            write_pos: 0,
        }
    }

    pub async fn prepare_ciphers(&mut self) -> Result<()> {
        let mut rv = vec![];
        loop {
            let v = {
                // self.read_pos += 1;
                self.reader.read_u8().await?
            };
            let cipher = match v {
                CIPHER_END => break,
                CIPHER_REVERSE_BITS => Cipher::ReverseBits,
                CIPHER_XOR => Cipher::Xor({
                    // self.read_pos += 1;
                    self.reader.read_u8().await?
                }),
                CIPHER_XOR_POS => Cipher::XorPos,
                CIPHER_ADD => Cipher::Add({
                    // self.read_pos += 1;
                    self.reader.read_u8().await?
                }),
                CIPHER_ADD_POS => Cipher::AddPos,
                typ => return Err(anyhow!("Invalid cipher type: 0x{:02x}", typ)),
            };
            rv.push(cipher);
        }
        self.ciphers.0 = rv;

        info!("-> Server: ciphers {:?}", self.ciphers);

        if self.ciphers.is_noop() {
            Err(anyhow!("Ciphers {:?} is noop, disconnected", self.ciphers))
        } else {
            Ok(())
        }
    }

    pub async fn recv_line(&mut self) -> Result<String> {
        let mut original = vec![];

        let mut buf = vec![];

        loop {
            let v = self.reader.read_u8().await?;
            original.push(v);
            let v = self.ciphers.decode(self.read_pos, v);
            self.read_pos += 1;

            if v == b'\n' {
                break;
            }

            buf.push(v);
        }

        {
            let before = {
                let parts: Vec<_> = original
                    .into_iter()
                    .map(|b| format!("0x{:02x}", b))
                    .collect();
                parts.join(" ")
            };
            let after = {
                let parts: Vec<_> = buf.iter().map(|b| format!("0x{:02x}", b)).collect();
                parts.join(" ")
            };
            let after_p = unsafe { String::from_utf8_unchecked(buf.clone()) };
            info!("-> Server: before '{}'", before);
            info!("-> Server: after  '{}' == '{}'", after, after_p);
        }

        Ok(unsafe { String::from_utf8_unchecked(buf) })
    }

    pub async fn send_line(&mut self, v: String) -> Result<()> {
        let p = v.clone();

        let mut buf = v.into_bytes();
        buf.push(b'\n');

        for i in 0..buf.len() {
            buf[i] = self.ciphers.encode(self.write_pos, buf[i]);
            self.write_pos += 1;
        }

        {
            let before = {
                let parts: Vec<_> = p
                    .as_bytes()
                    .iter()
                    .map(|b| format!("0x{:02x}", b))
                    .collect();
                parts.join(" ")
            };
            let after = {
                let parts: Vec<_> = buf.iter().map(|b| format!("0x{:02x}", b)).collect();
                parts.join(" ")
            };
            info!("<- Server: before '{}' == '{}'", before, p);
            info!("<- Server: after  '{}'", after);
        }

        self.writer.write_all(&buf).await?;
        Ok(())
    }
}

async fn handle(mut socket: TcpStream, _remote_addr: SocketAddr) -> Result<()> {
    let mut ctx = {
        let (rh, wh) = socket.split();
        Context::new(rh, wh)
    };

    ctx.prepare_ciphers().await?;

    loop {
        let req = ctx.recv_line().await?;
        let (n, toy) = req
            .split(',')
            .map(|s| {
                let (n, toy) = s.split_once("x ").expect("valid request");
                (n.parse::<u64>().expect("valid number"), toy.to_string())
            })
            .max_by_key(|(n, _)| *n)
            .expect("at least 1 toy");

        ctx.send_line(format!("{}x {}", n, toy)).await?;
    }
}

pub async fn run(addr: SocketAddr) -> Result<()> {
    tcp::serve(addr, handle).await
}

#[cfg(test)]
mod tests {
    use super::Cipher;
    use crate::insecure_sockets_layer::{
        Ciphers, CIPHER_ADD_POS, CIPHER_END, CIPHER_REVERSE_BITS, CIPHER_XOR, CIPHER_XOR_POS,
    };
    use bytes::BufMut;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpSocket, TcpStream};
    use tokio::time::sleep;
    use tracing::{info, Level};

    #[test]
    fn test_reverse_bits() {
        use super::reverse_bits;
        assert_eq!((42 + 230) % 256, 16);
        assert_eq!((16 as i32 - 230).rem_euclid(256), 42);
        assert_eq!((42 + 50) % 256, 92);
        assert_eq!((92 as i32 - 50).rem_euclid(256), 42);

        assert_eq!(reverse_bits(0x69), 0x96);
        assert_eq!(reverse_bits(0x64), 0x26);
        assert_eq!(reverse_bits(0x6d), 0xb6);
        assert_eq!(reverse_bits(0x6e), 0x76);

        for i in 0..255 {
            assert_eq!(i, reverse_bits(reverse_bits(i)));
        }

        let add_pos = Cipher::AddPos;
        assert_eq!(add_pos.encode(1, 65), 66);
        assert_eq!(add_pos.encode(1, 66), 67);
        assert_eq!(add_pos.decode(1, 67), 66);
        assert_eq!(add_pos.decode(1, 66), 65);
    }

    #[tokio::test]
    async fn test() {
        use super::run;
        tracing_subscriber::fmt().with_max_level(Level::INFO).init();

        let addr = "0.0.0.0:8888".parse().unwrap();

        tokio::spawn(run(addr));

        sleep(Duration::from_secs(3)).await;

        let mut cli = TcpStream::connect(addr).await.unwrap();

        let ciphers = Ciphers::new(vec![Cipher::Xor(123), Cipher::AddPos, Cipher::ReverseBits]);
        let requests: Vec<_> = b"4x dog,5x car\n3x rat,2x cat\n"
            .into_iter()
            .enumerate()
            .map(|(i, b)| ciphers.encode(i, *b))
            .collect();

        let mut data = vec![];

        data.extend(ciphers.to_data());
        data.put_slice(&requests);

        cli.write_all(&data).await.unwrap();

        let mut i = 0;
        let mut resp = Vec::new();
        while let Ok(v) = cli.read_u8().await {
            let decoded = ciphers.decode(i, v);
            resp.push(decoded);
            if decoded == b'\n' {
                info!("recv resp: {}", unsafe {
                    String::from_utf8_unchecked(resp.clone())
                });
                resp.clear();
            }
            i += 1;
        }

        sleep(Duration::from_secs(10)).await;
    }
}
