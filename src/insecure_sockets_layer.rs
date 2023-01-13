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

struct Context<R, W> {
    reader: R,
    writer: W,
    ciphers: Vec<Cipher>,
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
            ciphers: vec![],
            read_pos: 0,
            write_pos: 0,
        }
    }

    pub async fn prepare_ciphers(&mut self) -> Result<()> {
        let mut rv = vec![];
        loop {
            let v = {
                self.read_pos += 1;
                self.reader.read_u8().await?
            };
            let cipher = match v {
                CIPHER_END => break,
                CIPHER_REVERSE_BITS => Cipher::ReverseBits,
                CIPHER_XOR => Cipher::Xor({
                    self.read_pos += 1;
                    self.reader.read_u8().await?
                }),
                CIPHER_XOR_POS => Cipher::XorPos,
                CIPHER_ADD => Cipher::Add({
                    self.read_pos += 1;
                    self.reader.read_u8().await?
                }),
                CIPHER_ADD_POS => Cipher::AddPos,
                typ => return Err(anyhow!("Invalid cipher type: 0x{:02x}", typ)),
            };
            rv.push(cipher);
        }
        self.ciphers = rv;

        info!("-> Server: ciphers {:?}", self.ciphers);

        Ok(())
    }

    pub async fn recv_line(&mut self) -> Result<String> {
        let mut original = vec![];

        let mut buf = vec![];

        loop {
            let v = self.reader.read_u8().await?;
            original.push(v);
            info!("<- Server: encoded byte 0x{:02x}", v);
            let v = self.decode(self.read_pos, v);
            info!("<- Server: decoded byte 0x{:02x}", v);
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
            buf[i] = self.encode(self.write_pos, buf[i]);
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

    #[inline]
    fn encode(&mut self, i: usize, mut v: u8) -> u8 {
        for cipher in self.ciphers.iter() {
            v = cipher.encode(i, v);
        }

        v
    }

    #[inline]
    fn decode(&mut self, i: usize, mut v: u8) -> u8 {
        for j in (0..self.ciphers.len()).rev() {
            v = self.ciphers[j].decode(i, v);
        }

        v
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
        CIPHER_ADD_POS, CIPHER_END, CIPHER_REVERSE_BITS, CIPHER_XOR,
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

        let mut data = vec![
            CIPHER_XOR,
            123_u8,
            CIPHER_ADD_POS,
            CIPHER_REVERSE_BITS,
            CIPHER_END,
        ];

        let buf: Vec<_> = b"4x dog,5x car\n3x rat,2x cat\n"
            .into_iter()
            .enumerate()
            .map(|(i, b)| (i, Cipher::Xor(123).encode(i + 5, *b)))
            .map(|(i, b)| (i, Cipher::AddPos.encode(i + 5, b)))
            .map(|(i, b)| Cipher::ReverseBits.encode(i + 5, b))
            .collect();
        data.put_slice(&buf);

        cli.write_all(&data).await.unwrap();

        while let Ok(v) = cli.read_u8().await {
            info!("recv 0x{:02x}", v);
        }

        sleep(Duration::from_secs(10)).await;
    }
}
