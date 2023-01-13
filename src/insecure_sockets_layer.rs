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
            Self::ReverseBits => v.reverse_bits(),
            Self::Xor(n) => v ^ n,
            Self::XorPos => v ^ (i as u8),
            Self::Add(n) => ((v as u16 + *n as u16) % 256) as u8,
            Self::AddPos => ((v as u16 + i as u16) % 256) as u8,
        }
    }

    #[inline]
    pub fn decode(&self, i: usize, v: u8) -> u8 {
        match self {
            Self::ReverseBits => v.reverse_bits(),
            Self::Xor(n) => v ^ n,
            Self::XorPos => v ^ (i as u8),
            Self::Add(n) => ((v as i16 - *n as i16).rem_euclid(256)) as u8,
            Self::AddPos => ((v as i16 - i as i16).rem_euclid(256)) as u8,
        }
    }
}

#[derive(Debug, Clone)]
struct Ciphers(Vec<Cipher>, usize, usize);

impl Ciphers {
    #[inline]
    pub fn encode(&mut self, mut v: u8) -> u8 {
        self.0.iter().for_each(|cipher| {
            v = cipher.encode(self.1, v);
        });
        self.1 += 1;

        v
    }

    #[inline]
    pub fn decode(&mut self, mut v: u8) -> u8 {
        self.0.iter().rev().for_each(|cipher| {
            v = cipher.decode(self.2, v);
        });
        self.2 += 1;

        v
    }

    #[inline]
    pub fn is_noop(&self) -> bool {
        let mut me = Self(self.0.clone(), 0, 0);
        for i in 0..255 {
            if me.encode(i) != i {
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
            ciphers: Ciphers(vec![], 0, 0),
        }
    }

    pub async fn prepare_ciphers(&mut self) -> Result<()> {
        let mut rv = vec![];
        loop {
            let v = self.reader.read_u8().await?;
            let cipher = match v {
                CIPHER_REVERSE_BITS => Cipher::ReverseBits,
                CIPHER_XOR => Cipher::Xor(self.reader.read_u8().await?),
                CIPHER_XOR_POS => Cipher::XorPos,
                CIPHER_ADD => Cipher::Add(self.reader.read_u8().await?),
                CIPHER_ADD_POS => Cipher::AddPos,
                CIPHER_END => break,
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
        let mut buf = vec![];
        loop {
            let v = self.ciphers.decode(self.reader.read_u8().await?);
            if v == b'\n' {
                break;
            }
            buf.push(v);
        }

        let rv = unsafe { String::from_utf8_unchecked(buf) };
        info!("-> Server: request '{}'", rv);

        Ok(rv)
    }

    pub async fn send_line(&mut self, v: String) -> Result<()> {
        info!("<- Server: response '{}'", v);
        let mut buf = v.into_bytes();
        buf.push(b'\n');

        for i in 0..buf.len() {
            buf[i] = self.ciphers.encode(buf[i]);
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
