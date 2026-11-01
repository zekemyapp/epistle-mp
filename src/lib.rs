mod looper;

use looper::Loop;
use std::io::{BufRead, BufReader, Write, Read};
use std::io::{Error, ErrorKind, Result};
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::fd::AsRawFd;
use std::time::{Duration, Instant};
use std::thread;


pub struct Connection {
    addr: String,
    reader: Option<BufReader<UnixStream>>,
    writer: Option<UnixStream>,
    event_cb: Box<dyn Fn(&Connection, &str)>,
}

pub struct Server {
	pub looper: Loop<Connection>,
	pub conn: Connection
}

pub struct Client {
	writer: Option<UnixStream>,
}

impl Connection {

	fn server_cb (&mut self) {
		let mut line = String::new();
		if self.msg_read(&mut line).is_ok() {
			(self.event_cb)(self, line.as_str());
		}
	}

	pub fn msg_read(&mut self, line: &mut String) -> Result<()> {
		let reader = self.reader.as_mut()
			.ok_or_else(|| Error::new(ErrorKind::NotConnected,
					"Reader is None"))?;
		reader.read_line(line)?;
		Ok(())
	}

	pub fn msg_send(&self, msg: &str) -> Result<()> {
		self.writer.as_ref().expect("Writer is None")
			.write_all(msg.as_bytes())?;
		Ok(())
	}

}

impl Server {
	pub fn new(addr: &str, cb: impl Fn(&Connection, &str) + 'static) -> Result<Self> {
		Ok(Self {
			looper: Loop::new()?,
			conn: Connection { 
				addr: addr.to_string(),
				writer: None,
				reader: None.into(),
				event_cb: Box::new(cb),
			},
		})
	}

	pub fn listen(&mut self) -> Result<()> {
		println!("listening to {}", self.conn.addr);

		let _ = std::fs::remove_file(&self.conn.addr);
		/* This could be separated into socket creation and binding
		   if extra socket configuration is needed.
		   TODO: keep listener alive in server structure */
		let listener = Some(UnixListener::bind(&self.conn.addr)?);
		let (stream, _addr) = listener.as_ref()
			.expect("listener is None").accept()?;

		self.conn.reader = Some(BufReader::new(stream.try_clone()?)).into();
		self.conn.writer = Some(stream); // TODO: change name. This does more than writing

		self.looper.add(
			self.conn.writer.as_ref()
				.expect("UnixListener is None")
				.as_raw_fd(),
			libc::POLLIN,
			Connection::server_cb);
		Ok(())
	}

	pub fn msg_send(&self, msg: &str) -> Result<()> {
		self.conn.msg_send(msg)
	}
	
}

impl Client {
	pub fn new() -> Result<Self> {
		Ok(Self {
			writer: None,
		})
	}

	pub fn connect(&mut self, addr: &str ) -> Result<()> {
		let stream = Self::connect_with_retry(addr, None)?;
		self.writer = Some(stream);
		Ok(())
	}

	pub fn msg_read(&self, line: &mut String) -> Result<()> {
		let mut buf = [0u8; 1024];
		let n = self.writer.as_ref().expect("writer is None").read(&mut buf)?;
		*line = String::from_utf8_lossy(&buf[..n]).to_string();
		Ok(())
	}

	pub fn msg_send(&self, msg: &str) -> Result<()> {
		self.writer.as_ref().expect("Writer is None").write_all(msg.as_bytes())?;
		Ok(())
	}

	fn connect_with_retry(path: &str, timeout: Option<Duration>) -> Result<UnixStream> {
		let start = Instant::now();
		loop {
			match UnixStream::connect(path) {
				Ok(stream) => return Ok(stream),
				Err(e) if matches!(
					e.kind(),
					ErrorKind::NotFound | ErrorKind::ConnectionRefused
				) => {
					if let Some(t) = timeout {
						if start.elapsed() >= t {
							return Err(e);
						}
					}
					thread::sleep(Duration::from_millis(100));
				}
				Err(e) => return Err(e),
			}
		}
	}
}


/* Kept as an example for unit testing for later implementation */
pub fn new_listener(addr: &str) -> Result<UnixListener> {
	let _ = std::fs::remove_file(addr);
	return UnixListener::bind(addr);
}


#[cfg(test)]
mod tests;
