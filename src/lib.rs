use std::io::{BufRead, BufReader, Write, Read};
use std::io::{Error, ErrorKind, Result};
use std::io::{self, PipeReader, PipeWriter};
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::fd::AsRawFd;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use std::thread;


/* Used for ownership verification on debug
   TODO: Implement owner checker */
struct LoopOwner {
	creator: libc::pthread_t,
	current: libc::pthread_t,
	waiting: Option<libc::pthread_t>,
}

struct LoopFileDescriptor {
	fd: i32,
	events: i16,
	cb: Box<dyn Fn()>,
}

pub struct Loop {
	owner: LoopOwner,
	check_owner: bool,
	/* Holds the list of fd to be polled */
	pollfds: Vec<libc::pollfd>,
	/* Used to wakeup the looper from outside */
	wakeup_pipe_r: PipeReader,
	wakeup_pipe_w: PipeWriter,
	/* Holds registered fd */
	fds: Vec<LoopFileDescriptor>,
}

pub struct Server {
	addr: String,
	writer: Option<UnixStream>,
	reader: Mutex<Option<BufReader<UnixStream>>>,
	pub looper: Loop,
	event_cb: Option<Box<dyn Fn(u32)>>,
}

pub struct Client {
	writer: Option<UnixStream>,
}

impl Loop {
	pub fn new() -> Result<Self> {
		let tid: libc::pthread_t = unsafe { libc::pthread_self() };
		let (piper, pipew) = io::pipe()?;

		Ok(Self {
			owner: LoopOwner {
				creator: tid,
				current: tid,
				waiting: None,
			},
			check_owner: true,
			pollfds: Vec::new(),
			wakeup_pipe_r: piper,
			wakeup_pipe_w: pipew,
			fds: Vec::new(),
		})
	}

	pub fn add(&mut self, fd: i32, events: i16, cb: impl Fn() + 'static) {
		self.fds.push(
			LoopFileDescriptor {
				fd: fd,
				events: events,
				cb: Box::new(cb),
			}
		)
	}

	pub fn wait_and_process(&mut self, timeout: i32) -> Result<()> {
		let tid: libc::pthread_t = unsafe { libc::pthread_self() };
		self.owner.current = tid;
		self.owner.waiting = Some(tid);

		/* Do wait and process */
		self.pollfds.clear();

		/* Push wakeup fd */
		self.pollfds.push(libc::pollfd {
			fd: self.wakeup_pipe_r.as_raw_fd(),
			events: libc::POLLIN,
			revents: 0,
		});

		/* Push registered fds */
		for fd in &self.fds {
			self.pollfds.push(libc::pollfd {
				fd: fd.fd,
				events: fd.events,
				revents: 0,
			})
		}

		let ret = unsafe {
			libc::poll(
				self.pollfds.as_mut_ptr(),
				self.pollfds.len() as libc::nfds_t,
				timeout
			)
		};

		/* TODO: Handle events */
		for fd in &self.pollfds {
			println!("revents {:#x}", fd.revents);
		}

		self.owner.current = self.owner.creator;

		/* TODO: Handle actual wait and process on a different
		   method so I can centralize errors from polling and
		   events */
		if ret < 0 {
			/* Read errno */
			return Err(Error::last_os_error());
		}
		Ok(())
	}
}

impl Server {
	pub fn new(addr: &str) -> Result<Self> {
		Ok(Self {
			addr: addr.to_string(),
			writer: None,
			reader: None.into(),
			looper: Loop::new()?,
			event_cb: None,
		})
	}

	pub fn set_event_handler(&mut self, cb: impl Fn(u32) + 'static) {
		self.event_cb = Some(Box::new(cb));
	}

	pub fn listen(&mut self) -> Result<()> {
		println!("listening to {}", self.addr);

		let _ = std::fs::remove_file(&self.addr);
		/* This could be separated into socket creation and binding
		   if extra socket configuration is needed.
		   TODO: keep listener alive in server structure */
		let listener = Some(UnixListener::bind(&self.addr)?);
		let (stream, _addr) = listener.as_ref()
			.expect("listener is None").accept()?;

		self.reader = Some(BufReader::new(stream.try_clone()?)).into();
		self.writer = Some(stream);

		self.looper.add(
			self.writer.as_ref()
				.expect("UnixListener is None")
				.as_raw_fd(),
			libc::POLLIN,
			move || {});
		Ok(())
	}

	pub fn msg_read(&self, line: &mut String) -> Result<()> {
		let mut guard = self.reader.lock().unwrap();
		let reader = guard.as_mut()
			.ok_or_else(|| Error::new(ErrorKind::NotConnected,
					"Reader is None"))?;
		reader.read_line(line)?;
		Ok(())
	}

	pub fn msg_read_to_end(&self, dump: &mut Vec<u8>) -> Result<()> {
		let mut guard = self.reader.lock().unwrap();
		let reader = guard.as_mut()
			.ok_or_else(|| Error::new(ErrorKind::NotConnected,
					"Reader is None"))?;
		reader.read_to_end(dump)?;
		Ok(())
	}

	pub fn msg_send(&self, msg: &str) -> Result<()> {
		self.writer.as_ref().expect("Writer is None")
			.write_all(msg.as_bytes())?;
		Ok(())
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
mod tests {
	use super::*;

	#[test]
	fn replaces_stale_socket_file() {
		let dir = tempfile::tempdir().unwrap();
		let path = dir.path().join("test.sock");
		let path = path.to_str().unwrap();

		/* Dropping a UnixListener does NOT delete its socket file,
		   so this simulates a leftover from a crashed/previous run. */
		drop(new_listener(path).unwrap());
		assert!(std::path::Path::new(path).exists());

		/* Plain UnixListener::bind would fail here with AddrInUse. */
		new_listener(path).expect("should rebind over stale socket");
	}
}