use std::io::{BufRead, BufReader, Write, Read};
use std::io::{Error, ErrorKind, Result};
use std::io::{self, PipeReader, PipeWriter};
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::fd::AsRawFd;
use std::time::{Duration, Instant};
use std::thread;


/* Used for ownership verification on debug
   TODO: Implement owner checker */
struct LoopOwner {
	creator: libc::pthread_t,
	current: libc::pthread_t,
	waiting: Option<libc::pthread_t>,
}

struct LoopFileDescriptor<T>{
	fd: i32,
	events: i16,
	cb: Box<dyn FnMut(&mut T)>,
}

pub struct Loop<T> {
	owner: LoopOwner,
	check_owner: bool,
	/* Holds the list of fd to be polled */
	pollfds: Vec<libc::pollfd>,
	/* Used to wakeup the looper from outside */
	wakeup_pipe_r: PipeReader,
	wakeup_pipe_w: PipeWriter,
	/* Holds registered fds */
	reg_fds: Vec<LoopFileDescriptor<T>>,
}

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

impl<T> Loop<T> {
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
			reg_fds: Vec::new(),
		})
	}

	pub fn add(&mut self, fd: i32, events: i16, cb: impl FnMut(&mut T) + 'static) {
		self.reg_fds.push(
			LoopFileDescriptor {
				fd: fd,
				events: events,
				cb: Box::new(cb),
			}
		)
	}

	pub fn wakeup(&mut self) -> Result<()> {
		self.wakeup_pipe_w.write_all(&[0x1])?;
		Ok(())
	}

	pub fn wait_and_process(&mut self, ctx: &mut T, timeout: i32) -> Result<()> {
		let tid: libc::pthread_t = unsafe { libc::pthread_self() };
		self.owner.current = tid;
		self.owner.waiting = Some(tid);

		let ret = self.do_wait_and_process(ctx, timeout);

		self.owner.current = self.owner.creator;
		/* TODO: clear owner.waiting? */

		return ret;
	}

	fn do_wait_and_process(&mut self, ctx: &mut T, timeout: i32) -> Result<()> {
		let mut woke_up:i16 = 0x0;

		/* Do wait and process */
		self.pollfds.clear();

		/* Push wakeup fd */
		self.pollfds.push(libc::pollfd {
			fd: self.wakeup_pipe_r.as_raw_fd(),
			events: libc::POLLIN,
			revents: 0,
		});

		/* Push registered fds */
		for fd in &self.reg_fds {
			self.pollfds.push(libc::pollfd {
				fd: fd.fd,
				events: fd.events,
				revents: 0,
			})
		}

		loop {
			let ret = unsafe {libc::poll(
				self.pollfds.as_mut_ptr(),
				self.pollfds.len() as libc::nfds_t,
				timeout
			)};

			if ret < 0 {
				let os_error = Error::last_os_error();
				if os_error.kind() == ErrorKind::Interrupted {
				/* TODO: With a finite timeout, repeated signals
				 can make the call wait longer than requested.
				 Track a deadline and pass the remaining time
				 on each retry. */
					continue;
				} else {
					return Err(os_error);
				}
			}

			break;
		}

		/* TODO: Add watchdog to watch client callbacks */
		for pfd in &self.pollfds {
			let revents = pfd.revents;
			if revents == 0 {continue};

			if pfd.fd == self.wakeup_pipe_r.as_raw_fd() {
				woke_up = revents;
				continue;
			}

			for regfd in &mut self.reg_fds {
				if regfd.fd == pfd.fd {
					/* Add revent to callback parameters */
					(regfd.cb)(ctx);
				}
			}
		}
		if woke_up != 0 {self.wakeup_cb(woke_up);}

		Ok(())
	}

	fn wakeup_cb (&mut self, revents:i16) {
		if revents & libc::POLLIN != 0 {     
			let mut buf = [0x0; 1];
			/*TODO: Handle return */
    			let _ = self.wakeup_pipe_r.read_exact(&mut buf);
		}
	}
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