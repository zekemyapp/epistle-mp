use std::io::{Write, Read};
use std::io::{Error, ErrorKind, Result};
use std::io::{self, PipeReader, PipeWriter};
use std::os::fd::AsRawFd;

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

		let mut ret: libc::c_int;
		loop {
			ret = unsafe {libc::poll(
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

		let nevents = ret;

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

		if timeout == -1 || nevents > 0 {Ok(())}
		else {
			Err(Error::new(
				ErrorKind::TimedOut,
				"Poll timeout"))
		}
	}

	fn wakeup_cb (&mut self, revents:i16) {
		if revents & libc::POLLIN != 0 {     
			let mut buf = [0x0; 1];
			/*TODO: Handle return */
    			let _ = self.wakeup_pipe_r.read_exact(&mut buf);
		}
	}
}

#[cfg(test)]
mod tests;
