use super::*;

/* TODO: These tests cover most of the standard branch of the
   looper (when everything works). Add wrapper for syscalls
   so fail branches can also be tested */

#[test]
fn create_looper() -> Result<()>{
        let mut looper = Loop::<u8>::new().unwrap();
        assert_eq!(looper.owner.creator, unsafe { libc::pthread_self() });
        assert_eq!(looper.owner.current, unsafe { libc::pthread_self() });
        assert!(looper.pollfds.is_empty());
        assert!(looper.reg_fds.is_empty());

        looper.wakeup_pipe_w.write_all(b"hello")?;
        
        let mut buf = [0u8; 5];
        looper.wakeup_pipe_r.read_exact(&mut buf)?;
        assert_eq!(&buf, b"hello");

        Ok(())
}

#[test]
fn loop_timeout() {
        let mut looper = Loop::<u8>::new().unwrap();
        let mut buf = 0u8;

        let ret = looper.wait_and_process(&mut buf, 100);

        assert_eq!(ret.unwrap_err().kind(), ErrorKind::TimedOut);
}

#[test]
fn loop_wakeup() -> Result<()> {
        let mut looper = Loop::<u8>::new().unwrap();
        let mut buf = 0u8;

        looper.wakeup()?;

        let ret = looper.wait_and_process(&mut buf, -1);

        assert!(!ret.is_err());
        Ok(())
}

#[test]
fn loop_add_fd() -> Result<()> {
        let mut looper = Loop::<u8>::new().unwrap();
        let mut buf = 0u8;

        let (piper, mut pipew) = io::pipe()?;
        looper.add(
                piper.as_raw_fd(),
                libc::POLLIN,
                move |_| {});

        pipew.write_all(&[0x1])?;

        let ret = looper.wait_and_process(&mut buf, 100);
        if ret.is_err() {
                assert_ne!(ret.as_ref().unwrap_err().kind(), ErrorKind::TimedOut);
                return ret;
        }

        Ok(())
}
