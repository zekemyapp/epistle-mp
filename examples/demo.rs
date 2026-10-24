use std::{thread, time};
use epistle::Connection;


fn main() -> std::io::Result<()> {
	let path = "/tmp/test_socket";

	/* Server code */
	let server = thread::spawn(move || -> std::io::Result<()> {
		let mut epi_server = epistle::Server::new(
			path,
			move |conn: &Connection, msg: &str| {
				println!("server got: {}", msg.trim());
				let _ = conn.msg_send(&format!("echo: {msg}"));
			}
		)?;
		epi_server.listen()?;

		/* Test wakeup before waiting for client to write */
		epi_server.looper.wakeup()?;

		loop {
			let _ = epi_server.looper
				.wait_and_process(
					&mut epi_server.conn,
					-1);
		}
	});

	/* Client code */
	let mut epi_client = epistle::Client::new()?;
	epi_client.connect(path)?;
	loop {
		epi_client.msg_send("hello server\n")?;

		let mut line = String::new();
		epi_client.msg_read(&mut line)?;
		println!("client got: {}", line.trim());

		thread::sleep(time::Duration::from_millis(1000));
	}

	/* Wait for the server thread */
	server.join().expect("server thread panicked")?;
	std::fs::remove_file(path)?;

	Ok(())
}
