use std::thread;
use std::time::{Duration, Instant};


fn main() -> std::io::Result<()> {
	let path = "/tmp/test_socket";

	/* Server code */
	let server = thread::spawn(move || -> std::io::Result<()> {
		let mut epi_server = epistle::Server::new(path)?;
		epi_server.listen()?;

		let mut line = String::new();
		epi_server.msg_read(&mut line)?;
		println!("server got: {}", line.trim());

		epi_server.msg_send(&format!("echo: {line}"))?;

		loop {
			println!("loop running");
			let _ = epi_server.looper.wait_and_process(1000);
		}
	});

	/* Client code */
	let mut epi_client = epistle::Client::new()?;
	epi_client.connect(path)?;

	epi_client.msg_send("hello server\n")?;

	let mut line = String::new();
	epi_client.msg_read(&mut line)?;
	println!("client got: {}", line.trim());

	/* Wait for the server thread */
	server.join().expect("server thread panicked")?;
	std::fs::remove_file(path)?;

	Ok(())
}
