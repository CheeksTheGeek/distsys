all:
	cargo build --release

clean:
	cargo clean
	rm -rf .data

run client:
	cargo run --bin client

run datanode:
	cargo run --bin datanode -- --port 4120 --datadir .data

run namenode:
	cargo run --bin namenode
