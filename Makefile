shell:
	docker-compose -f docker-compose.yml run --rm  --use-aliases app /bin/bash

build:
	docker-compose -f docker-compose.yml build


replicator:
	# docker-compose -f docker-compose.yml run --rm  --service-ports app cargo run --bin=replicator --verbose -- --config=fixtures/example_config.toml -p 9445 -h 0.0.0.0

	docker-compose -f docker-compose.yml run --rm --use-aliases app cargo run --bin=kafka-replicator --verbose -- examples/example-config.yaml

up:
	docker-compose -f docker-compose.yml up --rm

test:
	RUST_LOG="debug" cargo test -- --nocapture
	# RUST_LOG="librdkafka=trace,rdkafka::client=debug;replicator=debug" cargo test -- --nocapture


kafkacat:
	docker-compose -f docker-compose.yml run --rm --use-aliases --service-ports replicator_kafkacat
