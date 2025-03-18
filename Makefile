start:
	docker compose -f tests/omdb/docker-compose.yaml up -d $(SERVICE)

stop:
	docker compose -f tests/omdb/docker-compose.yaml down -v $(SERVICE)

bash-sourcedb:
	docker exec -it omdb-sourcedb bash

bash-targetdb:
	docker exec -it omdb-targetdb bash

.PHONY: start stop bash-sourcedb bash-targetdb
