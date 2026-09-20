# Must match the Dockerfile and docker-compose.yml.
SQLFLOW_IMAGE ?= turbolytics/sql-flow:v2026.09.19.1

.PHONY: validate rollups up test psql clean

# render.yaml is checked against Render's published schema, fetched each run:
# Render renames plans and deprecates keys, and a vendored copy would pass a
# file Render had started to refuse. Needs uv: https://docs.astral.sh/uv/
validate:
	./bin/validate-render.py ../render.yaml
	docker run --rm -v $(CURDIR):/w -w /w \
		-e SQLFLOW_POSTGRES_URI=x -e SQLFLOW_WEBHOOK_HMAC_SECRET=x -e SQLFLOW_WRITER_ID=x \
		$(SQLFLOW_IMAGE) validate pipeline.yml
	docker run --rm -v $(CURDIR):/w -w /w \
		-e SQLFLOW_POSTGRES_URI=x -e SQLFLOW_SERVE_CLIENT_ID=x \
		$(SQLFLOW_IMAGE) validate serve.yml
	docker run --rm -v $(CURDIR):/w -w /w $(SQLFLOW_IMAGE) validate rollups.yml
	@# rollup check requires --serve. rollups.yml generates no dataset, so the
	@# serve file is only loaded, and the migration is what is compared.
	docker run --rm -v $(CURDIR):/w -w /w \
		-e SQLFLOW_POSTGRES_URI=x -e SQLFLOW_SERVE_CLIENT_ID=x \
		$(SQLFLOW_IMAGE) rollup check \
		-c rollups.yml --migration migrations/0003_rollups.sql --serve serve.yml

rollups:
	docker run --rm -v $(CURDIR):/w -w /w $(SQLFLOW_IMAGE) rollup ddl \
		-c rollups.yml > migrations/0003_rollups.sql
	@echo "# regenerated migrations/0003_rollups.sql"

up:
	docker compose up --build

test:
	SQLFLOW_IMAGE=$(SQLFLOW_IMAGE) test/e2e.sh

psql:
	docker compose exec postgres psql -U metrics -d metrics

clean:
	docker compose down -v
