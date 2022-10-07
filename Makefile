dbt-docs:
	@echo "Generating docs and spinning up the a webserver on port 8081..."
	@docker-compose run -p "8081:8081" dbt_image bash -c "dbt deps && dbt docs generate -t prod && dbt docs serve --port 8081"

generate-dbt-docs:
	@echo "Generating docs"
	@docker-compose run dbt_image bash -c "dbt deps && dbt docs generate -t prod"

data-image:
	@echo "Attaching to data-image and mounting repo..."
	@docker-compose run data_image bash

dbt-bash:
	@echo "Running bash with dbt..."
	@docker-compose run dbt_image bash -c "dbt deps && /bin/bash"