run:
	git clone --recursive https://github.com/amundsen-io/amundsen.git; \
	cd amundsen; \
	docker compose -f docker-amundsen-local.yml up; \
