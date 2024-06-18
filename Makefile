run:
	git clone --recursive https://github.com/amundsen-io/amundsen.git; \
	docker compose -f amundsen/docker-amundsen.yml up; \

stop:
	docker compose -f amundsen/docker-amundsen.yml down

clean:
	docker compose -f amundsen/docker-amundsen.yml down --volumes --remove-orphans --rmi all

