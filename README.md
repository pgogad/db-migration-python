docker run --name postgresql -it --rm --publish 5433:5432 --env 'PG_PASSWORD=test' --volume /Users/pawan/workspace/db-migration/data:/var/lib/postgresql --name postgres_doc sameersbn/postgresql:10

docker run -it --name ps-logstash --link some-rabbit --rm --volume /Users/pawan/workspace/db-migration-python/fs/:/data/ tech-spawn-logstash:latest

docker run -it --rm --hostname my-rabbit --name some-rabbit -p 8080:15672 -e RABBITMQ_DEFAULT_USER=user -e RABBITMQ_DEFAULT_PASS=password -v /Users/pawan/workspace/db-migration-python/rabbitmq/log:/var/log/rabbitmq -v /Users/pawan/workspace/db-migration-python/rabbitmq/data:/var/lib/rabbitmq rabbitmq:3-management