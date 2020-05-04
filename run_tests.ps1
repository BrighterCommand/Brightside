docker-compose -f tests-docker-compose.yml up -d --build
Start-Sleep 2
docker exec -it $(docker ps -aqf "name=brightside-tests") pipenv run python testrunner.py
docker-compose -f tests-docker-compose.yml down
