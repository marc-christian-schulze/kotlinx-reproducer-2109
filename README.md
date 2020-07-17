To build and test on your machine:
```
$ wget https://dl.min.io/server/minio/release/linux-amd64/minio
$ chmod +x minio
$ ./gradlew build
```
If in doubt whether local caches cause side effects try the container:
```
$ docker build -t test .
```
