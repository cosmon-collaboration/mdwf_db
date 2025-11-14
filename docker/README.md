# MDWF_DB - Docker images
For more information on how to use Docker, please see the [Docker documentation](https://docs.docker.com/get-started/).

### How to build

Currently, the image is not yet pushed to a registry but one can build it locally and run the container.

You may build it with either docker or podman/podman-hpc. Here's an example:
```
podman build -t mdwf_db:v0 .
```
Here ```mdwf_db``` is the image name and ```v0``` is the tag. ```.``` indicates the folder with Dockerfile.

You may wish to push the built image to a registry.

### How to run

Start the session with
```
cd ${DB_PATH}
podman run --rm -it -v$(pwd):/mdwf_db mdwf_db:v0
```
which mounts the current working directory (the folder which you should have database file) in the image's working
directory.  Alternatively, you can use a non-persistent version by running in the container only.
```
podman run --rm -it -v$(pwd):/mdwf_db mdwf_db:v0
```
