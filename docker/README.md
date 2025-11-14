# MDWF_DB - Docker images
For more information on how to use Docker, please see the [Docker documentation](https://docs.docker.com/get-started/).

### How to build

Currently, the image is not yet pushed to a registry but one can build it locally and run the container.

You may build it with either docker or podman/podman-hpc. Here's an example:
```
podman build -t mdwf_db:v0 .
```
Here ```mdwf_db``` is the image name and ```v0``` is the tag. The ```.``` after the image name indicates the folder with Dockerfile as the working directory.

After the build, you may list the image you have by typing:
```
podman images
```
And you may wish to push the built image to a registry for later uses.

### How to run

Start the session interactively with running containers
```
export DB_PATH=/path/to/your_database
cd ${DB_PATH}
podman run --rm -it -v$(pwd):/home/mdwf_db mdwf_db:v0 
```
Here are some detailed explanations: 
- ```--rm``` will remove the running cache after exiting.
- ```-it``` will start an interactive session.
- ```-v$(pwd):/home/mdwf_db``` mounts the current working directory (the folder which you should have database file) in the image's working
directory (here it's /home/mdwf_db in the image to match the default setting in the Dockerfile). 
- ``mdwf_db:v0``` is the image name and its tag. You can also use an address from a registry.

***WARNING*** Mounting a host volume will change the original files, please proceed with care or create a copy to do testings.
Alternatively, you can use a non-persistent version in a container-bounded/isolated environment by running
```
podman run --rm -it -v$(pwd):/mdwf_db mdwf_db:v0
```
### What's next
In the future, one may kickoff the container as a service daemon, expose its port and talk to it via GUI.
