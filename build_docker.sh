# replace "/" by "-", vì docker tag không cho phép dấu "/"
ORIGINAL_BRAND_NAME=$(git symbolic-ref --short HEAD)
VERSION=v1.0.2
DOCKER_IMAGE=airlake
DOCKER_REGISTRY=vantuan12345
#-----------------------------------------------------------------------
docker build . -t ${DOCKER_REGISTRY}/${DOCKER_IMAGE}:${VERSION}
docker push ${DOCKER_REGISTRY}/${DOCKER_IMAGE}:${VERSION}
