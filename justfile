image := "ghcr.io/lodrem/protohackers"
image_tag := "v0.0.1"

image:
  docker buildx build . \
    --output=type=docker \
    --no-cache \
    --force-rm \
    --tag {{ image }}:{{ image_tag }} \
    --file Dockerfile