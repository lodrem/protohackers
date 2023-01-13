image := "ghcr.io/lodrem/protohackers"
image_tag := "v0.0.1"

default:
  just --list

image:
  docker buildx build . \
    --output=type=docker \
    --no-cache \
    --force-rm \
    --tag {{ image }}:{{ image_tag }} \
    --file Dockerfile

run target:
  @echo 'Running solution for {{target}}'
  cargo run --release -- -c {{target}}