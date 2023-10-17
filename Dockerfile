FROM keboolabot/keboola-processor-split-dev as builder

COPY . /code

RUN /code/scripts/build-processor.sh

FROM scratch

WORKDIR /app

COPY --from=builder /code/build/target/processor /app/run

ENTRYPOINT ["/app/run"]
