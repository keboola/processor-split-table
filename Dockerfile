FROM keboola/processor-split-dev as builder

RUN /code/build.sh

FROM scratch

WORKDIR /app

COPY --from=builder /code/build/run /app/run

ENTRYPOINT ["/app/run"]
