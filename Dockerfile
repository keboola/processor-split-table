FROM keboolabot/keboola-processor-split-dev as builder

COPY . /code

RUN /code/scripts/build-processor.sh

FROM scratch

WORKDIR /app

COPY --from=builder /code/build/target/processor /app/run

# Let Go know how much memory it has available.
ENV GOMEMLIMIT="400MiB"

ENTRYPOINT ["/app/run"]
