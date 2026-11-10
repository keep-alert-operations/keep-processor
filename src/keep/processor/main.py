"""Source processor process entrypoint."""

from keep.kafka_source import SourceConsumer
from keep.pipeline_runtime import run


def main():
    run("source", SourceConsumer)


if __name__ == "__main__":
    main()
