"""Source processor process entrypoint."""

from appchi.kafka_source import SourceConsumer
from appchi.pipeline_runtime import run


def main():
    run("source", SourceConsumer)


if __name__ == "__main__":
    main()
