from pprint import pprint

from execution.builder_auth import health_snapshot


def main() -> None:
    print("=== Executor Health Check ===")
    snapshot = health_snapshot()
    pprint(snapshot)


if __name__ == "__main__":
    main()
