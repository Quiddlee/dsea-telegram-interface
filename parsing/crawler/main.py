import argparse
import logging
import os

from .crawler import run_crawler


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run parsing crawler.")
    parser.add_argument(
        "--artifacts-dir",
        default=os.environ.get("ARTIFACTS_DIR", "parsing/artifacts"),
        help="Directory for crawler artifacts (default: parsing/artifacts)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip writing artifacts to disk",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = _parse_args()
    run_crawler(artifacts_dir=args.artifacts_dir, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
