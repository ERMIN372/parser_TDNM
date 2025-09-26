"""Utility script to create a timestamped SQLite backup."""

from __future__ import annotations

from app.utils.backup import create_timestamped_backup


def main() -> None:
    archive = create_timestamped_backup()
    print(f"Backup saved to {archive}")


if __name__ == "__main__":
    main()
