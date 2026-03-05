# discord_talk_backfill

Backfill Discord channel history into Nextcloud Talk rooms using `matterbridge.toml` account and gateway mappings.

On startup, the script creates its own project virtual environment (`.venv`) if needed, re-runs itself inside that environment, and installs dependencies from `requirements.txt` automatically.

This tool only backfills `discord.*` to `nctalk.*` gateway pairs. Other bridge/account types in `matterbridge.toml` are ignored.

## Features

- Uses `matterbridge.toml` for:
  - Discord bot tokens (`[discord.*].Token`)
  - Nextcloud Talk credentials (`[nctalk.*].Server/Login/Password`)
  - Discord to Talk room mappings from enabled `[[gateway]]` blocks
- Creates and uses project `.venv` automatically when not already running in a virtual environment
- Ensures dependencies from `requirements.txt` are installed before runtime
- Supports dry-run mode and message window filters
- Applies Talk `RemoteNickFormat` with a backfill marker in the posted author line

## Requirements

- Python 3.10+
- A valid `matterbridge.toml` with `discord`, `nctalk`, and `gateway` sections

## Configuration

Use `matterbridge.example.toml` as a template:

```bash
cp matterbridge.example.toml matterbridge.toml
```

Then fill real values in `matterbridge.toml`.

Default config path is `./matterbridge.toml`.
You can override it with `MATTERBRIDGE_CONFIG=/path/to/matterbridge.toml`.

## Usage

### Show help

```bash
python discord_to_talk_backfill.py --help
```

### List utilities

```bash
python discord_to_talk_backfill.py --list-functions
```

### Run backfill

```bash
python discord_to_talk_backfill.py
```

### Dry run (no posting)

```bash
python discord_to_talk_backfill.py --dryrun
```

### Limit to most-recent messages

```bash
python discord_to_talk_backfill.py --maxmessages=500
```

### Exclude old messages

```bash
python discord_to_talk_backfill.py --daysback=30
```

### Combine options

```bash
python discord_to_talk_backfill.py --dryrun --maxmessages=300 --daysback=14
```

## Command-line arguments

- `--help` show command usage
- `--list-functions` print script utilities and exit
- `--dryrun` preview output only; do not post
- `--maxmessages=NUMBER` process up to NUMBER most-recent messages per channel
- `--daysback=NUMBER` skip messages older than NUMBER days

## Environment variables

- `MATTERBRIDGE_CONFIG` path to config file (default: `matterbridge.toml`)
- `DISCORD_LIMIT` optional fallback for max messages when `--maxmessages` is not provided
- `DAYS_BACK` optional fallback for days filter when `--daysback` is not provided
- `DRY_RUN` optional fallback dry-run flag (`1` enables dry-run)
- `POST_DELAY_SECONDS` delay between posts (default: `0.35`)

## Notes

- Discord messages with no text and no attachments are skipped.
- The script posts attachments links only; it does not include Discord message permalinks.
- If no message limit is provided, all available history may be read.
