#!/usr/bin/env python3
import asyncio
import argparse
import os
import subprocess
import sys
import time
import venv
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib


def is_running_in_venv() -> bool:
    return bool(getattr(sys, "real_prefix", None)) or (sys.prefix != sys.base_prefix)


def ensure_venv() -> None:
    if is_running_in_venv():
        return

    project_dir = Path(__file__).resolve().parent
    venv_dir = project_dir / ".venv"
    venv_python = venv_dir / "bin" / "python"

    if not venv_python.exists():
        print(f"Creating virtual environment at {venv_dir}")
        venv.create(venv_dir, with_pip=True)

    current_python = Path(sys.executable).resolve()
    target_python = venv_python.resolve()

    if current_python != target_python:
        os.execv(
            str(target_python),
            [str(target_python), str(Path(__file__).resolve()), *sys.argv[1:]],
        )


def ensure_requirements() -> None:
    project_dir = Path(__file__).resolve().parent
    requirements_path = project_dir / "requirements.txt"
    if not requirements_path.exists():
        raise SystemExit(f"ERROR: missing dependency file: {requirements_path}")
    if not requirements_path.is_file():
        raise SystemExit(f"ERROR: dependency path is not a file: {requirements_path}")
    if not os.access(requirements_path, os.R_OK):
        raise SystemExit(f"ERROR: no read permission for dependency file: {requirements_path}")

    print(f"Ensuring dependencies from {requirements_path}")
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "-r",
            str(requirements_path),
        ],
        check=True,
    )


ensure_venv()
ensure_requirements()

import discord
import requests

UTILITY_FUNCTIONS = [
    ("--help", "Show CLI help and available arguments."),
    ("--list-functions", "List high-level script utilities and exit."),
    ("--dryrun", "Preview transformed output without posting to Nextcloud Talk."),
    ("--maxmessages=NUMBER", "Process up to NUMBER most-recent messages per channel."),
    ("--daysback=NUMBER", "Skip messages older than NUMBER days."),
]


@dataclass(frozen=True)
class BridgeJob:
    gateway_name: str
    discord_account: str
    discord_channel_id: int
    nctalk_account: str
    nctalk_token: str


@dataclass(frozen=True)
class TalkAccount:
    name: str
    server: str
    login: str
    password: str
    remote_nick_format: str


def require_readable_file(path: Path) -> None:
    if not path.exists():
        raise SystemExit(f"ERROR: missing config file: {path}")
    if not path.is_file():
        raise SystemExit(f"ERROR: config path is not a file: {path}")
    if not os.access(path, os.R_OK):
        raise SystemExit(f"ERROR: no read permission for config file: {path}")


def parse_discord_channel_id(raw_channel: str) -> int:
    value = raw_channel
    if raw_channel.startswith("ID:"):
        value = raw_channel.split(":", 1)[1]
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"Invalid Discord channel identifier: {raw_channel}") from exc


def extract_talk_accounts(config: dict) -> dict[str, TalkAccount]:
    out: dict[str, TalkAccount] = {}
    nctalk_table = config.get("nctalk", {})
    if not isinstance(nctalk_table, dict):
        raise ValueError("Invalid matterbridge.toml: [nctalk] section missing or malformed")

    for short_name, section in nctalk_table.items():
        if not isinstance(section, dict):
            continue
        server = section.get("Server")
        login = section.get("Login")
        password = section.get("Password")
        remote_nick_format = section.get("RemoteNickFormat", "[{PROTOCOL}] <{NICK}> ")
        full_name = f"nctalk.{short_name}"

        if not (server and login and password):
            raise ValueError(f"Missing Server/Login/Password for [{full_name}]")

        out[full_name] = TalkAccount(
            name=full_name,
            server=str(server),
            login=str(login),
            password=str(password),
            remote_nick_format=str(remote_nick_format),
        )

    if not out:
        raise ValueError("No usable [nctalk.*] account sections found")
    return out


def extract_discord_tokens(config: dict) -> dict[str, str]:
    out: dict[str, str] = {}
    discord_table = config.get("discord", {})
    if not isinstance(discord_table, dict):
        raise ValueError("Invalid matterbridge.toml: [discord] section missing or malformed")

    for short_name, section in discord_table.items():
        if not isinstance(section, dict):
            continue
        token = section.get("Token")
        if not token:
            raise ValueError(f"Missing Token for [discord.{short_name}]")
        out[f"discord.{short_name}"] = str(token)

    if not out:
        raise ValueError("No usable [discord.*] account sections found")
    return out


def extract_bridge_jobs(config: dict) -> list[BridgeJob]:
    gateways = config.get("gateway", [])
    if not isinstance(gateways, list):
        raise ValueError("Invalid matterbridge.toml: [[gateway]] must be an array")

    jobs: list[BridgeJob] = []

    for gateway in gateways:
        if not isinstance(gateway, dict):
            continue
        if gateway.get("enable", True) is False:
            continue

        gateway_name = str(gateway.get("name", "(unnamed-gateway)"))
        inouts = gateway.get("inout", [])
        if not isinstance(inouts, list):
            continue

        discord_pairs: list[tuple[str, int]] = []
        nctalk_pairs: list[tuple[str, str]] = []

        for item in inouts:
            if not isinstance(item, dict):
                continue
            account = item.get("account")
            channel = item.get("channel")
            if not isinstance(account, str) or not isinstance(channel, str):
                continue

            if account.startswith("discord."):
                discord_pairs.append((account, parse_discord_channel_id(channel)))
            elif account.startswith("nctalk."):
                nctalk_pairs.append((account, channel))

        for discord_account, discord_channel_id in discord_pairs:
            for nctalk_account, nctalk_token in nctalk_pairs:
                jobs.append(
                    BridgeJob(
                        gateway_name=gateway_name,
                        discord_account=discord_account,
                        discord_channel_id=discord_channel_id,
                        nctalk_account=nctalk_account,
                        nctalk_token=nctalk_token,
                    )
                )

    if not jobs:
        raise ValueError(
            "No Discord -> Nextcloud Talk bridge pairs found in enabled [[gateway]] sections"
        )

    deduped = {
        (j.discord_account, j.discord_channel_id, j.nctalk_account, j.nctalk_token): j
        for j in jobs
    }
    return list(deduped.values())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill Discord messages to Nextcloud Talk using matterbridge mappings."
    )
    parser.add_argument(
        "--list-functions",
        action="store_true",
        help="List script utilities and exit.",
    )
    parser.add_argument(
        "--dryrun",
        action="store_true",
        help="Print transformed payloads without posting to Nextcloud Talk.",
    )
    parser.add_argument(
        "--maxmessages",
        type=int,
        default=None,
        help="Maximum number of most-recent messages to read per Discord channel.",
    )
    parser.add_argument(
        "--daysback",
        type=int,
        default=None,
        help="Exclude messages older than this many days.",
    )
    return parser.parse_args()


def print_function_listing() -> None:
    print("Available utilities:")
    for flag, desc in UTILITY_FUNCTIONS:
        print(f"  {flag:<22} {desc}")


def talk_post_message(
    session: requests.Session,
    nc_base_url: str,
    talk_token: str,
    message: str,
) -> None:
    headers = {
        "OCS-APIRequest": "true",
        "Accept": "application/json",
    }
    base = nc_base_url.rstrip("/")
    urls = [
        f"{base}/ocs/v2.php/apps/spreed/api/v4/chat/{talk_token}",
        f"{base}/ocs/v2.php/apps/spreed/api/v1/chat/{talk_token}",
    ]

    last_response = None
    for url in urls:
        r = session.post(
            url,
            headers=headers,
            data={"message": message},
            timeout=30,
        )
        if r.status_code in (200, 201):
            return
        last_response = r

    raise RuntimeError(
        f"Talk POST failed on all known endpoints: HTTP {last_response.status_code}: "
        f"{last_response.text[:400]}"
    )


def talk_check_room_access(session: requests.Session, nc_base_url: str, talk_token: str) -> None:
    headers = {
        "OCS-APIRequest": "true",
        "Accept": "application/json",
    }
    base = nc_base_url.rstrip("/")

    # Preferred check: room metadata endpoint.
    room_urls = [
        f"{base}/ocs/v2.php/apps/spreed/api/v4/room/{talk_token}",
        f"{base}/ocs/v2.php/apps/spreed/api/v1/room/{talk_token}",
    ]
    r = None
    for room_url in room_urls:
        probe = session.get(room_url, headers=headers, timeout=30)
        if probe.status_code == 200:
            return
        r = probe

    # Fallback check for deployments that require chat history endpoint semantics.
    fallback = None
    chat_urls = [
        f"{base}/ocs/v2.php/apps/spreed/api/v4/chat/{talk_token}",
        f"{base}/ocs/v2.php/apps/spreed/api/v1/chat/{talk_token}",
    ]
    for chat_url in chat_urls:
        probe = session.get(
            chat_url,
            headers=headers,
            params={"lookIntoFuture": "0", "limit": "1", "lastKnownMessageId": "0"},
            timeout=30,
        )
        if probe.status_code == 200:
            return
        fallback = probe

    raise RuntimeError(
        "Talk permission check failed for room token "
        f"{talk_token}: room probe HTTP {r.status_code}, chat probe HTTP {fallback.status_code}. "
        f"Response: {fallback.text[:400]}"
    )


def format_for_talk(
    author: str,
    remote_nick_format: str,
    content: str,
    attachments: list[str],
) -> str:
    header = render_backfill_author(remote_nick_format, author)
    body = content if content.strip() else "(no text)"
    lines = [header, body]
    if attachments:
        lines.append("Attachments:")
        lines.extend([f"- {u}" for u in attachments])
    return "\n".join(lines)


def render_backfill_author(remote_nick_format: str, author: str) -> str:
    rendered = remote_nick_format.replace("{NICK}", author).strip()
    rendered = rendered.replace(f"<{author}>", author)

    closing_idx = rendered.find("]")
    if rendered.startswith("[") and closing_idx != -1:
        rendered = f"{rendered[:closing_idx]}🪏{rendered[closing_idx:]}"
    else:
        rendered = f"🪏 {rendered}" if rendered else f"🪏 {author}"

    return rendered


def check_discord_history_permission(channel: discord.abc.GuildChannel, me: discord.Member) -> None:
    perms = channel.permissions_for(me)
    missing = []
    if not perms.view_channel:
        missing.append("view_channel")
    if not perms.read_message_history:
        missing.append("read_message_history")
    if missing:
        raise RuntimeError(
            f"Missing Discord permissions for channel {channel.id}: {', '.join(missing)}"
        )


async def process_discord_account(
    discord_account: str,
    discord_token: str,
    jobs: list[BridgeJob],
    talk_accounts: dict[str, TalkAccount],
    limit: int | None,
    days_back: int | None,
    dry_run: bool,
    delay_s: float,
) -> None:
    intents = discord.Intents.default()
    intents.guilds = True
    intents.messages = True
    intents.message_content = True

    client = discord.Client(intents=intents)

    sessions: dict[str, requests.Session] = {}
    for account_name in {j.nctalk_account for j in jobs}:
        talk = talk_accounts[account_name]
        session = requests.Session()
        session.auth = (talk.login, talk.password)
        sessions[account_name] = session

    started = False

    @client.event
    async def on_ready():
        nonlocal started
        if started:
            return
        started = True

        print(f"Connected as {client.user} for {discord_account}")

        total_posted = 0
        try:
            for job in jobs:
                talk = talk_accounts[job.nctalk_account]
                session = sessions[job.nctalk_account]

                channel = await client.fetch_channel(job.discord_channel_id)
                if not hasattr(channel, "history"):
                    raise RuntimeError(
                        f"Discord channel {job.discord_channel_id} does not support message history"
                    )

                if isinstance(channel, discord.abc.GuildChannel):
                    me = channel.guild.me
                    if me is None:
                        me = await channel.guild.fetch_member(client.user.id)
                    check_discord_history_permission(channel, me)

                talk_check_room_access(session, talk.server, job.nctalk_token)

                print(
                    f"Gateway {job.gateway_name}: backfilling Discord {job.discord_channel_id} -> "
                    f"Talk {job.nctalk_account}/{job.nctalk_token}"
                )

                posted = 0
                cutoff = None
                if days_back is not None:
                    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)

                fetched_messages = []
                async for msg in channel.history(limit=limit, oldest_first=False):
                    fetched_messages.append(msg)

                if cutoff is not None:
                    fetched_messages = [m for m in fetched_messages if m.created_at >= cutoff]

                # Post chronologically, but only from the most-recent window above.
                for msg in reversed(fetched_messages):
                    if msg.type not in (discord.MessageType.default, discord.MessageType.reply):
                        continue

                    attachments = [a.url for a in msg.attachments] if msg.attachments else []
                    if not (msg.content or "").strip() and not attachments:
                        continue

                    payload = format_for_talk(
                        author=str(msg.author),
                        remote_nick_format=talk.remote_nick_format,
                        content=msg.content or "",
                        attachments=attachments,
                    )

                    if dry_run:
                        print("\n---\n" + payload)
                    else:
                        talk_post_message(session, talk.server, job.nctalk_token, payload)
                        time.sleep(delay_s)

                    posted += 1
                    total_posted += 1
                    if posted % 25 == 0:
                        print(f"Channel {job.discord_channel_id}: posted {posted} messages...")

                print(
                    f"Gateway {job.gateway_name}: done, posted {posted} messages from "
                    f"Discord {job.discord_channel_id}"
                )

        except Exception as exc:
            print(f"ERROR during backfill: {exc}", file=sys.stderr)
        finally:
            print(f"Done for {discord_account}. Total posted: {total_posted}")
            await client.close()
            for session in sessions.values():
                session.close()

    await client.start(discord_token)


async def main() -> None:
    args = parse_args()
    if args.list_functions:
        print_function_listing()
        return

    config_path = Path(os.getenv("MATTERBRIDGE_CONFIG", "matterbridge.toml")).resolve()
    require_readable_file(config_path)

    with config_path.open("rb") as f:
        config = tomllib.load(f)

    talk_accounts = extract_talk_accounts(config)
    discord_tokens = extract_discord_tokens(config)
    jobs = extract_bridge_jobs(config)

    missing_discord_accounts = sorted(
        {j.discord_account for j in jobs if j.discord_account not in discord_tokens}
    )
    if missing_discord_accounts:
        raise SystemExit(
            "ERROR: missing Discord credentials for accounts: "
            + ", ".join(missing_discord_accounts)
        )

    missing_talk_accounts = sorted({j.nctalk_account for j in jobs if j.nctalk_account not in talk_accounts})
    if missing_talk_accounts:
        raise SystemExit(
            "ERROR: missing Nextcloud Talk credentials for accounts: "
            + ", ".join(missing_talk_accounts)
        )

    env_limit = os.getenv("DISCORD_LIMIT")
    limit = args.maxmessages if args.maxmessages is not None else (int(env_limit) if env_limit else None)
    if limit is not None and limit <= 0:
        raise SystemExit("ERROR: --maxmessages must be a positive integer")
    days_back = args.daysback
    if days_back is None and os.getenv("DAYS_BACK"):
        days_back = int(os.getenv("DAYS_BACK", "0"))
    if days_back is not None and days_back < 0:
        raise SystemExit("ERROR: --daysback must be zero or a positive integer")
    dry_run = args.dryrun or (os.getenv("DRY_RUN", "0") == "1")
    delay_s = float(os.getenv("POST_DELAY_SECONDS", "0.35"))

    print(f"Using config: {config_path}")
    print(f"Found {len(jobs)} Discord -> Nextcloud Talk mapping(s)")
    limit_display = limit if limit is not None else "none"
    print(f"Limit (most recent): {limit_display}   days_back: {days_back}   dry_run: {dry_run}")

    jobs_by_discord: dict[str, list[BridgeJob]] = {}
    for job in jobs:
        jobs_by_discord.setdefault(job.discord_account, []).append(job)

    for discord_account, account_jobs in jobs_by_discord.items():
        await process_discord_account(
            discord_account=discord_account,
            discord_token=discord_tokens[discord_account],
            jobs=account_jobs,
            talk_accounts=talk_accounts,
            limit=limit,
            days_back=days_back,
            dry_run=dry_run,
            delay_s=delay_s,
        )


if __name__ == "__main__":
    asyncio.run(main())
