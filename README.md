# YouTube transcript search

Search what was said in your favorite YouTube channels, and jump straight to the moment it was said.

Add a channel, choose how far back to go, and it downloads the captions of the channel's videos
and livestreams into a small local database. Then search them like a search engine: every match
shows the line where it was said, with a timestamp that opens the video at that point.

Runs in one Docker container. Built for a Raspberry Pi, and works on any Linux machine with Docker.

## Features

- **Search**: case-insensitive, all words or exact phrase, filter by channel and date, newest
  first or most matches. Search links can be bookmarked.
- **Timestamp links**: each match opens the YouTube video a few seconds before the words.
- **Incremental fetching**: videos you already have are never downloaded again. Fetch the last
  month today and the last month again next week, and only the new week is downloaded. Ask for
  more months later, and only the older part is downloaded.
- **Coverage at a glance**: each channel shows the date range you have, video count, storage
  used, and videos still waiting for captions.
- **Livestreams included, Shorts excluded.** Streams still in progress are left for a later run;
  finished streams whose captions aren't ready yet are retried every night for a week.
- **Nightly update** (03:00 by default): updates yt-dlp, fetches each channel's new videos, and
  retries videos that were waiting for captions.
- **Polite to YouTube**: one fetch at a time, a pause between videos, only one caption track
  downloaded per video, and it backs off when YouTube says to slow down.

Captions are the creator's English captions when there are some, otherwise YouTube's
auto-generated ones (no punctuation, the odd misheard word, still very searchable).

## Install

You need Docker with the Compose plugin.

```bash
git clone https://github.com/Lv2hack/yt-transcript-search.git
cd yt-transcript-search
mkdir data
docker compose up -d --build
```

Open `http://<your machine>:8002`, add a channel (an `@handle` or channel link), and pick how far
back to go. The first fetch of a busy channel can take an hour; it runs in the background.

The container runs as user `1000:1000`, so `data/` must be writable by that user. That's the
first user on most Linux systems, including Raspberry Pi OS and Ubuntu. Otherwise change
`user:` in `docker-compose.yml` to your own `id -u`:`id -g`.

To update: `git pull && docker compose up -d --build`.

## Security

**There is no login.** Anyone who can reach the port can search, add and remove channels, and
start downloads. Keep it on your home network, or put it behind a reverse proxy with
authentication (Caddy's `basic_auth`, for example) before exposing it to the internet.

## Behind your own web server

The page calls the API at `/api/yt/`, so you can serve `web/index.html` as part of your own site
and pass `/api/yt/` to the container. With nginx:

```nginx
location /api/yt/ {
    proxy_pass http://yt-search:8002;
}
```

## Settings

Environment variables, in `docker-compose.yml`:

| Variable | Default | |
|---|---|---|
| `NIGHTLY_AT` | `03:00` | Local time of the nightly update |
| `PENDING_DAYS` | `7` | How long to keep retrying videos without captions |
| `VIDEO_PAUSE` | `5` | Seconds between videos |
| `PORT` | `8002` | Web port inside the container |

## How it works

- **[yt-dlp](https://github.com/yt-dlp/yt-dlp)** lists the channel's Videos and Live tabs and
  downloads each new video's details and one English caption track. Deno and curl_cffi are
  installed alongside it, as YouTube now needs them.
- **SQLite full-text search (FTS5)** stores the captions in passages of about 15 seconds. A search
  matches within one passage, so a phrase split across two passages can be missed.
- **[ytsearch.py](ytsearch.py)** is a single Python file using only the standard library: the
  fetcher, the nightly scheduler, and the web server for the page and API.
- Data lives in `data/yt.db`. A few months of several channels takes tens of MB.

If YouTube blocks downloads for a while (HTTP 429, or "Sign in to confirm you're not a bot"), the
fetch stops without marking anything, the channel shows the error, and the nightly update carries on
where it left off.

## License

MIT. Downloading content from YouTube is subject to YouTube's Terms of Service; use this for your
own personal searching.
