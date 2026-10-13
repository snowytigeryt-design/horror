# The Hollow Ward (co-op)

First-person horror. Find five fuses, restore the power, leave through the east door. Solo, or 2 to 4 players
in a room. Everything is one small Node server plus one HTML file.

## Run it locally

    npm install
    npm start

Open http://localhost:3000. Open it in a second window to test a room.

## Deploy on Render

1. Push this folder to a GitHub repo.
2. Render dashboard: New > Web Service > pick the repo.
3. Runtime `Node`, Build Command `npm install`, Start Command `npm start`.
4. Health Check Path `/healthz`.
5. Deploy. Render sets `PORT` for you, and WebSockets work on the same URL with no extra setup.

Or use the included `render.yaml` as a Blueprint.

### Keeping player records

NeDB stores data in two files (`players.db`, `games.db`) inside `DATA_DIR` (default `./data`).
On Render the normal filesystem is erased on every deploy and restart, so records reset.
To keep them: add a Persistent Disk (paid instances), mount it at `/var/data`, and set the environment
variable `DATA_DIR=/var/data`.

Rooms live in memory only. Run a single instance (do not scale to several), because both the rooms and the
NeDB files belong to one process. On the free plan the service sleeps after about 15 minutes idle, which
ends any open rooms.

## If something looks wrong on Render

- **Pinned to Node 22.** The original `nedb` package breaks on Node 23 and newer, so `package.json` asks for
  `22.x`. If you ever see database errors in the Render logs, check the Node version first. A maintained
  drop-in alternative is `@seald-io/nedb`.
- **The server survives a database failure.** Players can still join, start and play. Records and the
  leaderboard are just empty until the database works again. Errors are printed as `[db]` or `[uncaught]`.
- **three.js is served by the server** from the `three` package (`/vendor/three.min.js`), with the cdnjs copy
  as a fallback only. That avoids browser "Tracking Prevention" warnings about the CDN.
- A `502` for a few seconds after a deploy, or after the free plan wakes up, is Render starting the app.

## How multiplayer works

- **Player id:** the browser makes a random UUID on first visit and keeps it in localStorage. The server
  keys a player record on it (name, escapes, deaths, fuses, best time). There are no accounts. The UUID is
  the only credential, so it is never sent to other players; they get a short per-room id instead. Clearing
  site data makes you a new player.
- **Rooms:** Create room gives a 4 letter code. Quick match drops you into any open public room. Anyone can
  join a run in progress with the code.
- **Same world:** the server picks a seed when the host starts, and every browser builds the same maze,
  fuses and lockers from it.
- **Shared:** fuses (one player takes it, it is gone for everyone), the main stalker, and player positions.
  The room host's browser runs the stalker's AI and the server relays it. If the host leaves, or their tab
  goes to the background and stops updating for 4 seconds, the role passes to another player automatically.
- **Personal:** the jumpscares, crawlers, brutes and wall scares are rolled per player, so each player is
  haunted separately.
- **Death:** a caught player can "Rise again" at the entrance. Fuses already taken stay taken.
- **Quick calls:** keys 1, 2, 3 send "Over here!", "Run!", "Stay quiet...".
- **No pausing** in co-op, because the world keeps running for everyone.

## Server messages

All JSON over `/ws`. Client to server: `hello create quick join leave start p mon fuse kill died respawn
escaped say ping`. Server to client: `welcome room start p mon fuse kill died respawn escaped say left end
error pong`. Moves, fuses and escapes are checked for shape and sanity, but a determined cheater can still
lie about their own position. This is a friends game, not a competitive one.

## Endpoints

- `GET /` the game
- `GET /healthz` returns `ok`
- `GET /api/leaderboard` top 10 by escapes

## Connection drops and reconnecting

- If a player's connection drops, the server keeps their seat in the room for 60 seconds. The game client
  automatically reconnects (retrying for about 45 seconds) and picks up where it left off, including any
  fuses taken in the meantime. Only after that does the player go back to the title screen.
- Heartbeat: the server pings every 20 seconds and only drops a socket after 2 missed pings.
- If the same player id joins from a second tab while in a room, the session moves to the new tab and the
  old tab is told it was signed in elsewhere (it does not fight to reconnect).
