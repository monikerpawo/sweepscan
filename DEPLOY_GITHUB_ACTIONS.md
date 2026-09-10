# Running SweepScan on GitHub Actions (zero devices, free)

This runs the scanner on GitHub's servers on a schedule, so your laptop
never has to be on. Each run is a fresh, temporary Linux machine that
GitHub spins up for you, scans once, sends any alerts to Telegram, saves
its memory of what it's already seen back into your repo, and shuts down.

**Read the limitation below before you set this up** - it decides whether
this actually covers your real trading, or just a crypto proxy of it.

## The one real limitation: this can't reach your Deriv charts directly

SweepScan's Deriv/MT5 feed only works because MetaTrader 5 (a Windows
program) is running on your computer feeding it prices. GitHub's runners
are headless Linux machines with no MT5 installed and no display - so a
scheduled run here **cannot watch your actual Volatility Index charts**.

What it *can* do unattended, for free, forever: scan the built-in public
feed (crypto pairs like BTC/USDT, ETH/USDT, plus whatever else that feed
supports) using the exact same sweep/displacement/FVG/retest logic as your
Deriv model - which is what the backtest you asked for was already doing,
for the same reason.

If you want the real Volatility Index charts watched around the clock, you
still need something with MT5 installed and running - your laptop, a cheap
Windows VPS (~$4-6/mo, several hosts offer these), or a Raspberry Pi
running Wine/a lightweight MT5 setup. GitHub Actions is the free option for
"watch crypto with this same model, no device needed" - not a way around
needing MT5 for Deriv itself. Worth doing both: GitHub Actions for crypto
so it costs you nothing, and one of the device options later if you want
the actual Deriv symbols covered too.

## What you're setting up

- A **private** GitHub repo containing `sweepscan_app.py`, a `config.json`
  (symbols/timeframes/detector settings - no secrets in it), and the
  workflow file in this folder.
- Two **repo secrets** holding your real Telegram bot token and chat ID -
  never committed to the code.
- A schedule (every 15 minutes by default) that runs one scan, and commits
  a small `state.json` file back to the repo so the next run remembers
  what it already alerted on.

## Step-by-step

### 1. Create the repo

1. Go to github.com -> **New repository**.
2. Name it anything (e.g. `sweepscan`). Set it to **Private**.
3. Don't add a README/.gitignore/license - just create it empty.

### 2. Upload the files

Fastest way if you're not comfortable with git: on the repo's page, click
**Add file -> Upload files**, then drag in all of these, keeping the
folder structure:

```
sweepscan_app.py
config.json
.github/workflows/sweepscan.yml
```

(`.github/workflows/sweepscan.yml` must be in that exact nested path - if
GitHub's uploader flattens it, create the folders first by typing
`.github/workflows/sweepscan.yml` as the filename when you add the file.)

Commit directly to `main`.

If you'd rather use git from a terminal:
```
git clone <your-new-repo-url>
cd sweepscan
# copy sweepscan_app.py, config.json, and .github/ into this folder
git add .
git commit -m "Initial SweepScan setup"
git push
```

### 3. Add your Telegram secrets

Your bot token and chat ID must **never** go in `config.json` - GitHub
Secrets keep them out of the repo entirely, even though the repo is
private.

1. Repo page -> **Settings** -> **Secrets and variables** -> **Actions**.
2. **New repository secret**:
   - Name: `SWEEPSCAN_TG_TOKEN` -> value: your bot token (from BotFather).
   - **New repository secret** again:
   - Name: `SWEEPSCAN_TG_CHAT_ID` -> value: your chat ID.

### 4. Turn on Actions and let it run

1. Repo -> **Actions** tab -> if prompted, click **"I understand my
   workflows, go ahead and enable them"**.
2. You'll see **SweepScan** in the left sidebar. Click it, then
   **Run workflow** (top right) to trigger the very first run by hand
   instead of waiting for the schedule.
3. **The first run is silent on purpose** - it "primes" against whatever
   setups already exist in recent history so you don't get flooded with
   alerts for old, already-played-out setups the moment this goes live.
   Open the run's log (click into it, then the "Run SweepScan" step) and
   you should see something like:
   `First run - primed silently with N existing setup(s). State saved to state.json.`
4. Check the repo's file list - a new `state.json` should now exist,
   committed automatically by the workflow. That's its memory.
5. From here on, every scheduled run (every 15 minutes) compares fresh
   data against that memory and only alerts on genuinely new setups /
   entries, sending them to your Telegram exactly like running it on your
   laptop would.

### 5. Adjust the schedule

Edit the `cron:` line in `.github/workflows/sweepscan.yml`:

```yaml
- cron: "*/15 * * * *"   # every 15 minutes (default)
- cron: "*/5 * * * *"    # every 5 minutes
- cron: "0 * * * *"      # once an hour, on the hour
```

All times are UTC. GitHub's scheduler is best-effort and often runs a few
minutes late during busy periods - fine for a swing-style setup like this,
but don't rely on it for second-precision timing.

### 6. Free-tier limits (you will not come close to hitting these)

Private repos on a free GitHub account get **2,000 Actions minutes/month**.
Each SweepScan run finishes in well under a minute (it's pure Python,
stdlib only - no dependencies to install). At the default 15-minute
schedule that's ~2,880 runs/month; even at 1 minute each that's far inside
the free allowance. Every 5 minutes (~8,640 runs/month) still comfortably
fits. If you ever add many more symbols/timeframes and runs start taking
longer, the Actions tab shows exact minutes used under **Settings ->
Billing**.

### 7. Changing settings later

Edit `config.json` in the repo (symbols, timeframes, sensitivity,
`entry_retracement_pct`, breakout on/off, etc.) and commit - the very next
scheduled run picks it up automatically. No redeploy step.

### 8. How to tell it's actually working

- **Actions tab**: every run is logged there with a green check or red X.
  Click any run to see the console output, same as running it locally.
- **state.json growing/changing**: commits to that file in the repo's
  history are proof runs are happening and finding things.
- **A real Telegram alert**: the real test. Since the first run is silent
  by design, either wait for a genuine new setup, or temporarily delete
  `state.json` from the repo and re-run by hand - that forces a fresh
  "priming" pass you can watch in the log, though it won't send you a
  Telegram message (priming never does).

### Troubleshooting

- **Workflow doesn't appear under Actions**: make sure the YAML file is at
  exactly `.github/workflows/sweepscan.yml` (note the leading dot on
  `.github`) and that Actions is enabled for the repo (step 4.1).
- **Run fails with a Telegram error in the log**: double check the two
  secret names are spelled exactly `SWEEPSCAN_TG_TOKEN` and
  `SWEEPSCAN_TG_CHAT_ID`, and that you've messaged your bot at least once
  (Telegram chat IDs only become valid after the first message to the bot).
- **"Data source failed" / no candles**: the public feed occasionally rate
  limits or has an outage; a run or two failing doesn't lose any state -
  the next scheduled run just tries again against the same `state.json`.
- **Push from the workflow is rejected**: this happens if `contents: write`
  permission got removed from the workflow file, or branch protection
  rules block pushes to `main` from Actions - either loosen branch
  protection for the `sweepscan-bot` committer or push state to a
  dedicated branch instead (ask if you want that variant).
