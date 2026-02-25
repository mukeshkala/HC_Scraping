# Delhi High Court Judgments Scraper (Date-wise)

This project provides a robust Playwright-based scraper/downloader for:

- https://delhihighcourt.nic.in/app/judgement-dates-wise

It scrapes all rows across paginated results for a date range, downloads only PDF files, and exports a full Excel report.

## Features

- Human-in-the-loop CAPTCHA flow (no automatic CAPTCHA solving).
- Scrapes all table columns and pagination.
- Downloads only PDF links into `output/pdfs/`.
- Resumable runs via `checkpoint.json`.
- Retry logic with exponential backoff for PDF downloads.
- Incremental Excel output with download metadata.
- Detailed logging to `output/logs/run_<timestamp>.log`.

## Output Structure

```text
output/
  pdfs/
  logs/
  results/
    dhc_judgments_<from>_<to>.xlsx
    checkpoint.json
```

## Setup (macOS)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
playwright install chromium
```

## Usage

Basic:

```bash
python dhc_scraper.py --from-date 01-02-2026 --to-date 25-02-2026 --outdir output
```

Resume from checkpoint:

```bash
python dhc_scraper.py --from-date 01-02-2026 --to-date 25-02-2026 --outdir output --resume
```

Testing a short run:

```bash
python dhc_scraper.py --from-date 01-02-2026 --to-date 25-02-2026 --max-pages 2 --delay 0.7 --retries 3
```

Optional arguments:

- `--headless`: Run browser headless (default is headful for CAPTCHA visibility).
- `--max-pages N`: Limit number of pages (useful for test runs).
- `--delay SECONDS`: Base polite delay between actions/downloads (default `0.6`).
- `--retries N`: Number of retries per PDF download (default `3`).
- `--resume`: Continue using `output/results/checkpoint.json`.

## CAPTCHA Flow

1. Script opens page and fills From/To dates.
2. You manually read/solve CAPTCHA in browser.
3. You submit the search from browser.
4. Return to terminal and press Enter when results are visible.

## Notes

- `_token` and related session params are never hardcoded.
- Pagination is handled through UI controls (`Next`) to preserve session behavior.
- Existing non-empty PDF files are not re-downloaded and are marked `SKIPPED`.
- If `Case No.` is missing/duplicate, filenames are auto-suffixed to keep them unique.
