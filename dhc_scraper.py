#!/usr/bin/env python3
"""Delhi High Court judgments scraper/downloader (date-wise)."""

from __future__ import annotations

import argparse
import json
import logging
import random
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import pandas as pd
from playwright.sync_api import BrowserContext, Page, TimeoutError, sync_playwright

BASE_URL = "https://delhihighcourt.nic.in/app/judgement-dates-wise"


@dataclass
class ScrapeRow:
    s_no: str
    case_no: str
    judgment_date: str
    party: str
    corrigendum: str
    upload_date: str
    remark: str
    pdf_url: str
    pdf_file_path: str
    download_status: str
    error_message: str
    page_number: int
    scraped_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "S.No.": self.s_no,
            "Case No.": self.case_no,
            "Date of Judgment/Order": self.judgment_date,
            "Party": self.party,
            "Corrigendum": self.corrigendum,
            "Date of Uploading": self.upload_date,
            "Remark": self.remark,
            "pdf_url": self.pdf_url,
            "pdf_file_path": self.pdf_file_path,
            "download_status": self.download_status,
            "error_message": self.error_message,
            "page_number": self.page_number,
            "scraped_at": self.scraped_at,
        }


class DHCScraper:
    def __init__(
        self,
        from_date: str,
        to_date: str,
        outdir: Path,
        headless: bool,
        max_pages: int | None,
        delay: float,
        retries: int,
        resume: bool,
    ) -> None:
        self.from_date = from_date
        self.to_date = to_date
        self.outdir = outdir
        self.headless = headless
        self.max_pages = max_pages
        self.delay = delay
        self.retries = retries
        self.resume = resume

        self.pdf_dir = self.outdir / "pdfs"
        self.log_dir = self.outdir / "logs"
        self.results_dir = self.outdir / "results"

        self.run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file = self.log_dir / f"run_{self.run_timestamp}.log"
        self.excel_file = self.results_dir / f"dhc_judgments_{self.from_date}_{self.to_date}.xlsx"
        self.checkpoint_file = self.results_dir / "checkpoint.json"

        self.rows: list[dict[str, Any]] = []
        self.seen_row_keys: set[str] = set()
        self.used_filenames: set[str] = set()
        self.checkpoint: dict[str, Any] = {
            "from_date": self.from_date,
            "to_date": self.to_date,
            "processed_pdf_urls": {},
            "rows": [],
            "metadata": {
                "last_page": 0,
                "total_expected": None,
                "updated_at": None,
            },
        }

    def setup(self) -> None:
        for folder in (self.pdf_dir, self.log_dir, self.results_dir):
            folder.mkdir(parents=True, exist_ok=True)

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[logging.FileHandler(self.log_file, encoding="utf-8"), logging.StreamHandler(sys.stdout)],
        )
        logging.info("Starting run for %s to %s", self.from_date, self.to_date)
        logging.info("Output directory: %s", self.outdir.resolve())

        if self.resume and self.checkpoint_file.exists():
            self._load_checkpoint()

    def _load_checkpoint(self) -> None:
        try:
            saved = json.loads(self.checkpoint_file.read_text(encoding="utf-8"))
            if saved.get("from_date") != self.from_date or saved.get("to_date") != self.to_date:
                logging.warning("Checkpoint date range does not match current run; ignoring checkpoint.")
                return
            self.checkpoint = saved
            self.rows = saved.get("rows", [])
            for row in self.rows:
                self.seen_row_keys.add(self._row_key(row))
                path_text = row.get("pdf_file_path", "")
                if path_text:
                    self.used_filenames.add(Path(path_text).name)
            logging.info(
                "Loaded checkpoint: %s rows, %s processed PDFs",
                len(self.rows),
                len(self.checkpoint.get("processed_pdf_urls", {})),
            )
        except Exception as exc:  # pragma: no cover
            logging.exception("Failed to load checkpoint: %s", exc)

    def _save_checkpoint(self, last_page: int, total_expected: int | None) -> None:
        self.checkpoint["rows"] = self.rows
        self.checkpoint["metadata"] = {
            "last_page": last_page,
            "total_expected": total_expected,
            "updated_at": datetime.now().isoformat(),
        }
        self.checkpoint_file.write_text(json.dumps(self.checkpoint, ensure_ascii=False, indent=2), encoding="utf-8")

    def _write_excel(self) -> None:
        if not self.rows:
            return
        df = pd.DataFrame(self.rows)
        df.to_excel(self.excel_file, index=False)

    @staticmethod
    def _validate_date(date_str: str) -> str:
        datetime.strptime(date_str, "%d-%m-%Y")
        return date_str

    @staticmethod
    def _clean_text(value: str) -> str:
        return re.sub(r"\s+", " ", value or "").strip()

    @staticmethod
    def _sanitize_filename(value: str, max_len: int = 150) -> str:
        cleaned = re.sub(r"[\\/:*?\"<>|]", "_", value)
        cleaned = re.sub(r"\s+", "_", cleaned).strip("._")
        return cleaned[:max_len] or "unknown_case"

    def _build_filename(self, case_no: str, judgment_date: str) -> str:
        base = self._sanitize_filename(case_no) if case_no else "unknown_case"
        if base == "unknown_case" and judgment_date:
            base = f"unknown_case_{self._sanitize_filename(judgment_date)}"

        candidate = f"{base}.pdf"
        counter = 2
        while candidate in self.used_filenames:
            candidate = f"{base}_{counter}.pdf"
            counter += 1
        self.used_filenames.add(candidate)
        return candidate

    @staticmethod
    def _row_key(row: dict[str, Any]) -> str:
        return "|".join(
            [
                str(row.get("Case No.", "")),
                str(row.get("Date of Judgment/Order", "")),
                str(row.get("Party", "")),
                str(row.get("pdf_url", "")),
            ]
        )

    def _polite_sleep(self) -> None:
        jitter = random.uniform(max(0.1, self.delay * 0.6), max(0.2, self.delay * 1.4))
        time.sleep(jitter)

    def _download_pdf(self, context: BrowserContext, pdf_url: str, target: Path) -> tuple[str, str]:
        if target.exists() and target.stat().st_size > 0:
            return "SKIPPED", "Already exists"

        last_error = ""
        for attempt in range(1, self.retries + 1):
            try:
                response = context.request.get(pdf_url, timeout=60_000)
                if response.status != 200:
                    raise RuntimeError(f"HTTP {response.status}")

                content = response.body()
                if not content:
                    raise RuntimeError("Empty response body")

                target.write_bytes(content)
                if target.stat().st_size == 0:
                    raise RuntimeError("Downloaded file is empty")
                return "SUCCESS", ""
            except Exception as exc:  # pragma: no cover
                last_error = f"Attempt {attempt}/{self.retries}: {exc}"
                logging.warning("Download failed for %s: %s", pdf_url, last_error)
                if attempt < self.retries:
                    time.sleep(2 ** (attempt - 1))
        return "FAILED", last_error

    def _find_results_table(self, page: Page):
        table = page.locator("table:has(th:has-text('Case No.'))").first
        table.wait_for(state="visible", timeout=90_000)
        return table

    def _extract_total_records(self, page: Page) -> int | None:
        text = page.inner_text("body")
        match = re.search(r"Total\s+No\s+of\s+Records\s*:\s*(\d+)", text, flags=re.I)
        if match:
            return int(match.group(1))
        return None

    def _current_page_number(self, page: Page, fallback: int) -> int:
        active = page.locator(".pagination .active").first
        if active.count() > 0:
            txt = self._clean_text(active.inner_text())
            if txt.isdigit():
                return int(txt)
        return fallback

    def _parse_table_rows(self, page: Page, page_number: int, context: BrowserContext) -> int:
        table = self._find_results_table(page)
        body_rows = table.locator("tbody tr")
        row_count = body_rows.count()
        logging.info("Page %s has %s rows", page_number, row_count)

        new_rows = 0
        for i in range(row_count):
            tr = body_rows.nth(i)
            tds = tr.locator("td")
            if tds.count() < 7:
                continue

            cell_values = [self._clean_text(tds.nth(col).inner_text()) for col in range(min(7, tds.count()))]
            while len(cell_values) < 7:
                cell_values.append("")

            links = tr.locator("a")
            pdf_url = ""
            for j in range(links.count()):
                href = links.nth(j).get_attribute("href") or ""
                label = self._clean_text(links.nth(j).inner_text()).lower()
                if "pdf" in label or href.lower().endswith(".pdf"):
                    pdf_url = urljoin(page.url, href)
                    break

            case_no = self._clean_text(cell_values[1])
            judgment_date = self._clean_text(cell_values[2])

            scraped_at = datetime.now().isoformat(timespec="seconds")
            row_payload = {
                "S.No.": cell_values[0],
                "Case No.": case_no,
                "Date of Judgment/Order": judgment_date,
                "Party": self._clean_text(cell_values[3]),
                "Corrigendum": self._clean_text(cell_values[4]),
                "Date of Uploading": self._clean_text(cell_values[5]),
                "Remark": self._clean_text(cell_values[6]),
                "pdf_url": pdf_url,
                "pdf_file_path": "",
                "download_status": "FAILED",
                "error_message": "PDF link not found",
                "page_number": page_number,
                "scraped_at": scraped_at,
            }

            row_key = self._row_key(row_payload)
            if row_key in self.seen_row_keys:
                continue

            if pdf_url:
                previous_status = self.checkpoint.get("processed_pdf_urls", {}).get(pdf_url)
                if previous_status:
                    row_payload["pdf_file_path"] = previous_status.get("pdf_file_path", "")
                    row_payload["download_status"] = previous_status.get("download_status", "SKIPPED")
                    row_payload["error_message"] = previous_status.get("error_message", "")
                else:
                    filename = self._build_filename(case_no, judgment_date)
                    target_path = self.pdf_dir / filename
                    status, err = self._download_pdf(context, pdf_url, target_path)
                    row_payload["pdf_file_path"] = str(target_path.resolve())
                    row_payload["download_status"] = status
                    row_payload["error_message"] = err
                    self.checkpoint["processed_pdf_urls"][pdf_url] = {
                        "pdf_file_path": row_payload["pdf_file_path"],
                        "download_status": status,
                        "error_message": err,
                    }
                    self._polite_sleep()

            self.rows.append(row_payload)
            self.seen_row_keys.add(row_key)
            new_rows += 1

        return new_rows

    def _go_to_next_page(self, page: Page) -> bool:
        next_link = page.get_by_role("link", name=re.compile(r"^Next$", re.I)).first
        if next_link.count() == 0:
            return False

        classes = (next_link.get_attribute("class") or "").lower()
        parent_classes = (next_link.locator("xpath=..").get_attribute("class") or "").lower()
        if "disabled" in classes or "disabled" in parent_classes:
            return False

        current_url = page.url
        next_link.click()
        try:
            page.wait_for_load_state("networkidle", timeout=30_000)
        except TimeoutError:
            logging.info("networkidle timeout after Next click; continuing")
        page.wait_for_timeout(800)
        return page.url != current_url or page.locator("table tbody tr").count() > 0

    def run(self) -> None:
        self.setup()
        expected_total: int | None = self.checkpoint.get("metadata", {}).get("total_expected")
        page_loop = max(1, int(self.checkpoint.get("metadata", {}).get("last_page", 0) or 0) + 1)

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context(accept_downloads=True)
            page = context.new_page()

            logging.info("Navigating to %s", BASE_URL)
            page.goto(BASE_URL, wait_until="domcontentloaded", timeout=90_000)

            page.locator("input[name='from_date'], input#from_date").first.fill(self.from_date)
            page.locator("input[name='to_date'], input#to_date").first.fill(self.to_date)

            print("\nCAPTCHA step required.")
            print("1) Solve CAPTCHA manually in the browser window.")
            print("2) Submit the form in the browser.")
            input("Press ENTER here after results are loaded in browser... ")

            self._find_results_table(page)
            expected_total = expected_total or self._extract_total_records(page)
            logging.info("Expected total records: %s", expected_total)

            current_page = 1
            no_new_pages = 0

            while True:
                if self.max_pages and current_page > self.max_pages:
                    logging.info("Reached max-pages=%s; stopping", self.max_pages)
                    break

                page_number = self._current_page_number(page, current_page)
                if page_number < page_loop:
                    if not self._go_to_next_page(page):
                        break
                    current_page += 1
                    continue

                new_rows = self._parse_table_rows(page, page_number, context)
                if new_rows == 0:
                    no_new_pages += 1
                else:
                    no_new_pages = 0

                self._write_excel()
                self._save_checkpoint(last_page=page_number, total_expected=expected_total)

                if expected_total is not None and len(self.rows) >= expected_total:
                    logging.info("Collected rows (%s) reached expected total (%s)", len(self.rows), expected_total)
                    break
                if no_new_pages >= 2:
                    logging.warning("No new rows across two consecutive pages; stopping to avoid loop")
                    break

                moved = self._go_to_next_page(page)
                if not moved:
                    logging.info("No next page available.")
                    break

                current_page += 1
                self._polite_sleep()

            browser.close()

        self._write_excel()
        self._save_checkpoint(last_page=current_page, total_expected=expected_total)
        self._print_summary(expected_total)

    def _print_summary(self, expected_total: int | None) -> None:
        success = sum(1 for r in self.rows if r.get("download_status") == "SUCCESS")
        failed = sum(1 for r in self.rows if r.get("download_status") == "FAILED")
        skipped = sum(1 for r in self.rows if r.get("download_status") == "SKIPPED")
        summary = {
            "total_records_expected": expected_total,
            "total_rows_scraped": len(self.rows),
            "pdf_success": success,
            "pdf_failed": failed,
            "pdf_skipped": skipped,
            "excel_path": str(self.excel_file.resolve()),
            "pdf_folder": str(self.pdf_dir.resolve()),
        }
        logging.info("Run summary: %s", summary)
        print("\nFinal summary")
        for k, v in summary.items():
            print(f"- {k}: {v}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Delhi High Court date-wise judgments scraper")
    parser.add_argument("--from-date", required=True, help="From date in DD-MM-YYYY format")
    parser.add_argument("--to-date", required=True, help="To date in DD-MM-YYYY format")
    parser.add_argument("--outdir", default="output", help="Output directory (default: output)")
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint if available")
    parser.add_argument("--headless", action="store_true", help="Run browser in headless mode (default: false)")
    parser.add_argument("--max-pages", type=int, default=None, help="Limit pages to scrape (for testing)")
    parser.add_argument("--delay", type=float, default=0.6, help="Base delay between requests (seconds)")
    parser.add_argument("--retries", type=int, default=3, help="Retries for each PDF download")
    args = parser.parse_args()

    try:
        DHCScraper._validate_date(args.from_date)
        DHCScraper._validate_date(args.to_date)
    except ValueError as exc:
        raise SystemExit(f"Invalid date format. Use DD-MM-YYYY. Details: {exc}") from exc

    return args


def main() -> None:
    args = parse_args()
    scraper = DHCScraper(
        from_date=args.from_date,
        to_date=args.to_date,
        outdir=Path(args.outdir),
        headless=args.headless,
        max_pages=args.max_pages,
        delay=args.delay,
        retries=args.retries,
        resume=args.resume,
    )
    scraper.run()


if __name__ == "__main__":
    main()
