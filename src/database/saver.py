import json
import time
import re
from unidecode import unidecode
from os import environ
from pathlib import Path
from threading import Thread, RLock
from multiprocessing import Queue
from urllib.parse import unquote
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

ONEDRIVE_SAVE_DIR = Path(rf"{environ.get('ONEDRIVE_SAVE_DIR', 'outputs/legislation')}")
ERROR_LOG_DIR = rf"{environ.get('ERROR_LOG_DIR', 'logs/legislation')}"

print(f"Default saving to ONEDRIVE_SAVE_DIR: {ONEDRIVE_SAVE_DIR}")
print(f"Default saving to ERROR_LOG_DIR: {ERROR_LOG_DIR}")


class OneDriveSaver(Thread):
    """Background thread to save data to txt files in OneDrive folder"""

    def __init__(
        self,
        queue: Queue,
        error_queue: Queue,
        save_dir: Path = ONEDRIVE_SAVE_DIR,
        error_log_dir: str = ERROR_LOG_DIR,
        max_path_length: int = 225,  # sinology max path length
        buffer_size: int = 100,
    ):
        super().__init__(daemon=True)
        self.queue = queue
        self.error_queue = error_queue
        self.save_dir = save_dir
        self.error_log_dir = error_log_dir
        self.max_path_length = max_path_length
        self.buffer_size = buffer_size
        self.buffer = []
        self.format_regex_1 = re.compile(r"[\s]+")
        self.format_regex_2 = re.compile(r"[^\w\s-]")
        self.lock = RLock()
        self.running = True
        self.last_year = None
        self._set_last_year()
        print(f"Saving to {save_dir}")
        print(f"Saving errors to {error_log_dir}")

    def _set_last_year(self):
        """Set the last year that was saved (always the year before the current year in save_dir, to account for some possible delay in saving)"""
        save_dir = Path(self.save_dir)

        if not save_dir.exists():
            self.last_year = None
            return

        years = []
        for year_dir in save_dir.iterdir():
            if year_dir.is_dir() and year_dir.name.isdigit():
                years.append(int(year_dir.name))

        self.last_year = max(years) - 1 if years else None

    def run(self):
        while self.running:
            if self.queue.empty() and self.error_queue.empty():
                time.sleep(3)
                continue

            if not self.queue.empty():
                data = self.queue.get()
                self.save(data)

            if not self.error_queue.empty():
                data = self.error_queue.get()
                self.save_error(data)

        # get all remaining data in queue
        print(f"Saving remaining {self.queue.qsize()} data in queue")
        progress = tqdm(total=self.queue.qsize())
        while not self.queue.empty():
            data = self.queue.get()
            self.save(data)
            progress.update(1)

        self.flush_buffer()

        print(
            f"{self.__class__.__name__} stopped since queue is empty and running is {self.running}"
        )

    def save(self, data: dict):
        """Add data to buffer and flush if buffer is full or year changes."""
        with self.lock:
            if self.buffer and data.get("year") != self.buffer[-1].get("year"):
                self.flush_buffer()

            # check for duplicates before appending
            if data.get("document_url") not in {
                item.get("document_url") for item in self.buffer
            }:
                self.buffer.append(data)

            if len(self.buffer) >= self.buffer_size:
                self.flush_buffer()

    def flush_buffer(self):
        """Save buffered data to json files, grouped by year."""
        with self.lock:
            if not self.buffer:
                return

            print(f"Flushing buffer with {len(self.buffer)} items.")
            data_by_year = {}
            for item in self.buffer:
                year = item.get("year")
                if year:
                    if year not in data_by_year:
                        data_by_year[year] = []
                    data_by_year[year].append(item)

            for year, items in data_by_year.items():
                file_path = None
                try:
                    save_dir = Path(self.save_dir)
                    year_dir = save_dir / str(year)
                    year_dir.mkdir(parents=True, exist_ok=True)
                    file_path = year_dir / "data.json"

                    existing_data = []
                    if file_path.exists():
                        try:
                            with open(file_path, "r", encoding="utf-8") as f:
                                existing_data = json.load(f)
                        except (json.JSONDecodeError, FileNotFoundError):
                            existing_data = []

                    # overwrite existing data with new items (making sure no duplicates by removing any existing items with same document_url)
                    existing_urls = {item.get("document_url") for item in existing_data}
                    new_items = []
                    for item in items:
                        if item.get("document_url") in existing_urls:
                            # remove existing item with same document_url
                            existing_data = [
                                e_item
                                for e_item in existing_data
                                if e_item.get("document_url")
                                != item.get("document_url")
                            ]
                        new_items.append(item)

                    existing_data.extend(new_items)

                    with open(file_path, "w", encoding="utf-8") as f:
                        json.dump(existing_data, f, ensure_ascii=False, indent=2)

                except Exception as e:
                    error_msg = f"Error saving {len(items)} items for year {year}"
                    if file_path:
                        error_msg += f" to {file_path}"
                    error_msg += f": {e}"
                    print(error_msg)
                    for item in items:
                        self.save_error(item)

            self.buffer.clear()

    def save_error(self, data: dict):
        """Save error data to txt file. Data will be a dict with keys {"title": title, "year": self.params["ano"], "situation": self.params["situacao"], "type": self.params["tipo"], "summary": summary, "html_link": document_html_link}. Folder structure will be 'ERROR_LOG_DIR/{year}/{type}/{situation}/{title}_{document_url}.json"""
        with self.lock:
            file_path = None
            try:
                save_dir = Path(self.error_log_dir)
                year_dir = save_dir / str(data["year"])
                type_dir = year_dir / data["type"]
                situation_dir = type_dir / data["situation"]

                # decode path
                situation_dir = Path(unquote(str(situation_dir)))
                situation_dir.mkdir(parents=True, exist_ok=True)

                # use regex to remove invalid characters
                title = unidecode(data["title"]).replace(" ", "_")
                title = self.format_regex_1.sub("_", title)
                title = self.format_regex_2.sub("", title)

                html_link = unidecode(data["html_link"]).replace(" ", "_")
                html_link = self.format_regex_1.sub("_", Path(html_link).stem)
                html_link = self.format_regex_2.sub("", html_link)

                file_path = situation_dir / f"{title}_{html_link}.json"

                # save json
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)

            except Exception as e:
                error_msg = f"Error saving error {data['title']}"
                if file_path:
                    error_msg += f" to {file_path}"
                error_msg += f": {e}"
                print(error_msg)

    def stop(self):
        print(f"Sending stop signal to {self.__class__.__name__}")
        self.running = False
