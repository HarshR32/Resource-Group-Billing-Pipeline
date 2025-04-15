import os
from watchdog.observers import Observer
from pathlib import Path
from watchdog.events import FileSystemEventHandler
import time
import threading

from pipeline import run_pipeline
from config.config import data
from src.utils.excelToCsv import excel_to_csv

raw_input_dir = os.path.join(Path().resolve(), data['raw'])

stop_event = threading.Event()

class RawFileHandler(FileSystemEventHandler):
    def __init__(self,output_dir):
        self.output_dir= output_dir
    def on_created(self, event):
        if event.src_path.endswith('.xlsx'):
            print(f"[INFO] New Excel file detected: {event.src_path}")
            excel_to_csv(event.src_path,self.output_dir)

def start_excel_watcher():
    raw_output_dir = {name: os.path.join(Path().resolve(), path) for name, path in data['raw-extracted'].items()}
    observer = Observer()
    observer.schedule(RawFileHandler(raw_output_dir), path=raw_input_dir, recursive=False)
    observer.start()
    print("[WATCHER] Started.")

    try:
        while not stop_event.is_set():
            time.sleep(1)
    finally:
        print("[WATCHER] Stopping...")
        observer.stop()
        observer.join()
        print("[WATCHER] Stopped.")


def start_spark_stream():
    print("[PIPELINE] Starting Pipeline...")
    try:
        run_pipeline()
    finally:
        stop_event.set()  
        print("[PIPELINE] Pipeline ended.")

if __name__ == "__main__":
    try:
        watcher_thread = threading.Thread(target=start_excel_watcher)
        spark_thread = threading.Thread(target=start_spark_stream)

        watcher_thread.start()
        spark_thread.start()

        watcher_thread.join()
        spark_thread.join()

    except KeyboardInterrupt:
        print("[MAIN] Ctrl+C detected. Stopping threads...")
        stop_event.set()