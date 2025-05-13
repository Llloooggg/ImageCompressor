import os
import sys
import hashlib
import sqlite3
import subprocess
import logging
import argparse
import threading
import multiprocessing
import warnings
from PIL import Image, ImageFile
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

ImageFile.LOAD_TRUNCATED_IMAGES = True
warnings.filterwarnings("ignore", category=UserWarning, module="PIL")

logging.basicConfig(
    filename="image_compressor.log",
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
)

MIN_SIZE = 2 * 1024 * 1024
TARGET_SIZE_MB = 2 * 1024 * 1024
MAX_WORKERS = min(32, (multiprocessing.cpu_count() or 1) * 5)

DB_PATH = "image_compressor.db"
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cursor = conn.cursor()
cursor.execute(
    "CREATE TABLE IF NOT EXISTS processed (hash TEXT PRIMARY KEY, filename TEXT)"
)
conn.commit()

processed_count = 0
skipped_count = 0
total_saved_bytes = 0
db_lock = threading.Lock()


def get_tool_path(tool_name):
    if hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS) / "tools" / tool_name
    return Path("tools") / tool_name


def file_hash(path):
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def extract_exif(path):
    try:
        with Image.open(path) as img:
            return img.info.get("exif")
    except:
        return None


def inject_exif(jpeg_path, exif_bytes):
    try:
        with Image.open(jpeg_path) as img:
            rgb = img.convert("RGB")
            rgb.save(jpeg_path, "JPEG", exif=exif_bytes)
    except Exception as e:
        logging.error(f"Ошибка при вставке EXIF в {jpeg_path}: {e}")


def convert_png_to_jpeg(path: Path) -> Path | None:
    try:
        temp_path = path.with_suffix(".jpg")
        with Image.open(path) as img:
            img.convert("RGB").save(
                temp_path, "JPEG", quality=85, optimize=True
            )
        if temp_path.stat().st_size < path.stat().st_size:
            path.unlink()
            return temp_path
        else:
            temp_path.unlink()
    except Exception as e:
        logging.error(f"Ошибка при конвертации PNG в JPEG для {path}: {e}")
    return None


def compress_with_external(path: str, ext: str) -> tuple[bool, Path]:
    path = Path(path)
    original_size = path.stat().st_size
    tmp_path = path.with_name(path.stem + ".compressed" + path.suffix)
    target_size = TARGET_SIZE_MB

    try:
        if ext == ".png":
            new_path = convert_png_to_jpeg(path)
            if not new_path:
                return False, path
            path = new_path
            ext = ".jpg"

        if ext in [".jpg", ".jpeg"]:
            exif_data = extract_exif(path)
            tool = get_tool_path("cjpeg-static.exe")
            quality = 85
            while True:
                subprocess.run(
                    [
                        tool,
                        "-quality",
                        str(quality),
                        "-outfile",
                        str(tmp_path),
                        str(path),
                    ],
                    check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                if os.path.getsize(tmp_path) <= target_size or quality < 50:
                    break
                quality -= 5
            if tmp_path.exists() and exif_data:
                inject_exif(tmp_path, exif_data)

        elif ext == ".webp":
            tool = get_tool_path("cwebp.exe")
            quality = 80
            while True:
                subprocess.run(
                    [
                        tool,
                        str(path),
                        "-o",
                        str(tmp_path),
                        "-m",
                        "6",
                        "-q",
                        str(quality),
                        "-metadata",
                        "all",
                    ],
                    check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                if os.path.getsize(tmp_path) <= target_size or quality < 50:
                    break
                quality -= 5
        else:
            return False, path
    except FileNotFoundError:
        return None, path
    except Exception as e:
        logging.error(f"Ошибка внешнего сжатия {path}: {e}")
        return False, path

    if tmp_path.exists():
        new_size = tmp_path.stat().st_size
        if new_size < original_size:
            tmp_path.replace(path)
            return True, path
        else:
            tmp_path.unlink()
    return False, path


def compress_with_pillow(path: str) -> tuple[bool, Path]:
    path = Path(path)
    original_size = path.stat().st_size
    temp_path = path.with_name(path.stem + ".pillowtmp" + path.suffix)

    try:
        with Image.open(path) as img:
            img_format = img.format
            exif = img.info.get("exif", None)
            quality = 85
            while quality >= 50:
                img.save(
                    temp_path,
                    format=img_format,
                    optimize=True,
                    quality=quality,
                    exif=exif,
                )
                if temp_path.stat().st_size <= TARGET_SIZE_MB:
                    break
                quality -= 5

        if temp_path.exists() and temp_path.stat().st_size < original_size:
            temp_path.replace(path)
            return True, path
        elif temp_path.exists():
            temp_path.unlink()
            return False, path
    except Exception as e:
        logging.error(f"Ошибка Pillow для {path}: {e}")
    return False, path


def compress_image(path: str, fallback_to_pillow: bool = False):
    global processed_count, skipped_count, total_saved_bytes

    try:
        path = Path(path)
        original_size = path.stat().st_size
        if original_size < MIN_SIZE:
            skipped_count += 1
            logging.info(
                f"Пропущено (уже малый): {path} ({original_size / 1024:.1f} KB)"
            )
            return

        h = file_hash(path)
        with db_lock:
            cursor.execute("SELECT 1 FROM processed WHERE hash = ?", (h,))
            if cursor.fetchone():
                skipped_count += 1
                logging.info(
                    f"Пропущено (уже сжато): {path} ({original_size / 1024:.1f} KB)"
                )
                return

        ext = path.suffix.lower()
        result, path = compress_with_external(path, ext)

        if result is None and fallback_to_pillow:
            result, path = compress_with_pillow(path)

        new_size = path.stat().st_size
        if result and new_size < original_size:
            saved_bytes = original_size - new_size
            total_saved_bytes += saved_bytes
            percent = (1 - new_size / original_size) * 100
            logging.info(
                f"Сжато: {path} ({original_size / 1024:.1f} KB -> {new_size / 1024:.1f} KB, сохранено {percent:.2f}%)"
            )
        else:
            logging.info(
                f"Пропущено (не меньше): {path} ({original_size / 1024:.1f} KB)"
            )

        h = file_hash(path)
        with db_lock:
            cursor.execute(
                "INSERT INTO processed(hash, filename) VALUES(?, ?)",
                (h, path.name),
            )
            conn.commit()

        processed_count += 1
    except Exception as e:
        logging.error(f"Ошибка обработки {path}: {e}")


def find_images(root: str):
    exts = {".png", ".jpg", ".jpeg", ".webp"}
    for dirpath, _, filenames in os.walk(root):
        for f in filenames:
            if Path(f).suffix.lower() in exts:
                yield Path(dirpath) / f


def main():
    parser = argparse.ArgumentParser(description="Компрессор изображенеий")
    parser.add_argument(
        "--input",
        help="Путь для сканирования. По умолчанию текущая директория.",
        default=None,
    )
    args = parser.parse_args()

    if args.input:
        input_dir = Path(args.input)
    else:
        print(
            "Не указан путь. Обрабатывать текущую папку и все подпапки? [y/n]"
        )
        choice = input().strip().lower()
        if choice != "y":
            print("Отменено.")
            return
        input_dir = Path(os.getcwd())

    print("Проверка утилит...")
    required_tools = ["cjpeg-static.exe", "cwebp.exe"]
    missing = [
        tool
        for tool in required_tools
        if not os.path.exists(get_tool_path(tool))
    ]

    fallback = False
    if missing:
        print("Не найдены внешние утилиты:", ", ".join(missing))
        print("Использовать Pillow вместо них, где возможно? [y/n]")
        if input().strip().lower() == "y":
            fallback = True
        else:
            print("Без утилит работа невозможна.")
            return

    files = list(find_images(input_dir))
    print(f"Найдено {len(files)} изображений.")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(compress_image, f, fallback) for f in files]
        for i, _ in enumerate(as_completed(futures), 1):
            print(f"\rОбработка изображений: {i}/{len(files)}", end="")

    print("\nОбработка завершена.")
    print(f"Всего обработано: {processed_count}")
    print(f"Пропущено: {skipped_count}")
    print(f"Сэкономлено: {total_saved_bytes / 1024 / 1024:.2f} MB")
    logging.info(
        f"Завершено. Обработано: {processed_count}, Пропущено: {skipped_count}, Сэкономлено: {total_saved_bytes / 1024 / 1024:.2f} MB"
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception("Ошибка в main()")
    input()
