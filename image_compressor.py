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

MIN_SIZE = 1.5 * 1024 * 1024
TARGET_SIZE_MB = 1.5 * 1024 * 1024
MAX_WORKERS = min(32, (multiprocessing.cpu_count() or 1) * 5)

DB_PATH = "image_compressor.db"
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cursor = conn.cursor()
cursor.execute("CREATE TABLE IF NOT EXISTS processed (hash TEXT PRIMARY KEY)")
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


def compress_with_external(path: str, ext: str) -> bool:
    path = Path(path)
    original_size = path.stat().st_size
    tmp_path = path.with_name(path.name + ".compressed")
    target_size = TARGET_SIZE_MB

    try:
        if ext == ".png":
            tool = get_tool_path("oxipng.exe")
            for compression_level in range(1, 8):
                subprocess.run(
                    [
                        tool,
                        "--strip",
                        "safe",
                        f"-o{compression_level}",
                        "--out",
                        str(tmp_path),
                        str(path),
                    ],
                    check=True,
                )
                if os.path.getsize(tmp_path) <= target_size:
                    break
        elif ext in [".jpg", ".jpeg"]:
            exif_data = extract_exif(path)

            tool = get_tool_path("cjpeg-static.exe")
            quality = 85
            while True:
                subprocess.run(
                    [
                        tool,
                        f"-quality",
                        str(quality),
                        f"-outfile",
                        str(tmp_path),
                        str(path),
                    ],
                    check=True,
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
                )
                if os.path.getsize(tmp_path) <= target_size or quality < 50:
                    break
                quality -= 5
        else:
            return False
    except FileNotFoundError:
        return None

    if tmp_path.exists():
        new_size = os.path.getsize(tmp_path)
        if new_size < original_size:
            tmp_path.replace(path)
            return True
        else:
            tmp_path.unlink()
    return False


def compress_with_pillow(path: str) -> bool:
    ext = Path(path).suffix.lower()
    original_size = os.path.getsize(path)
    target_size = TARGET_SIZE_MB
    temp_path = Path(path).with_suffix(".pillowtmp")

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
                if temp_path.stat().st_size <= target_size:
                    break
                quality -= 5

        if temp_path.exists() and temp_path.stat().st_size < original_size:
            temp_path.replace(path)
            return True
        elif temp_path.exists():
            temp_path.unlink()
            return False

    except Exception as e:
        logging.error(f"Ошибка Pillow для {path}: {e}")
        return False


def compress_image(path: str, fallback_to_pillow: bool = False):
    global processed_count, skipped_count, total_saved_bytes

    try:
        original_size = os.path.getsize(path)
        if original_size < MIN_SIZE:
            skipped_count += 1
            return

        h = file_hash(path)
        with db_lock:
            cursor.execute("SELECT 1 FROM processed WHERE hash = ?", (h,))
            if cursor.fetchone():
                skipped_count += 1
                return

        ext = Path(path).suffix.lower()
        result = compress_with_external(path, ext)

        if result is None and fallback_to_pillow:
            result = compress_with_pillow(path)

        new_size = os.path.getsize(path)
        if result and new_size < original_size:
            saved_bytes = original_size - new_size
            total_saved_bytes += saved_bytes

            saved_percent = (1 - new_size / original_size) * 100
            msg = (
                f"Сжато: {path} "
                f"({original_size / 1024:.1f} KB -> {new_size / 1024:.1f} KB, "
                f"сохранено {saved_percent:.2f}%)"
            )
            logging.info(msg)
        else:
            msg = f"Пропущено (не меньше): {path} ({original_size / 1024:.1f} KB)"
            logging.info(msg)

        with db_lock:
            cursor.execute("INSERT INTO processed(hash) VALUES(?)", (h,))
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
    parser.add_argument(
        "--dry-run",
        help="Показать, что будет сделано, без изменения файлов.",
        action="store_true",
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
    required_tools = ["cjpeg-static.exe", "cwebp.exe", "oxipng.exe"]
    missing = []
    for tool in required_tools:
        if not os.path.exists(get_tool_path(tool)):
            missing.append(tool)

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
    print(f"Пропущено (маленькие/повтор): {skipped_count}")
    print(f"Сэкономлено в среднем: {total_saved_bytes / 1024 / 1024:.2f} MB")
    logging.info(
        f"Завершено. Обработано: {processed_count}, Пропущено: {skipped_count}, Сэкономлено: {total_saved_bytes / 1024 / 1024:.2f} MB"
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception("Ошибка в main()")
    input()
