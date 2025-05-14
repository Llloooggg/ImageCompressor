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
from typing import Tuple, Optional

# --- Константы ---
TARGET_SIZE = 2 * 1024 * 1024  # 2MB
MIN_SIZE = TARGET_SIZE
MAX_WORKERS = min(32, (multiprocessing.cpu_count() or 1) * 5)
DB_PATH = "image_compressor.db"

# --- Настройки Pillow ---
Image.MAX_IMAGE_PIXELS = None
ImageFile.LOAD_TRUNCATED_IMAGES = True
warnings.filterwarnings("ignore", category=UserWarning, module="PIL")
warnings.simplefilter("ignore", Image.DecompressionBombWarning)

# --- Логирование ---
logging.basicConfig(
    filename="image_compressor.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# --- Глобальные переменные ---
processed_count = 0
skipped_count = 0
total_saved_bytes = 0
db_lock = threading.Lock()

# --- База данных ---
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cursor = conn.cursor()
cursor.execute(
    "CREATE TABLE IF NOT EXISTS processed_images (hash TEXT PRIMARY KEY, filename TEXT, reduced BOOLEAN)"
)
conn.commit()

# --- Утилиты ---


def get_tool_path(name: str) -> Path:
    if hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS) / "tools" / name
    return Path("tools") / name


def file_hash(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def extract_exif(path: Path):
    try:
        with Image.open(path) as img:
            return img.info.get("exif")
    except:
        return None


def inject_exif(path: Path, exif):
    try:
        with Image.open(path) as img:
            img.convert("RGB").save(path, "JPEG", exif=exif)
    except Exception as e:
        logging.error(f"Не удалось вставить EXIF в {path}: {e}")


# --- Сжатие ---


def convert_png_to_jpeg(path: Path) -> Optional[Path]:
    temp_path = path.with_suffix(".jpg")
    try:
        with Image.open(path) as img:
            img.convert("RGB").save(
                temp_path, "JPEG", quality=85, optimize=True
            )
        if temp_path.stat().st_size < path.stat().st_size:
            path.unlink()
            return temp_path
        temp_path.unlink()
    except Exception as e:
        logging.error(f"Ошибка при конвертации PNG в JPEG: {path}: {e}")
    return None


def compress_with_external(
    path: Path, ext: str
) -> Tuple[Optional[bool], Path]:
    original_size = path.stat().st_size
    tmp_path = path.with_name(path.stem + ".compressed" + path.suffix)
    exif = extract_exif(path)

    try:
        if ext == ".png":
            converted = convert_png_to_jpeg(path)
            if not converted:
                return False, path
            path = converted
            ext = ".jpg"
            original_size = path.stat().st_size

        if ext in [".jpg", ".jpeg"]:
            tool = get_tool_path("cjpeg-static.exe")
            args_base = [
                str(tool),
                "-quality",
                "",
                "-outfile",
                str(tmp_path),
                str(path),
            ]
        elif ext == ".webp":
            tool = get_tool_path("cwebp.exe")
            args_base = [
                str(tool),
                str(path),
                "-o",
                str(tmp_path),
                "-m",
                "6",
                "-q",
                "",
                "-metadata",
                "all",
            ]
        else:
            return False, path

        quality = 85
        while quality >= 50:
            args = args_base.copy()
            args[args.index("")] = str(quality)
            subprocess.run(
                args,
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            if tmp_path.stat().st_size <= TARGET_SIZE:
                break
            quality -= 5

    except FileNotFoundError:
        return None, path
    except Exception as e:
        logging.error(f"Ошибка при сжатии {path} внешней утилитой: {e}")
        return False, path

    if tmp_path.exists():
        if tmp_path.stat().st_size < original_size:
            if exif:
                inject_exif(tmp_path, exif)
            tmp_path.replace(path)
            return True, path
        tmp_path.unlink()
    return False, path


def compress_with_pillow(path: Path) -> Tuple[bool, Path]:
    original_size = path.stat().st_size
    temp_path = path.with_name(path.stem + ".pillowtmp" + path.suffix)

    try:
        with Image.open(path) as img:
            exif = img.info.get("exif")
            img_format = img.format
            quality = 85
            while quality >= 50:
                img.save(
                    temp_path,
                    format=img_format,
                    optimize=True,
                    quality=quality,
                    exif=exif,
                )
                if temp_path.stat().st_size <= TARGET_SIZE:
                    break
                quality -= 5

        if temp_path.exists() and temp_path.stat().st_size < original_size:
            temp_path.replace(path)
            return True, path
        temp_path.unlink()
    except Exception as e:
        logging.error(f"Pillow не смог сжать {path}: {e}")
    return False, path


def compress_image(path: Path, use_fallback: bool = False):
    global processed_count, skipped_count, total_saved_bytes

    try:
        if not path.exists() or path.stat().st_size < MIN_SIZE:
            skipped_count += 1
            logging.info(
                f"Пропущено (малый размер или не найден): {path} ({path.stat().st_size // 1024}KB)"
            )
            return

        original_size = path.stat().st_size
        h = file_hash(path)
        with db_lock:
            cursor.execute(
                "SELECT 1 FROM processed_images WHERE hash = ?", (h,)
            )
            if cursor.fetchone():
                skipped_count += 1
                logging.info(
                    f"Пропущено (уже обработано): {path} ({original_size // 1024}KB)"
                )
                return

        ext = path.suffix.lower()
        result, final_path = compress_with_external(path, ext)

        if result is None and use_fallback:
            result, final_path = compress_with_pillow(path)

        if not final_path.exists():
            logging.warning(f"Файл не найден после сжатия: {final_path}")
            return

        new_size = final_path.stat().st_size

        if result:
            if new_size < original_size:
                saved = original_size - new_size
                total_saved_bytes += saved
                percent = (1 - new_size / original_size) * 100
                logging.info(
                    f"Сжато: {path} ({original_size//1024}KB -> {new_size//1024}KB, -{percent:.2f}%)"
                )
            else:
                logging.info(
                    f"Пропущено (не уменьшилось): {path} ({new_size // 1024}KB)"
                )

            h = file_hash(final_path)
            with db_lock:
                cursor.execute(
                    "INSERT INTO processed_images(hash, filename, reduced) VALUES(?, ?, ?)",
                    (h, final_path.name, new_size < original_size),
                )
                conn.commit()

        processed_count += 1
    except Exception as e:
        logging.error(f"Ошибка при обработке {path}: {e}")


# --- Основной процесс ---


def find_images(root: Path):
    exts = {".jpg", ".jpeg", ".png", ".webp"}
    for dirpath, _, filenames in os.walk(root):
        for name in filenames:
            if Path(name).suffix.lower() in exts:
                yield Path(dirpath) / name


def prepare_and_copy_files(input_dir: Path, output_dir: Path) -> list[Path]:
    if input_dir.resolve() == output_dir.resolve():
        return list(find_images(input_dir))

    output_dir.mkdir(parents=True, exist_ok=True)
    copied = []

    for image in find_images(input_dir):
        rel_path = image.relative_to(input_dir)
        dest = output_dir / rel_path
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(image.read_bytes())
        copied.append(dest)

    return copied


def main():
    parser = argparse.ArgumentParser(
        description="Сжатие изображений до заданного размера"
    )
    parser.add_argument(
        "--input", help="Папка со входными изображениями", default=os.getcwd()
    )
    parser.add_argument(
        "--output", help="Папка для сжатых изображений", default=None
    )
    args = parser.parse_args()

    input_dir = Path(args.input).resolve()
    output_dir = Path(args.output).resolve() if args.output else input_dir

    print(f"Входная папка: {input_dir}")
    print(f"Выходная папка: {output_dir}")
    if input("Начать обработку? [y/n]: ").strip().lower() != "y":
        print("Отменено.")
        return

    print("Проверка необходимых инструментов...")
    required = ["cjpeg-static.exe", "cwebp.exe"]
    missing = [t for t in required if not get_tool_path(t).exists()]

    use_fallback = False
    if missing:
        print("Не найдены:", ", ".join(missing))
        choice = input("Использовать Pillow? [y/n]: ").strip().lower()
        if choice != "y":
            print("Работа прервана.")
            return
        use_fallback = True

    files = prepare_and_copy_files(input_dir, output_dir)
    print(f"Найдено {len(files)} изображений.")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(compress_image, f, use_fallback) for f in files
        ]
        for i, _ in enumerate(as_completed(futures), 1):
            print(f"\rОбработка: {i}/{len(files)}", end="")

    print("\nГотово.")
    print(f"Обработано: {processed_count}, Пропущено: {skipped_count}")
    print(f"Сэкономлено: {total_saved_bytes / 1024 / 1024:.2f} MB")
    logging.info(
        f"Завершено. Обработано: {processed_count}, Пропущено: {skipped_count}, Сэкономлено: {total_saved_bytes / 1024 / 1024:.2f} MB"
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception("Ошибка в main()")
    input("Нажмите Enter для выхода...")
