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

TARGET_SIZE = 2 * 1024 * 1024
MIN_SIZE = 2 * 1024 * 1024
MAX_WORKERS = min(32, (multiprocessing.cpu_count() or 1) * 5)
DB_PATH = "image_compressor.db"

Image.MAX_IMAGE_PIXELS = None
ImageFile.LOAD_TRUNCATED_IMAGES = True
warnings.filterwarnings("ignore", category=UserWarning, module="PIL")
warnings.simplefilter("ignore", Image.DecompressionBombWarning)

logging.basicConfig(
    filename="image_compressor.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

processed_count = 0
skipped_count = 0
skipped_size_count = 0
error_count = 0
total_saved_bytes = 0
total_original_size = 0
total_new_size = 0
db_lock = threading.Lock()
processed_hashes = set()

conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cursor = conn.cursor()
cursor.execute(
    "CREATE TABLE IF NOT EXISTS processed_images (hash TEXT PRIMARY KEY, filename TEXT)"
)
conn.commit()


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
    except Exception:
        return None


def inject_exif(path: Path, exif):
    try:
        with Image.open(path) as img:
            img.convert("RGB").save(path, "JPEG", exif=exif)
    except Exception as e:
        logging.error(f"Не удалось вставить EXIF в {path}: {e}")


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

    except Exception as e:
        logging.error(f"Ошибка при сжатии {path} внешней утилитой: {e}")
        if tmp_path.exists():
            tmp_path.unlink()
        return False, path

    if tmp_path.exists():
        if tmp_path.stat().st_size < original_size:
            if exif:
                inject_exif(tmp_path, exif)
            tmp_path.replace(path)
            return True, path
        logging.error(
            f"Не удалось сжать (не уменьшилось): {path} ({original_size // 1024}KB)"
        )
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

        if temp_path.exists():
            if temp_path.stat().st_size < original_size:
                temp_path.replace(path)
                return True, path
            logging.error(
                f"Не удалось сжать (не уменьшилось): {path} ({original_size // 1024}KB)"
            )
            temp_path.unlink()
    except Exception as e:
        logging.error(f"Ошибка при сжатии {path} Pillow: {e}")
        if temp_path.exists():
            temp_path.unlink()
    return False, path


def compress_image(path: Path, use_fallback: bool = False):
    global processed_count, skipped_count, skipped_size_count, error_count, total_saved_bytes, total_original_size, total_new_size

    try:
        if not path.exists():
            skipped_size_count += 1
            logging.info(
                f"Пропущено (не найден): {path} ({path.stat().st_size // 1024}KB)"
            )
            return

        h = file_hash(path)

        if path.stat().st_size < MIN_SIZE:
            skipped_size_count += 1
            logging.info(
                f"Пропущено (малый размер): {path} ({path.stat().st_size // 1024}KB)"
            )
            processed_hashes.add(h)
            return

        original_size = path.stat().st_size
        total_original_size += original_size

        with db_lock:
            cursor.execute(
                "SELECT filename FROM processed_images WHERE hash = ?", (h,)
            )
            row = cursor.fetchone()
            if row:
                hash_files = row[0].split("|")
                file_path = str(path)
                if file_path in hash_files:
                    skipped_count += 1
                    logging.info(
                        f"Пропущено (уже обработано): {path} ({original_size // 1024}KB)"
                    )
                    processed_hashes.add(h)
                    return
                else:
                    hash_files.append(str(path))
                    cursor.execute(
                        "UPDATE processed_images SET filename = ? WHERE hash = ?",
                        ("|".join(hash_files), h),
                    )
                    conn.commit()
                    skipped_count += 1
                    logging.info(
                        f"Пропущено (дубликат хэша, другой путь): {path} ({original_size // 1024}KB)"
                    )
                    processed_hashes.add(h)
                    return

        ext = path.suffix.lower()
        result, final_path = compress_with_external(path, ext)

        if result is None and use_fallback:
            result, final_path = compress_with_pillow(path)

        if not final_path.exists():
            error_count += 1
            logging.error(f"Файл не найден после сжатия: {final_path}")
            return

        new_size = final_path.stat().st_size
        total_new_size += new_size

        if result:
            saved = original_size - new_size
            total_saved_bytes += saved
            percent = (1 - new_size / original_size) * 100
            logging.info(
                f"Сжато: {path} ({original_size//1024}KB -> {new_size//1024}KB, {percent:.2f}%)"
            )

            h = file_hash(final_path)
            with db_lock:
                cursor.execute(
                    "SELECT filename FROM processed_images WHERE hash = ?",
                    (h,),
                )
                row = cursor.fetchone()
                if row:
                    hash_files = row[0].split("|")
                    file_path = str(path)
                    if file_path not in hash_files:
                        hash_files.append(str(path))
                        cursor.execute(
                            "UPDATE processed_images SET filename = ? WHERE hash = ?",
                            ("|".join(hash_files), h),
                        )
                        conn.commit()
                else:
                    cursor.execute(
                        "INSERT INTO processed_images(hash, filename) VALUES(?, ?)",
                        (h, str(final_path)),
                    )
                    conn.commit()

            processed_hashes.add(h)
            processed_count += 1
        else:
            error_count += 1

    except Exception as e:
        error_count += 1
        logging.error(f"Ошибка при обработке {path}: {e}")


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
    total_files = len(files)
    print(f"Найдено {total_files} изображений.")
    logging.info(
        f"Найдено {total_files} изображений. Fallback = {use_fallback}"
    )

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(compress_image, f, use_fallback) for f in files
        ]
        for i, _ in enumerate(as_completed(futures), 1):
            print(f"\rОбработка: {i}/{len(files)}", end="")

    with db_lock:
        cursor.execute("SELECT hash, filename FROM processed_images")
        all_records = cursor.fetchall()
        all_db_hashes = {row[0] for row in all_records}
        stale_hashes = all_db_hashes - processed_hashes
        deleted_count = 0

        for h, filenames in all_records:
            db_file_list = [
                f.strip() for f in filenames.split("|") if f.strip()
            ]
            real_file_list = [f for f in db_file_list if Path(f).exists()]
            if not real_file_list or h in stale_hashes:
                cursor.execute(
                    "DELETE FROM processed_images WHERE hash = ?", (h,)
                )
                logging.info(f"Удалена запись в БД: {h} {db_file_list}")
                deleted_count += 1
            else:
                cursor.execute(
                    "UPDATE processed_images SET filename = ? WHERE hash = ?",
                    ("|".join(sorted(set(real_file_list))), h),
                )
        conn.commit()

        print(f"\nУдалено устаревших записей из БД: {deleted_count}")
        logging.info(f"Удалено устаревших записей из БД: {deleted_count}")

    print("\n\nГотово.")
    print(f"Обработано успешно: {processed_count}")
    print(f"Уже обработано: {skipped_count}")
    print(f"Пропущено из-за размера: {skipped_size_count}")
    print(f"Ошибки: {error_count}")
    if total_original_size > 0:
        total_percent_saved = (1 - total_new_size / total_original_size) * 100
        print(
            f"Общий размер до сжатия: {total_original_size / 1024 / 1024:.2f} MB"
        )
        print(f"Сэкономлено в процентах: {total_percent_saved:.2f}%")

    logging.info(
        f"Завершено. Обработано успешно: {processed_count}, Уже обработано: {skipped_count}, Пропущено из-за размера: {skipped_size_count}, Ошибки: {error_count}"
    )
    if total_original_size > 0:
        total_percent_saved = (1 - total_new_size / total_original_size) * 100
        logging.info(
            f"Общий размер до сжатия: {total_original_size / 1024 / 1024:.2f} MB, Сэкономлено в процентах: {total_percent_saved:.2f}%"
        )


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.exception("Ошибка в main()")
    input("Нажмите Enter для выхода...")
