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
MAX_SIZE = 2 * 1024 * 1024
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
total_images_original_size = 0
total_images_new_size = 0
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


def get_folder_size(path: Path) -> int:
    excluded = {
        "image_compressor.exe",
        "image_compressor.py",
        "image_compressor.db",
        "image_compressor.db-journal",
        "image_compressor.log",
    }
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            if filename in excluded:
                continue
            file_path = Path(dirpath) / filename
            total_size += file_path.stat().st_size
    return total_size


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
    tmp_path = path.with_suffix(".jpg")
    try:
        with Image.open(path) as img:
            img.convert("RGB").save(tmp_path, "JPEG")
            path.unlink()
            return tmp_path
    except Exception as e:
        logging.error(
            f"Ошибка при конвертации PNG в JPEG: {path} ({path.stat().st_size // 1024} KB): {e}"
        )
        if tmp_path.exists():
            tmp_path.unlink()
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
        logging.error(
            f"Ошибка при сжатии внешней утилитой {path} ({original_size // 1024} KB): {e}"
        )
        if tmp_path.exists():
            tmp_path.unlink()
        return False, path

    if tmp_path.exists():
        new_size = tmp_path.stat().st_size
        if new_size < original_size:
            if exif:
                inject_exif(tmp_path, exif)
            tmp_path.replace(path)
            return True, path
        else:
            logging.error(
                f"Не удалось сжать внешней утилитой (не уменьшилось): {path} ({original_size // 1024} KB)"
            )
            tmp_path.unlink()

    return False, path


def compress_with_pillow(path: Path) -> Tuple[bool, Path]:
    original_size = path.stat().st_size
    tmp_path = path.with_name(path.stem + ".pillowtmp" + path.suffix)
    exif = extract_exif(path)

    try:
        with Image.open(path) as img:
            img_format = img.format
            quality = 85
            while quality >= 50:
                img.save(
                    tmp_path,
                    format=img_format,
                    optimize=True,
                    quality=quality,
                    exif=exif,
                )
                if tmp_path.stat().st_size <= TARGET_SIZE:
                    break
                quality -= 5

    except Exception as e:
        logging.error(
            f"Ошибка при сжатии Pillow {path} ({original_size // 1024} KB): {e}"
        )
        if tmp_path.exists():
            tmp_path.unlink()

    if tmp_path.exists():
        if tmp_path.stat().st_size < original_size:
            if exif:
                inject_exif(tmp_path, exif)
            tmp_path.replace(path)
            return True, path
        logging.error(
            f"Не удалось сжать Pillow (не уменьшилось): {path} ({original_size // 1024} KB)"
        )
        tmp_path.unlink()
    return False, path


def compress_image(path: Path):
    global processed_count, skipped_count, skipped_size_count, error_count, total_saved_bytes, total_images_original_size, total_images_new_size, processed_hashes

    try:
        original_size = path.stat().st_size
        total_images_original_size += original_size

        h = file_hash(path)

        if path.stat().st_size < MAX_SIZE:
            logging.info(
                f"Пропущено (малый размер): {path} ({path.stat().st_size // 1024} KB)"
            )
            processed_hashes.add(h)
            skipped_size_count += 1
            total_images_new_size += original_size
            return

        with db_lock:
            cursor.execute(
                "SELECT filename FROM processed_images WHERE hash = ?", (h,)
            )
            row = cursor.fetchone()
            file_path = str(path)
            if row:
                processed_hashes.add(h)
                skipped_count += 1
                total_images_new_size += original_size

                hash_files = row[0].split("|")
                if file_path in hash_files:
                    logging.info(
                        f"Пропущено (уже обработано): {path} ({original_size // 1024} KB)"
                    )
                else:
                    hash_files.append(file_path)
                    cursor.execute(
                        "UPDATE processed_images SET filename = ? WHERE hash = ?",
                        ("|".join(hash_files), h),
                    )
                    conn.commit()
                    logging.info(
                        f"Пропущено (дубликат хэша, другой путь): {path} ({original_size // 1024} KB)"
                    )
                return

        ext = path.suffix.lower()
        result, final_path = compress_with_external(path, ext)

        if not result:
            result, final_path = compress_with_pillow(path)

        new_size = final_path.stat().st_size
        total_images_new_size += new_size

        if result:
            h = file_hash(final_path)

            processed_count += 1

            saved = original_size - new_size
            total_saved_bytes += saved
            percent = (1 - new_size / original_size) * 100
            logging.info(
                f"Сжато: {path} ({original_size//1024} KB -> {new_size//1024} KB, {percent:.2f}%)"
            )

            with db_lock:
                cursor.execute(
                    "SELECT filename FROM processed_images WHERE hash = ?",
                    (h,),
                )
                row = cursor.fetchone()
                file_path = str(final_path)
                if row:
                    hash_files = row[0].split("|")
                    if file_path not in hash_files:
                        hash_files.append(file_path)
                        cursor.execute(
                            "UPDATE processed_images SET filename = ? WHERE hash = ?",
                            ("|".join(hash_files), h),
                        )
                        conn.commit()
                else:
                    cursor.execute(
                        "INSERT INTO processed_images(hash, filename) VALUES(?, ?)",
                        (h, file_path),
                    )
                    conn.commit()
        else:
            error_count += 1

        processed_hashes.add(h)

    except Exception as e:
        error_count += 1
        total_images_new_size += original_size
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

    if missing:
        print("Не найдены:", ", ".join(missing))
        logging.error("Не найдены:", ", ".join(missing))
        return

    total_original_size = get_folder_size(input_dir)

    files = prepare_and_copy_files(input_dir, output_dir)
    total_files = len(files)
    print(f"Найдено {total_files} изображений.")
    logging.info(f"Начато. Найдено {total_files} изображений.")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(compress_image, f) for f in files]
        for i, _ in enumerate(as_completed(futures), 1):
            print(f"\rОбработка: {i}/{len(files)}", end="")

    total_new_size = get_folder_size(input_dir)

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
            real_file_list = [
                f
                for f in db_file_list
                if Path(f).exists() and file_hash(Path(f)) == h
            ]
            reasone = None
            if h in stale_hashes:
                reasone = "не встречался хэш"
            if not real_file_list:
                reasone = "файлы не сущестувуют"
            if reasone:
                cursor.execute(
                    "DELETE FROM processed_images WHERE hash = ?", (h,)
                )
                logging.info(
                    f'Удалена запись в БД по причине "{reasone}": {h} {db_file_list}'
                )
                deleted_count += 1
            else:
                cursor.execute(
                    "UPDATE processed_images SET filename = ? WHERE hash = ?",
                    ("|".join(sorted(set(real_file_list))), h),
                )
        conn.commit()

        print(f"\n\nУдалено записей в БД: {deleted_count}")
        logging.info(f"Удалено записей в БД: {deleted_count}")

    print("\nГотово.")
    print(f"Обработано успешно: {processed_count}")
    print(f"Пропущено уже обработанных: {skipped_count}")
    print(f"Пропущено малых: {skipped_size_count}")
    print(f"Ошибки: {error_count}")
    if total_original_size > 0:
        total_percent_saved = (1 - total_new_size / total_original_size) * 100
    else:
        total_percent_saved = 0
    if total_images_original_size > 0:
        total_images_percent_saved = (
            1 - total_images_new_size / total_images_original_size
        ) * 100
    else:
        total_images_percent_saved = 0

    msg_total = f"Сжато всего: {total_original_size / 1024 / 1024:.2f} MB -> {total_new_size / 1024 / 1024:.2f} MB, {total_percent_saved:.2f}%)"
    msg_total_images = f"Сжато изображений: {total_images_original_size / 1024 / 1024:.2f} MB -> {total_images_new_size / 1024 / 1024:.2f} MB, {total_images_percent_saved:.2f}%)"

    print(msg_total)
    print(msg_total_images)

    logging.info(
        f"Завершено. Обработано успешно: {processed_count}, Уже обработано: {skipped_count}, Пропущено: {skipped_size_count}, Ошибки: {error_count}"
    )
    logging.info(msg_total)
    logging.info(msg_total_images)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("\nОшибка в main()")
        logging.exception("Ошибка в main()")
    input("\nНажмите Enter для выхода...")
