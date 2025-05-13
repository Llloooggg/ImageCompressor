import os
import hashlib
import sqlite3
import argparse
import logging
from PIL import Image, ImageFile
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import threading

ImageFile.LOAD_TRUNCATED_IMAGES = True

DB_NAME = "compressed_images.db"
LOG_FILE = "compression.log"
TARGET_SIZE_MB = 1.5
MIN_SIZE_MB = 1.0
THREADS = os.cpu_count() or 4
LOCK = threading.Lock()

stats = {
    "total": 0,
    "skipped": 0,
    "already_done": 0,
    "compressed": 0,
    "original_bytes": 0,
    "compressed_bytes": 0,
}


def init_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def init_db():
    conn = sqlite3.connect(DB_NAME)
    with conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS compressed_images (
                hash TEXT PRIMARY KEY
            )
        """
        )
    conn.close()


def calculate_hash(file_path):
    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def is_already_compressed(file_hash):
    with LOCK:
        conn = sqlite3.connect(DB_NAME)
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM compressed_images WHERE hash = ?", (file_hash,)
        )
        result = cur.fetchone()
        conn.close()
    return result is not None


def mark_as_compressed(file_hash):
    with LOCK:
        conn = sqlite3.connect(DB_NAME)
        conn.execute(
            "INSERT OR IGNORE INTO compressed_images (hash) VALUES (?)",
            (file_hash,),
        )
        conn.commit()
        conn.close()


def compress_image(file_path):
    try:
        stats["total"] += 1
        original_size = os.path.getsize(file_path)
        size_mb = original_size / (1024 * 1024)

        if size_mb < MIN_SIZE_MB:
            stats["skipped"] += 1
            return f"Пропущено (<1MB): {file_path}"

        file_hash = calculate_hash(file_path)
        if is_already_compressed(file_hash):
            stats["already_done"] += 1
            return f"Пропущено (уже обработано): {file_path}"

        img = Image.open(file_path)
        img_format = img.format
        img_exif = img.info.get("exif", None)

        # Поддержка только RGB/RGBA для webp
        if img.mode not in ["RGB", "RGBA"]:
            img = img.convert("RGB")

        quality = 95
        step = 5
        buffer = BytesIO()

        while quality > 10:
            buffer.seek(0)
            buffer.truncate()
            try:
                img.save(
                    buffer,
                    format=img_format,
                    quality=quality,
                    optimize=True,
                    exif=img_exif,
                )
            except Exception:
                img.save(
                    buffer, format=img_format, quality=quality, optimize=True
                )
            size = buffer.tell() / (1024 * 1024)
            if size <= TARGET_SIZE_MB:
                break
            quality -= step

        compressed_size = buffer.tell()

        if compressed_size < original_size:
            with open(file_path, "wb") as f:
                f.write(buffer.getvalue())

            stats["compressed"] += 1
            stats["original_bytes"] += original_size
            stats["compressed_bytes"] += compressed_size
            mark_as_compressed(file_hash)

            saved = original_size - compressed_size
            return f"Сжато: {file_path} (-{saved / 1024 / 1024:.2f} MB)"
        else:
            stats["skipped"] += 1
            return f"Без изменений (не удалось уменьшить): {file_path}"
    except Exception as e:
        return f"Ошибка: {file_path} — {e}"


def walk_images(root):
    supported = {".jpg", ".jpeg", ".png", ".webp"}
    for dirpath, _, filenames in os.walk(root):
        for name in filenames:
            if os.path.splitext(name)[1].lower() in supported:
                yield os.path.join(dirpath, name)


def get_user_confirmation(default_path):
    print(
        f"Будет выполнен рекурсивный обход всех изображений по пути: {default_path}"
    )
    answer = input("Продолжить? [y/n]: ").strip().lower()
    return answer == "y"


def print_summary():
    original = stats["original_bytes"]
    compressed = stats["compressed_bytes"]
    saved = original - compressed

    if original > 0:
        saved_pct = saved / original * 100
    else:
        saved_pct = 0.0

    summary = (
        f"\n==== СТАТИСТИКА ====\n"
        f"Всего файлов: {stats['total']}\n"
        f"Сжато: {stats['compressed']}\n"
        f"Пропущено: {stats['skipped']} (мелкие/не уменьшено)\n"
        f"Уже обработано ранее: {stats['already_done']}\n"
        f"Экономия: {saved / 1024 / 1024:.2f} MB ({saved_pct:.2f}%)\n"
        f"====================\n"
    )
    logging.info(summary)


def main():
    init_logging()

    parser = argparse.ArgumentParser(
        description="Сжатие изображений с сохранением качества, EXIF и учётом уже обработанных."
    )
    parser.add_argument(
        "--input",
        "-i",
        type=str,
        help="Путь к папке с изображениями",
        default=".",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        help="Путь для вывода (не используется пока)",
        default=None,
    )
    args = parser.parse_args()

    input_path = os.path.abspath(args.input)

    if input_path == os.path.abspath(".") and not get_user_confirmation(
        input_path
    ):
        print("Операция отменена.")
        return

    init_db()
    images = list(walk_images(input_path))

    logging.info(f"Найдено {len(images)} изображений для обработки...")

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = {executor.submit(compress_image, img): img for img in images}
        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Обработка изображений",
        ):
            result = future.result()
            if result:
                logging.info(result)

    print_summary()


if __name__ == "__main__":
    main()
