from loguru import logger
import os
import shutil
import schedule
import time
from datetime import datetime
import mcrcon

source_folder = ""
backup_base_path = ""
subfolders_to_backup = ["world", "world_nether", "world_the_end"]


def create_backup():
    logger.info("Create backup now")
    try:
        with mcrcon.MCRcon(host="localhost", port=25575, password="rcon123") as client:
            client.command("say save-all in 5 seconds...")
            time.sleep(5)
            client.command("say save-all...")
            t0 = time.time()
            response = client.command("save-all flush")
            logger.info(f"rcon response: {response}")
            t1 = time.time()
            client.command(f"say save-all...flushed in {t1 - t0:.3f}")
            logger.info(f"save-all...flushed in {t1 - t0:.3f}")
    except ConnectionRefusedError as err:
        logger.warning("An error occurs when 'save-all':")
        logger.exception(err)
        logger.warning("But backuping continues.")

    now = datetime.now()

    backup_folder_name = now.strftime("%Y%m%d_%H%M%S")
    backup_folder_path = os.path.join(backup_base_path, backup_folder_name)

    if not os.path.exists(backup_folder_path):
        os.makedirs(backup_folder_path)

    try:
        for subfolder in subfolders_to_backup:
            subfolder_path = os.path.join(source_folder, subfolder)
            if os.path.isdir(subfolder_path):
                destination_path = os.path.join(backup_folder_path, subfolder)
                shutil.copytree(
                    subfolder_path,
                    destination_path,
                    ignore=shutil.ignore_patterns("session.lock"),
                )
                logger.info(
                    f"Backup created for subfolder {subfolder} at {destination_path}"
                )
            else:
                logger.warning(
                    f"Subfolder {subfolder} does not exist in source folder."
                )
        logger.info(f"Backup created at {backup_folder_path}")
    except Exception as e:
        logger.error(f"Failed to create backup: {e}")


def cleanup_old_backups(daily_backup_retention: int):
    logger.info("Cleanup old backups now")
    today = datetime.now().date()
    backup_folders = [
        os.path.join(backup_base_path, folder)
        for folder in os.listdir(backup_base_path)
        if os.path.isdir(os.path.join(backup_base_path, folder))
    ]
    backups_by_date = {}

    for folder in backup_folders:
        folder_date = datetime.strptime(os.path.basename(folder)[:8], "%Y%m%d").date()
        if folder_date not in backups_by_date:
            backups_by_date[folder_date] = []
        backups_by_date[folder_date].append(folder)
    logger.debug(f"{backups_by_date=}")

    for date, folders in backups_by_date.items():
        if date < today:
            if len(folders) > daily_backup_retention:
                folders.sort()
                for folder_to_delete in folders[:-daily_backup_retention]:
                    try:
                        shutil.rmtree(folder_to_delete)
                        logger.info(f"Deleted old backup: {folder_to_delete}")
                    except Exception as e:
                        logger.error(f"Failed to delete old backup: {e}")


def main():
    if not all(bool(e) for e in [source_folder, backup_base_path]):
        raise Exception("'source_folder' and 'backup_base_path' cannot be empty")

    if not os.path.exists(backup_base_path):
        os.makedirs(backup_base_path)

    log_file_path = os.path.join(backup_base_path, "backup_scheduler.log")
    logger.add(
        log_file_path,
        rotation="100 MB",
        compression="zip",
        backtrace=True,
        diagnose=True,
    )

    schedule.every(60).minutes.do(create_backup)
    schedule.every().day.at("00:02").do(lambda: cleanup_old_backups(1))

    logger.info("Backup scheduler started. Press Ctrl+C to stop.")
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
