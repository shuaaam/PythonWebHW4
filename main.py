import requests
import threading
import queue
import logging
from pathlib import Path
import shutil
import sys
import file_parser as parser
from normalize import normalize


class Concat:
    def __init__(self, folder_for_scan, event):
        self.work_order = queue.Queue()
        self.event = event
        self.folder_for_scan = folder_for_scan

    def __call__(self, *args, **kwargs):
        while True:
            if self.work_order.empty():
                if self.event.is_set():
                    logging.info('Operation completed')
                    break
            else:
                reader_file, data = self.work_order.get()
                logging.info(f'operation with file {reader_file.name}')
                if sys.argv[1]:
                    folder_for_scan = Path(sys.argv[1])
                    print(f'Start in folder {folder_for_scan.resolve()}')
                    main(folder_for_scan.resolve())


def reader(work_queue):
    while True:
        if files_queue.empty():
            break
        reader_file = files_queue.get()
        logging.info(f'read file {reader_file.name}')
        work_queue.put(reader_file)


def handle_media(filename: Path, target_folder: Path):
    target_folder.mkdir(exist_ok=True, parents=True)
    filename.replace(target_folder / normalize(filename.name))


def handle_other(filename: Path, target_folder: Path):
    target_folder.mkdir(exist_ok=True, parents=True)
    filename.replace(target_folder / normalize(filename.name))


def handle_archive(filename: Path, target_folder: Path):
    target_folder.mkdir(exist_ok=True, parents=True)
    folder_for_file = target_folder / \
        normalize(filename.name.replace(filename.suffix, ''))

    folder_for_file.mkdir(exist_ok=True, parents=True)
    try:
        shutil.unpack_archive(str(filename.resolve()),
                              str(folder_for_file.resolve()))
    except shutil.ReadError:
        print(f'{filename} не є архівом!')
        folder_for_file.rmdir()
        return None
    filename.unlink()


def handle_folder(folder: Path):
    try:
        folder.rmdir()
    except OSError:
        print(f'Не вдалося видалити папку {folder}')


def main(folder: Path):
    parser.scan(folder)

    for file in parser.JPEG_IMAGES:
        handle_media(file, folder / 'images' / 'JPEG')
    for file in parser.JPG_IMAGES:
        handle_media(file, folder / 'images' / 'JPG')
    for file in parser.PNG_IMAGES:
        handle_media(file, folder / 'images' / 'PNG')
    for file in parser.SVG_IMAGES:
        handle_media(file, folder / 'images' / 'SVG')
    for file in parser.MP3_AUDIO:
        handle_media(file, folder / 'audio' / 'MP3')
    for file in parser.OGG_AUDIO:
        handle_media(file, folder / 'audio' / 'OGG')
    for file in parser.WAV_AUDIO:
        handle_media(file, folder / 'audio' / 'WAV')
    for file in parser.AMR_AUDIO:
        handle_media(file, folder / 'audio' / 'AMR')
    for file in parser.AVI_VIDEO:
        handle_media(file, folder / 'video' / 'AVI')
    for file in parser.MP4_VIDEO:
        handle_media(file, folder / 'video' / 'MP4')
    for file in parser.MOV_VIDEO:
        handle_media(file, folder / 'video' / 'MOV')
    for file in parser.MKV_VIDEO:
        handle_media(file, folder / 'video' / 'MKV')
    for file in parser.DOC_DOCUMENTS:
        handle_media(file, folder / 'documents' / 'DOC')
    for file in parser.DOCX_DOCUMENTS:
        handle_media(file, folder / 'documents' / 'DOCX')
    for file in parser.TXT_DOCUMENTS:
        handle_media(file, folder / 'documents' / 'TXT')
    for file in parser.PDF_DOCUMENTS:
        handle_media(file, folder / 'documents' / 'PDF')
    for file in parser.XLSX_DOCUMENTS:
        handle_media(file, folder / 'documents' / 'XLSX')
    for file in parser.PPTX_DOCUMENTS:
        handle_media(file, folder / 'documents' / 'PPTX')
    for file in parser.OTHER_FILES:
        handle_other(file, folder / 'other_files')
    for file in parser.ARCHIVES:
        handle_archive(file, folder / 'archives')

    for folder in parser.FOLDERS[::-1]:
        handle_folder(folder)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(threadName)s %(message)s')
    event_reader = threading.Event()
    files_queue = queue.Queue()

    list_files = Path('.').joinpath('garbage').glob('**/*')

    for file in list_files:
        files_queue.put(file)

    if files_queue.empty():
        logging.info('Folder is empty')
    else:
        if sys.argv[1]:
            folder_for_scan = Path(sys.argv[1])
        concat = Concat(folder_for_scan, event_reader)
        thread_concat = threading.Thread(target=concat, name='Concat')
        thread_concat.start()

        threads = []
        for i in range(2):
            threads_reader = threading.Thread(target=reader, args=(concat.work_order, ), name=f'reader-{i}')
            threads.append(threads_reader)
            threads_reader.start()

        [thread.join() for thread in threads]
        event_reader.set()
