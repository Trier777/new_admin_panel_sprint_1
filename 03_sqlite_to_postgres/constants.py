from dotenv import load_dotenv
from os import path
import os


load_dotenv()


DB_PATH = os.environ.get('DB_PATH')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')
UPLOAD_PORTION = os.environ.get('UPLOAD_PORTION')
LOGGING_PATH = os.environ.get('LOGGING_PATH')

DSL = {'dbname': DB_NAME, 'user': DB_USER, 'password': DB_PASSWORD, 'host': DB_HOST, 'port': DB_PORT}


def get_script_dir() -> str:
    abs_path = path.abspath(__file__)  # полный путь к файлу скрипта
    return path.dirname(abs_path)


LOG_FILE = get_script_dir() + path.sep + LOGGING_PATH


DB_FILE = get_script_dir() + path.sep + DB_PATH
