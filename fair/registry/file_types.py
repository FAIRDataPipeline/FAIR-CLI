import json
import os

FILE_TYPES = json.load(
    open(os.path.join(os.path.dirname(__file__), "file_formats.json"))
)
