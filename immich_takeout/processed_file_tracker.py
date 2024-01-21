import os
import json


class ProcessedFileTracker(object):
    def __init__(self, filename):
        self.filename = filename
        self.items = set()

    def read_file(self):
        if os.path.exists(self.filename):
            with open(self.filename, "r") as fle:
                self.items = set(json.load(fle))

    def write_file(self):
        with open("uploaded.json", "w") as fle:
            json.dump(list(self.items), fle)

    def add(self, name):
        self.items.add(name)
        if len(self.items) % 100 == 0:
            self.write_file()

    def __contains__(self, name):
        return name in self.items

    def __len__(self):
        return len(self.items)
