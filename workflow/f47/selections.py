#!/usr/bin/env python3

'''
Class needed for selecta.py
script functionality
'''

import csv


class Selections():

    def __init__(self, input_file):
        self.input_file = input_file
        self.fields = next(self.read())

    def read(self):
        with open(self.input_file) as f:
            rows = csv.reader(f)
            for r in rows:
                yield r

    def _get(self, column):
        data = self.read()
        next(data)

        for r in data:
            i = r[column]
            yield i

    def _write(self, data):
        with open(self.input_file, 'a') as of:
            writer = csv.writer(of)
            writer.writerow(data)

    def add(self, **kwargs):
        d = {i: kwargs[i] for i in ['package_number', 'can_ID'] if kwargs[i]}
        for type_ in sorted(d, reverse=True):
            id_ = d[type_]

            if not self.is_selected(id_, type_):
                row = []
                for f in self.fields:
                    if f in kwargs:
                        row.append(kwargs[f])
                    else:
                        row.append(None)

                self._write(row)

    def is_selected(self, id_, type_):
        d = {'can_ID': 0, 'package_number': 1}

        if id_ in self._get(d[type_]):
            return True
        else:
            return False

    def list_packages(self):
        return list(self._get(1))

    def list_cans(self):
        return list(self._get(0))

    def list_items(self):
        items = []

        rows = self.read()
        next(rows)
        for i in rows:
            data = i[8].split(',')
            items.extend(data)

        return items
