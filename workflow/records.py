#!/usr/bin/env python3

"""
Script called by workflow.py for
creation of OFCOM workflow records

Updating to Python3.11
"""

# Public packges
from lxml import etree


class Field:
    # Possibly works okay, needs live test
    def __init__(self, xml=None, name=None, text=None, lang=None, field_type=str):
        super(Field, self).__setattr__("data", {"values": {}, "field_type": field_type})

        if xml is not None:
            self.from_xml(xml)
        else:
            if not field_type:
                raise ValueError("argument: field_type is required")

            if lang is None:
                lang = self._update_language()

            self.data["name"] = name
            self.data["values"] = {lang: text}

    def __getattr__(self, key):
        if key == "text":
            values = [self.data["values"][i] for i in self.data["values"]]
            if len(values) > 1:
                for i in ["", "neutral"]:
                    try:
                        return self.data["values"][i]
                    except Exception:
                        pass

            return values[0]
        elif key == "fields":
            return None
        else:
            return self.data[key]

    def __setattr__(self, key, value):
        if key == "text":
            if isinstance(value, (str, int)):
                lang = self._update_language()
                self.data["values"] = {lang: value}
            else:
                self.data["values"] = value
        else:
            raise ValueError("attribute: {} is not available".format(key))

    def _update_language(self):
        field_attributes = {"linked": "", "enumeration": "neutral"}
        if self.data["field_type"] in field_attributes:
            return field_attributes[self.data["field_type"]]

    def from_xml(self, xml):
        if isinstance(xml, str):
            xml = etree.fromstring(xml)

        self.data["name"] = xml.tag
        if xml.text is not None:
            self.data["values"] = {None: xml.text}

        attributes_fields = {"": "linked", "neutral": "enumeration"}

        for element in xml:
            lang = element.get("lang")
            if lang in attributes_fields:
                self.data["field_type"] = attributes_fields[lang]
            self.data["values"][lang] = element.text

    def to_xml(self):
        root = etree.Element(self.data["name"])

        for i in self.data["values"]:
            if i is not None:
                e = etree.Element("value")
                e.attrib["lang"] = i
                e.text = self.data["values"][i]
                root.append(e)
            else:
                root.text = self.data["values"][i]

        return root


class Group:
    # Possibly works okay, needs live test
    def __init__(self, xml=None, name=None, fields=None):
        self.fields = []

        if xml is not None:
            self.from_xml(xml)
        else:
            self.name = name

            if fields:
                if not isinstance(fields, list):
                    raise TypeError("argument: fields must be type: list")

                for item in fields:
                    self.append(**item)

    def __getitem__(self, k):
        return [i for i in self.fields if i.name == k]

    def append(self, **kwargs):
        self.fields.append(Field(**kwargs))

    def from_xml(self, xml):
        if isinstance(xml, str):
            xml = etree.fromstring(xml)

        self.name = xml.tag
        self.xml = xml

        for item in self.xml:
            self.fields.append(Field(item))

    def get(self, k):
        pass

    def to_xml(self, to_string=False):
        root = etree.Element(self.name)
        for i in self.fields:
            root.append(i.to_xml())

        if to_string:
            return etree.tostring(root, pretty_print=to_string)
        else:
            return root


class Record:
    # Possibly works okay, needs live test
    def __init__(self, xml=None, data=None):
        self.fields = []
        self.xml = xml

        if xml:
            self.from_xml(xml)
        elif data:
            self.from_data(data)

    def __getitem__(self, k):
        return [i for i in self.fields if i.name == k]

    def get(self, k):
        """
        Get all fields in any group matching field k
        """

        matches = []

        for element in self.fields:
            if type(element).__name__ == "Group":
                for f in element.fields:
                    if f.name == k:
                        matches.append(f)
            else:
                if element.name == k:
                    matches.append(element)

        return matches

    def _is_grouped(self, field):
        """
        Determine if field is grouped by checking if it has child node <value>
        """

        for child in field:
            if child.tag == "value":
                return False

        return True

    def ungrouped(self):
        fields = []

        for i in self.fields:
            if i.fields is not None:
                for f in i.fields:
                    fields.append(f)
            else:
                fields.append(i)

        return fields

    def from_data(self, data):
        if not isinstance(data, list):
            data = [data]

        for d in data:
            k = d.keys()[0]
            v = d[k]

            if isinstance(v, dict):
                # Grouped
                names = v.keys()
                fields = [{"name": n, "text": v[n]} for n in names]
                self.append(field=Group(name=k, fields=fields))
            else:
                # Ungrouped
                self.append(field=Field(name=k, text=v))

    def from_xml(self, xml):
        if isinstance(xml, str):
            xml = etree.fromstring(xml)

        for i in xml:
            grouped = self._is_grouped(i)
            if grouped:
                self.fields.append(Group(xml=i))
            else:
                self.fields.append(Field(xml=i))

    def append(self, xml=None, field=None):
        if xml is not None:
            self.from_xml(xml)
        else:
            self.fields.append(field)

    def to_xml(self, to_string=False):
        root = etree.Element("record")

        for i in self.fields:
            root.append(i.to_xml())

        if to_string:
            return etree.tostring(root, pretty_print=to_string)
        else:
            return root

    def save(self, database=None):
        """
        Save changes to record with updaterecord
        """
        pass
