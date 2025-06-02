#!/usr/bin/env python3

"""
Script, takes arguments of full title, and title language
Converts casing of split title_art to Python title(), and language to Python lower()
Loads TITLE_ARTICLES dict, which returns a list of title articles relevant to supplied language
Script matches title_art to language key's value list, if possible
Where found returns Title/Title article, where not found returns None

2021
"""

from typing import Final

TITLE_ARTICLES: Final = {
    "af": ["Die ", "'N ", "DIE ", "'N "],
    "sq": ["Nji ", "Një ", "NJI ", "NJË "],
    "ar": [
        "El-",
        "Ad-",
        "Ag-",
        "Ak-",
        "An-",
        "Ar-",
        "As-",
        "At-",
        "Az-",
        "EL-",
        "AD-",
        "AG-",
        "AK-",
        "AN-",
        "AR-",
        "AS-",
        "AT-",
        "AZ-",
    ],
    "da": ["Den ", "Det ", "De ", "En ", "Et ", "DEN ", "DET ", "DE ", "EN ", "ET "],
    "nl": [
        "De ",
        "Het ",
        "'S ",
        "Een ",
        "Eene ",
        "'N ",
        "DE ",
        "HET ",
        "EEN ",
        "EENE ",
    ],
    "en": ["The ", "A ", "An ", "THE ", "AN "],
    "fr": [
        "Le ",
        "La ",
        "L'",
        "Les ",
        "Un ",
        "Une ",
        "LE ",
        "LA ",
        "LES ",
        "UN ",
        "UNE ",
    ],
    "de": [
        "Der ",
        "Die ",
        "Das ",
        "Ein ",
        "Eine ",
        "DER ",
        "DIE ",
        "DAS ",
        "EIN ",
        "EINE ",
    ],
    "el": [
        "Ho ",
        "He ",
        "To ",
        "Hoi ",
        "Hai ",
        "Ta ",
        "Henas ",
        "Heis ",
        "Mia ",
        "Hena ",
        "HO ",
        "HE ",
        "TO ",
        "HOI ",
        "HAI ",
        "TA ",
        "HENAS ",
        "HEIS ",
        "MIA ",
        "HENA ",
    ],
    "he": ["Ha-", "Ho-", "HA-", "HO-"],
    "hu": ["A ", "Az ", "Egy ", "AZ ", "EGY "],
    "is": [
        "Hinn ",
        "Hin ",
        "Hid ",
        "Hinir ",
        "Hinar ",
        "HINN ",
        "HIN ",
        "HID ",
        "HINIR ",
        "HINAR ",
    ],
    "it": [
        "Il ",
        "La ",
        "Lo ",
        "I ",
        "Gli ",
        "Gl'",
        "Le ",
        "L'",
        "Un ",
        "Uno ",
        "Una ",
        "Un'",
        "IL ",
        "LA ",
        "LO ",
        "GLI ",
        "GL'",
        "LE ",
        "UN ",
        "UNO ",
        "UNA ",
        "UN'",
    ],
    "nb": ["Den ", "Det ", "De ", "En ", "Et ", "DEN ", "DET ", "DE ", "EN ", "ET "],
    "nn": [
        "Dent ",
        "Det ",
        "Dei ",
        "Ein ",
        "Ei ",
        "Eit ",
        "DENT ",
        "DET ",
        "DEI ",
        "EIN ",
        "EI ",
        "EIT ",
    ],
    "pt": ["O ", "A ", "Os ", "As ", "Um ", "Uma ", "OS ", "AS ", "UM ", "UMA "],
    "ro": ["Un ", "Una ", "O ", "UN ", "UNA "],
    "es": [
        "El ",
        "La ",
        "Lo ",
        "Los ",
        "Las ",
        "Un ",
        "Una ",
        "EL ",
        "LA ",
        "LO ",
        "LOS ",
        "LAS ",
        "UN ",
        "UNA ",
    ],
    "ca": [
        "El ",
        "La ",
        "L'",
        "Els ",
        "Les ",
        "Lo ",
        "Los ",
        "Un ",
        "Una ",
        "Uns ",
        "Unes ",
        "EL ",
        "LA ",
        "ELS ",
        "LES ",
        "LO ",
        "LOS ",
        "UN ",
        "UNA ",
        "UNS ",
        "UNES ",
    ],
    "sv": ["Den ", "Det ", "De ", "En ", "Ett ", "DEN ", "DET ", "DE ", "EN ", "ETT "],
    "tr": ["Bir ", "BIR "],
    "cy": ["Y ", "Yr ", "YR "],
    "yi": [
        "Der ",
        "Di ",
        "Die ",
        "Dos ",
        "Das ",
        "A ",
        "An ",
        "Eyn ",
        "Eyne ",
        "DER ",
        "DI ",
        "DIE ",
        "DOS ",
        "DAS ",
        "AN ",
        "EYN ",
        "EYNE ",
    ],
}


def splitter(title_supplied: str, language: str) -> tuple[str, str]:
    """
    Checks for article prefix in supplied title
    and given language code. Splits if match found
    """

    title: str = ""
    title_art: str = ""

    # Counts words in the supplied title:
    title_strip: str = title_supplied.strip()
    count: int = 1 + title_strip.count(" ")

    # Prepares title, splitting into title and title_art
    language = language.lower()
    if count == 1:
        if "ar" in language or "he" in language:
            title_supplied = title_supplied.capitalize()
            # Split here on the first word - hyphen
            if title_supplied.startswith(
                (
                    "El-",
                    "Ad-",
                    "Ag-",
                    "Ak-",
                    "An-",
                    "Ar-",
                    "As-",
                    "At-",
                    "Az-",
                    "Ha-",
                    "Ho-" "EL-",
                    "AD-",
                    "AG-",
                    "AK-",
                    "AN-",
                    "AR-",
                    "AS-",
                    "AT-",
                    "AZ-",
                    "HA-",
                    "HO-",
                )
            ):
                title_art_split = title_supplied.split("-")
                title_art = title_art_split[0]
                title = f"{title_art_split[1]}"
        elif "it" in language or "fr" in language:
            title_supplied = title_supplied.capitalize()
            # Split on the first word apostrophe where present
            if title_supplied.startswith(("L'", "Un'", "Gl'", "UN'", "GL'")):
                title_art_split = title_supplied.split("'")
                title_art = f"{title_art_split[0]}'"
                title = f"{title_art_split[1]}"
        else:
            title = title_supplied
            title_art = ""

    elif count > 1:
        ttl = []
        title_split = title_supplied.split()
        title_art_split = title_split[0]
        title_art_split = title_art_split.capitalize()
        if "ar" in language or "he" in language:
            # Split here on the first word - hyphen
            if title_art_split.startswith(
                (
                    "El-",
                    "Ad-",
                    "Ag-",
                    "Ak-",
                    "An-",
                    "Ar-",
                    "As-",
                    "At-",
                    "Az-",
                    "Ha-",
                    "Ho-" "EL-",
                    "AD-",
                    "AG-",
                    "AK-",
                    "AN-",
                    "AR-",
                    "AS-",
                    "AT-",
                    "AZ-",
                    "HA-",
                    "HO-",
                )
            ):
                article_split = title_art_split.split("-")
                title_art = str(article_split[0])
                ttl.append(article_split[1])
                ttl += title_split[1:]
                title = " ".join(ttl)
        elif "it" in language or "fr" in language:
            # Split on the first word apostrophe where present
            if title_art_split.startswith(("L'", "Un'", "Gl'", "UN'", "GL'")):
                article_split = title_art_split.split("'")
                title_art = f"{article_split[0]}'"
                ttl.append(article_split[1])
                ttl += title_split[1:]
                title_join = " ".join(ttl)
                title = title_join.strip()
            else:
                ttl = title_split[1:]
                title_art = title_split[0]
                title = " ".join(ttl)
        else:
            ttl = title_split[1:]
            title_art = title_split[0]
            title = " ".join(ttl)

    # Searches through keys for language match
    for key in TITLE_ARTICLES.keys():
        if language == str(key):
            lst = []
            lst = TITLE_ARTICLES[language]

            # Looks to match title_art with values in language key match
            for item in zip(lst):
                if len(title_art) > 0:
                    title_art = title_art.capitalize()
                    if title_art in str(item):
                        title_art = title_art.title()
                        title = title[0].upper() + title[1:]
                        if title.isupper():
                            title = title.title()
                            return title, title_art
                        else:
                            return title, title_art

    # Returns titles where no article language matches
    for key in TITLE_ARTICLES.keys():
        if language != str(key):
            return title_supplied, ""
