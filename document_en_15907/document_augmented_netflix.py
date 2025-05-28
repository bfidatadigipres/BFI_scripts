#!/usr/bin/ python3

'''
Script to ingest
Netflix EPG metadata
based on CSV list of
required programmes

Receives CSV input as
sys.argv[1] and iterates
contents to make records

Steps:
1. Read all CSV lines into dictionary
   Each programme seasons must have a
   separate entry with total episodes
2. Iterate looking for folder matches
   with CSV data {article}_{title}
3. Check if eposidic/monographic
   Check for existing CID records that
   match the ID for programme, skip if found.
4. Access JSONs data needed for:
   Monographic work/manifestation/item
   Episodic Series work/work/manifestation/item
5. Build CID records
6. Create CID records
7. Append contributors where available

NOTES: Dependency for cast create_contributors()
       will need review when API updates complete
       Updated for adlib_v3 and new API

2023
'''

# Public packages
import os
import sys
import json
import logging
import datetime
import pandas
import yaml
from typing import Final, Optional, Any, Iterable

# Local packages
from document_augmented_streaming_cast import create_contributors
sys.path.append(os.environ['CODE'])
import adlib_v3 as adlib
import utils

# Global variables
STORAGE: Final = os.environ.get('QNAP_IMAGEN')
NETFLIX: Final = os.path.join(STORAGE, 'NETFLIX')
CAT_ID: Final = os.environ.get('PA_NETFLIX')
ADMIN: Final = os.environ.get('ADMIN')
LOGS: Final = os.path.join(ADMIN, 'Logs')
CODE: Final = os.environ.get('CODE')
GENRE_MAP: Final = os.path.join(CODE, 'document_en_15907/EPG_genre_mapping.yaml')
CONTROL_JSON: Final = os.path.join(LOGS, 'downtime_control.json')
CID_API: Final = utils.get_current_api()
FORMAT: Final = '%Y-%m-%d'

# PATV API details including unique identifiers for Netflix catalogue
URL: Final = os.path.join(os.environ['PATV_STREAM_URL'], f'catalogue/{CAT_ID}/')
URL2: Final = os.path.join(os.environ['PATV_STREAM_URL'], 'asset/')
HEADERS: Final = {
    "accept": "application/json",
    "apikey": os.environ['PATV_KEY']
}

# Setup logging
LOGGER = logging.getLogger('document_augmented_netflix')
HDLR = logging.FileHandler(os.path.join(LOGS, 'document_augmented_netflix.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def read_csv_to_dict(csv_path: str) -> dict[str, list[str]]:
    '''
    Make set of all entries
    with title as key, and value
    to contain all other entries
    as a list (use pandas)
    '''

    data = pandas.read_csv(csv_path)
    data_dct = data.to_dict(orient='list')
    print(data)
    return data_dct


def get_folder_title(article: str, title: str) -> str:
    '''
    Match title to folder naming
    '''

    title = title.replace("/","").replace("'","").replace("&", "and").replace("(","").replace(")","").replace("!", "").replace("’", "")
    if article != '-':
        title = f'{article}_{title.replace(" ", "_")}_'
    else:
        title = f'{title.replace(" ", "_")}_'
    return title


def split_title(title_article: str) -> tuple[str, str]:
    '''
    An exception needs adding for "Die " as German language content
    This list is not comprehensive.
    '''
    if title_article.startswith(("A ", "An ", "Am ", "Al-", "As ", "Az ", "Bir ", "Das ", "De ", "Dei ", "Den ",
                                 "Der ", "Det ", "Di ", "Dos ", "Een ", "Eene", "Ei ", "Ein ", "Eine", "Eit ",
                                 "El ", "el-", "En ", "Et ", "Ett ", "Het ", "Il ", "Na ", "A'", "L'", "La ",
                                 "Le ", "Les ", "Los ", "The ", "Un ", "Une ", "Uno ", "Y ", "Yr ")):
        title_split: list[str] = title_article.split()
        ttl = title_split[1:]
        title = ' '.join(ttl)
        title_art = title_split[0]
        return title, title_art
    else:
        return title_article, '-'


def get_folder_match(foldername: str) -> list[str]:
    '''
    Get full folder path
    from Netflix folder excluding
    any folders that have additional
    title data, eg 'Enola_Holmes_2_'
    '''
    folder_list = [x for x in os.listdir(NETFLIX) if x.startswith(foldername)]
    for fr in folder_list:
        id_ = fr.split(foldername)[-1]
        if '_' in id_:
            print(f"SKIPPING: Title match has additional title items: {fr}")
            folder_list.remove(fr)
    return folder_list


def get_json_files(fpath: str) -> list[str]:
    '''
    Fetch JSON files in folder
    '''
    json_files: list[str] = []

    for root, _, files in os.walk(fpath):
        for file in files:
            if file.endswith('.json'):
                json_files.append(os.path.join(root, file))

    return json_files


def retrieve_json(json_pth: str) -> dict[str, str]:
    '''
    One at a time, retrieve metadata for
    a given programme title, and check
    series number match and enough episodes
    present for supplied episode_num
    '''
    with open(json_pth, 'r') as file:
        data = json.load(file)

    return data


def get_cat_data(data=None) -> dict[Optional[str], Optional[str]]:
    '''
    Get catalogue data and return as dct
    '''
    if data is None:
        data = {}

    c_data: dict[Optional[str], Optional[str]] = {}
    if 'id' in data:
        c_data['cat_id'] = data['id']
    if 'title' in data:
        title_full = data['title']
        title, article = split_title(title_full)
        c_data['title'] = title
        c_data['title_article'] = article
    if 'runtime' in data:
        c_data['runtime'] = data['runtime']
    try:
        c_data['production_year'] = data['productionYear']
    except (IndexError, TypeError, KeyError):
        pass
    try:
        c_data['cert_netflix'] = data['certification']['netflix']
    except (IndexError, TypeError, KeyError):
        pass
    try:
        c_data['cert_bbfc'] = data['certification']['bbfc']
    except (IndexError, TypeError, KeyError):
        pass
    try:
        c_data['writers'] = data['meta']['writers']
    except (IndexError, TypeError, KeyError):
        pass
    try:
        cast_all = data['meta']['cast']
        c_data['cast'] = cast_all.split(',')
    except (IndexError, TypeError, KeyError):
        pass
    try:
        c_data['directors'] = data['meta']['directors']
    except (IndexError, TypeError, KeyError):
        pass
    try:
        genres_all = data['meta']['genres']
        c_data['genres'] = genres_all.split(',')
    except (IndexError, TypeError, KeyError):
        pass
    try:
        c_data['attribute'] = data['attribute']
    except (IndexError, TypeError, KeyError):
        c_data['attribute'] = ''
    try:
        short_desc = data['summary']['short'].replace("\'", "'")
        c_data['d_short'] = short_desc
    except (IndexError, TypeError, KeyError):
        pass
    try:
        med_desc = data['summary']['medium'].replace("\'", "'")
        c_data['d_medium'] = med_desc
    except (IndexError, TypeError, KeyError):
        pass
    try:
        long_desc = data['summary']['long'].replace("\'", "'")
        c_data['d_long'] = long_desc
    except (IndexError, TypeError, KeyError):
        pass
    try:
        c_data['start_date'] = data['availability']['start']
    except (IndexError, TypeError, KeyError):
        c_data['start_date'] = ''
    if 'deeplink' in data:
        for link in data['deeplink']:
            if link['rel'] == 'url':
                c_data['browse_url'] = link['href']
            if link['rel'] == 'watch-url':
                c_data['watch_url'] = link['href']
    try:
        c_data['episode_number'] = data['number']
    except (IndexError, TypeError, KeyError):
        pass
    try:
        c_data['contributors'] = data['contributor']
    except (IndexError, TypeError, KeyError):
        pass

    return c_data


def get_json_data(data=None) -> dict[Optional[str], Optional[str]]:
    '''
    Retrieve data from a PATV JSONs
    and return as dictionary
    '''
    if data is None:
        data = {}

    j_data: dict[Optional[str], Optional[str]] = {}
    if 'id' in data:
        j_data['work_id'] = data['id']
    if 'type' in data:
        j_data['type'] = data['type']
    if 'title' in data:
        title_full = data['title']
        title, article = split_title(title_full)
        j_data['title'] = title
        j_data['title_article'] = article
    if 'productionYear' in data:
        j_data['production_year'] = data['productionYear']
    try:
        j_data['runtime'] = data['runtime']
    except (IndexError, KeyError, TypeError):
        pass
    try:
        j_data['episode_number'] = data['number']
    except (IndexError, KeyError, TypeError):
        pass
    try:
        j_data['episode_total'] = data['total']
    except (IndexError, KeyError, TypeError):
        pass
    if 'category' in data:
        genres: list[str] = []
        for item in data['category']:
            genres.append(item['code'])
        if genres:
            j_data['genres'] = genres
    if 'meta' in data:
        try:
            j_data['episode_number'] = data['meta']['episode']
        except (IndexError, TypeError, KeyError):
            pass
        try:
            j_data['episode_total'] = data['meta']['episodeTotal']
        except (IndexError, TypeError, KeyError):
            pass
    if 'certification' in data:
        try:
            j_data['cert_netflix'] = data['certification']['netflix']
        except (IndexError, TypeError, KeyError):
            pass
        try:
            j_data['cert_bbfc'] = data['certification']['bbfc']
        except (IndexError, TypeError, KeyError):
            pass
    if 'summary' in data:
        try:
            short_desc = data['summary']['short'].replace("\'", "'")
            j_data['d_short'] = short_desc
        except (IndexError, TypeError, KeyError):
            pass
        try:
            med_desc = data['summary']['medium'].replace("\'", "'")
            j_data['d_medium'] = med_desc
        except (IndexError, TypeError, KeyError):
            pass
        try:
            long_desc = data['summary']['long'].replace("\'", "'")
            j_data['d_long'] = long_desc
        except (IndexError, TypeError, KeyError):
            pass
    if 'contributor' in data:
        try:
            j_data['contributors'] = data['contributor']
        except (IndexError, TypeError, KeyError):
            pass
    if 'vod' in data:
        try:
            j_data['start_date'] = data['vod']['netflix-uk']['start']
        except (IndexError, TypeError, KeyError):
            pass
    return j_data


def cid_check_works(patv_id: str) -> Optional[tuple[int, str, str, str, list[str], list[str]]]:
    '''
    Sends CID request for series_id data
    '''

    query: str = f'alternative_number="{patv_id}"'
    try:
        hits, record = adlib.retrieve_record(CID_API, 'works', query, 0)
    except Exception as err:
        LOGGER.warning("cid_check_works(): Unable to access series data from CID using Series ID: %s\n%s", patv_id, err)
        print("cid_check_works(): Record not found. Series hit count and series priref will return empty strings")
        return None
    if hits is None:
        LOGGER.exception('"CID API was unreachable for Works search: %s', query)
        raise Exception(f"CID API was unreachable for Works search: {query}")
    try:
        priref: str = adlib.retrieve_field_name(record[0], 'priref')[0]
        print(f"cid_check_works(): Series priref: {priref}")
    except Exception as err:
        priref = ''
    try:
        title: str = adlib.retrieve_field_name(record[0], 'title')[0]
        print(f"cid_check_works(): Series title: {title}")
    except Exception as err:
        title = ''
    try:
        title_art: str = adlib.retrieve_field_name(record[0], 'title_article')[0]
        print(f"cid_check_works(): Series title: {title_art}")
        if title_art is None:
            title_art = ''
    except Exception as err:
        title_art = ''

    groupings: list[str] = []
    for num in range(0, hits):
        try:
            grouping = adlib.retrieve_field_name(record[num], 'grouping.lref')[0]
            print(f"cid_check_works(): Grouping: {grouping}")
            groupings.append(grouping)
        except (IndexError, TypeError, KeyError):
            pass

    alt_type: list[str] = []
    for num in range(0, hits):
        try:
            all_priref = adlib.retrieve_field_name(record[num], 'priref')[0]
        except (IndexError, TypeError, KeyError):
            return None

        type_query = f'priref="{all_priref}"'
        hits, type_record = adlib.retrieve_record(CID_API, 'works', type_query, 1)
        if hits is None:
            LOGGER.exception('"CID API was unreachable for Works search: %s', type_query)
            raise Exception(f"CID API was unreachable for Works search: {type_query}")
        try:
            alt_num_type = adlib.retrieve_field_name(type_record[0]['Alternative_number'][0], 'alternative_number.type')[0]
            print(f"cid_check_works(): Alternative number type {alt_num_type}")
            alt_type.append(alt_num_type)
        except (IndexError, TypeError, KeyError):
            pass

    return hits, priref, title, title_art, groupings, alt_type


def genre_retrieval(category_code: str, description: str, title: str) -> list[str, str]:
    '''
    Retrieve genre data, return as list
    '''
    with open(GENRE_MAP, 'r') as files:
        data = yaml.load(files, Loader=yaml.FullLoader)
        print(f"genre_retrieval(): The genre data is being retrieved for: {category_code}")
        for _ in data:
            if category_code in data['genres']:
                genre_one = []
                genre_two = []
                try:
                    genre_one = data['genres'][category_code.strip('u')]['Genre']
                    print(f"genre_retrieval(): Genre one: {genre_one}")
                    if "Undefined" in genre_one:
                        print(f"genre_retrieval(): Undefined category_code discovered: {category_code}")
                        with open(os.path.join(ADMIN, 'off_air_tv/redux_undefined_genres.txt'), 'a') as genre_log:
                            print("genre_retrieval(): Writing Undefined category details to genre log")
                            genre_log.write("\n")
                            genre_log.write(f"Category: {category_code}     Title: {title}     Description: {description}")
                        genre_one_priref = ''
                    else:
                        for val in genre_one.values():
                            genre_one_priref = val
                        print(f"genre_retrieval(): Key value for genre_one_priref: {genre_one_priref}")
                except Exception:
                    genre_one_priref = ''
                try:
                    genre_two = data['genres'][category_code.strip('u')]['Genre2']
                    for key, val in genre_two.items():
                        genre_two_priref = val
                    print(f"genre_retrieval(): Key value for genre_two_priref: {genre_two_priref}")
                except Exception:
                    genre_two_priref = ''
                return [genre_one_priref, genre_two_priref]
            else:
                LOGGER.warning("%s -- New category not in EPG_genre_map.yaml: %s", category_code, title)
                with open(os.path.join(ADMIN, 'off_air_tv/redux_undefined_genres.txt'), 'a') as genre_log:
                    print("genre_retrieval(): Writing Undefined category details to genre log")
                    genre_log.write("\n")
                    genre_log.write(f"Category: {category_code}     Title: {title}     Description: {description}")


def make_work_dictionary(episode_no: str, csv_data: dict[str, str], cat_dct: Optional[dict[str, str]], json_dct: dict[str, str]) -> dict[str, str]:
    '''
    Build up work data into dictionary for Work creation
    '''
    if not cat_dct:
        cat_dct: dict[str, str ] = {}
    if not json_dct:
        json_dct: dict[str, str] = {}

    work_dict: dict[str, str] = {}
    if 'title' in cat_dct:
        work_dict['title'] = cat_dct['title']
    elif 'title' in json_dct:
        work_dict['title'] = json_dct['title']
    if 'title_article' in cat_dct:
        work_dict['title_article'] = cat_dct['title_article']
    elif 'title_article' in json_dct:
        work_dict['title_article'] = json_dct['title_article']
    if int(csv_data[5]) > 0:
        work_dict['series_num'] = csv_data[5]
        work_dict['episode_total'] = csv_data[7]

    # Film, programme or series
    if 'series' in csv_data[4].lower():
        work_dict['work_type'] = 'T'
    else:
        work_dict['work_type'] = 'F'
    if 'non-fiction' in csv_data[3].lower():
        work_dict['nfa_category'] = 'D'
    else:
        work_dict['nfa_category'] = 'F'
    if episode_no and int(episode_no) > 0:
        work_dict['episode_num'] = episode_no

    if 'runtime' in cat_dct:
        work_dict['runtime'] = cat_dct['runtime']
    elif 'runtime' in json_dct:
        work_dict['runtime'] = json_dct['runtime']
    desc_list: list[str] = []
    if 'd_short' in cat_dct:
        work_dict['d_short'] = cat_dct['d_short']
        desc_list.append(cat_dct['d_short'])
    elif 'd_short' in json_dct:
        work_dict['d_short'] = json_dct['d_short']
        desc_list.append(json_dct['d_short'])
    else:
        desc_list.append('')
    if 'd_medium' in cat_dct:
        work_dict['d_medium'] = cat_dct['d_medium']
        desc_list.append(cat_dct['d_medium'])
    elif 'd_medium' in json_dct:
        work_dict['d_medium'] = json_dct['d_medium']
        desc_list.append(json_dct['d_medium'])
    else:
        desc_list.append('')
    if 'd_long' in cat_dct:
        work_dict['d_long'] = cat_dct['d_long']
        desc_list.append(cat_dct['d_long'])
    elif 'd_long' in json_dct:
        work_dict['d_long'] = json_dct['d_long']
        desc_list.append(json_dct['d_long'])
    else:
        desc_list.append('')
    desc_list.sort(key=len, reverse=True)
    description = desc_list[0]
    if len(description) > 0:
        work_dict['description'] = description

    try:
        work_dict['patv_id'] = json_dct['work_id']
    except (IndexError, TypeError, KeyError):
        work_dict['patv_id'] = ''

    try:
        work_dict['cat_id'] = cat_dct['cat_id']
    except (IndexError, TypeError, KeyError):
        work_dict['cat_id'] = ''

    if 'production_year' in json_dct:
        work_dict['production_year'] = json_dct['production_year']
    elif 'production_year' in cat_dct:
        work_dict['production_year'] = cat_dct['production_year']
    if 'cert_netflix' in json_dct:
        work_dict['certification_netflix'] = json_dct['cert_netflix']
    elif 'cert_netflix' in cat_dct:
        work_dict['certification_netflix'] = cat_dct['cert_netflix']
    if 'cert_bbfc' in json_dct:
        work_dict['certification_bbfc'] = json_dct['cert_bbfc']
    elif 'cert_bbfc' in cat_dct:
        work_dict['certification_bbfc'] = cat_dct['cert_bbfc']
    if csv_data[8]:
        work_dict['acquisition_date'] = csv_data[8]

    if 'genres' in json_dct:
        genres = json_dct['genres']
        all_genre = []
        all_subject = []
        for gen in genres:
            gen1, gen2, sub1, sub2 = genre_retrieval_term(gen, description, csv_data[1])
            if gen1:
                all_genre.append(str(gen1))
            if gen2:
                all_genre.append(str(gen2))
            if sub1:
                all_subject.append(str(sub1))
            if sub2:
                all_subject.append(str(sub2))
        if len(all_genre) > 0:
            work_dict['genres'] = all_genre
        if len(all_subject) > 0:
            work_dict['subjects'] = all_subject
    if 'start_date' in json_dct:
        work_dict['title_date_start'] = json_dct['start_date'][:10]
    elif 'start_date' in cat_dct:
        work_dict['title_date_start'] = cat_dct['start_date'][:10]
    else:
        work_dict['title_date_start'] = ''

    if 'black-and-white' in str(cat_dct):
        work_dict['colour_manifestation'] = 'B'
    else:
        work_dict['colour_manifestation'] = 'C'
    if 'attribute' in cat_dct:
        work_dict['attribute'] = cat_dct['attribute']
    if 'browse_url' in cat_dct:
        work_dict['browse_url'] = cat_dct['browse_url']
    if 'watch_url' in cat_dct:
        work_dict['watch_url'] = cat_dct['watch_url']
    if 'contributors' in json_dct:
        work_dict['contributors'] = json_dct['contributors']
    return work_dict


def main():
    '''
    Retrieve CSV path from sys.argv[1]
    Load into Python dictionary to iterate
    and create CID work/man/item records.
    Check in CID for alternative_number
    matches to episodes, in case of
    repeat runs of CSV and skip.
    Retrieve metadata by title matching
    to NETFLIX programme folders
    Where an episodic series, create a
    series work. Link all records as needed.
    '''
    if not utils.check_control('pause_scripts'):
        LOGGER.info('Script run prevented by downtime_control.json. Script exiting.')
        sys.exit('Script run prevented by downtime_control.json. Script exiting.')
    if not utils.cid_check(CID_API):
        LOGGER.critical("* Cannot establish CID session, exiting script")
        sys.exit("* Cannot establish CID session, exiting script")

    csv_path = sys.argv[1]
    if not os.path.isfile(csv_path):
        sys.exit(f"Problem with supplied CSV path {csv_path}")

    prog_dct: dict[str, list[str]] = read_csv_to_dict(csv_path)
    csv_range = len(prog_dct['title'])
    LOGGER.info("=== Document augmented Netflix start ===============================")
    for num in range(0, csv_range):
        # Capture CSV supplied data to vars
        title = prog_dct['title'][num]
        article = prog_dct['article'][num]
        nfa = prog_dct['nfa'][num]
        level = prog_dct['level'][num]
        season_num = int(prog_dct['series_number'][num])
        genres = prog_dct['genre'][num]
        episode_num = int(prog_dct['episode_number'][num])
        platform = prog_dct['platform'][num]
        year_release = prog_dct['year_of_release'][num]
        acquisition_date = prog_dct['acquisition_date'][num]
        episode = prog_dct['episode'][num]
        print(article, title, nfa, level, season_num, genres, episode_num, platform, year_release, acquisition_date)
        print(f"Episode only wanted: {episode}")

        if platform != 'Netflix':
            continue

        LOGGER.info("** Processing item: %s %s", article, title)

        # Make season number a list
        csv_data: list[str] = [year_release, title, article, nfa, level, season_num, genres, episode_num, acquisition_date]

        # Match NETFLIX folder to article/title
        foldertitle: str = get_folder_title(article, title)
        matched_folders: list[str] = get_folder_match(foldertitle)
        if len(matched_folders) > 1:
            title_count = len(title.split(" ")) + 1
            if len(article) > 0:
                title_count = title_count + 1
            for fold in matched_folders:
                folder_length = len(fold.split('_'))
                if title_count == folder_length:
                    matched_folders = [fold]
            if len(matched_folders) != 1:
                print(f"More than one entry found for {article} {title}. Manual assistance needed.\n{matched_folders}")
                continue
        if len(matched_folders) == 0:
            print(f"No match found: {article} {title}")
            # At some point initiate 'title' search in PATV data
            continue

        print(f"TITLE MATCH: {article} {title} -- {matched_folders[0]}")
        patv_id: str = matched_folders[0].split('_')[-1]

        # Create Work/Manifestation if film/programme
        if 'film' in level.lower() or 'programme' in level.lower():
            prog_path = os.path.join(NETFLIX, matched_folders[0])

            # Check CID work exists / Make work if needed
            hits, priref_work, work_title, work_title_art, groupings, alt_type = cid_check_works(patv_id)
            if int(hits) > 0:
                if '400947' in str(groupings):
                    print(f"SKIPPING PRIREF FOUND: {priref_work}")
                    LOGGER.info("Skipping this item, likely already has CID record: %s", priref_work)
                    continue
                if 'PATV asset id' in str(alt_type):
                    LOGGER.warning("STORA PATV id found to match work priref for this title: %s", priref_work)
                if 'PATV Amazon asset id' in str(alt_type):
                    LOGGER.warning("Amazon PATV id found to match work priref for this title: %s", priref_work)

            # Retrieve all available
            mono_cat: list[str] = [ x for x in os.listdir(prog_path) if x.startswith('mono_catalogue_') ]
            mono: list[str] = [ x for x in os.listdir(prog_path) if x.startswith('monographic_') ]
            try:
                cat_data = retrieve_json(os.path.join(prog_path, mono_cat[0]))
                cat_dct = get_cat_data(cat_data)
            except (IndexError, TypeError, KeyError) as exc:
                print(exc)
                cat_dct = {}
            try:
                mono_data = retrieve_json(os.path.join(prog_path, mono[0]))
                mono_dct = get_json_data(mono_data)
            except (IndexError, TypeError, KeyError) as exc:
                print(exc)
                mono_dct = {}

            if not cat_dct:
                print("SKIPPING: Missing data from JSON files.")
                continue

            # Make monographic work here
            data_dct = make_work_dictionary('', csv_data, cat_dct, mono_dct)
            print(f"Dictionary for monograph creation: \n{data_dct}")
            print("*************")
            record, series_work, work, work_restricted, manifestation, item = build_defaults(data_dct)

            if priref_work.isnumeric():
                print(f"Found priref is for monographic work: {priref_work}")
                print(f"Monograph work already exists for {title}.")
                LOGGER.info("Monograph work exists that is not Netflix origin. Linking repeat to existing work")
            else:
                priref_work = create_work('', '', '', data_dct, record, work, work_restricted)
                if len(priref_work) == 0:
                    LOGGER.warning("Monograph work record creation failed, skipping all further record creations")
                    continue
                print(f"PRIREF MONOGRAPH WORK: {priref_work}")

            # Create contributors if supplied / or in addition to solo contributors
            if 'contributors' in data_dct and len(data_dct['contributors']) >= 1:
                print('** Contributor data found')
                success = create_contributors(priref_work, data_dct['nfa_category'], data_dct['contributors'], 'Netflix')
                if success:
                    LOGGER.info("Contributor data written to Work record: %s", priref_work)
                else:
                    LOGGER.warning("Failure to write contributor data to Work record: %s", priref_work)

            # Make monographic manifestation here
            priref_man = create_manifestation(priref_work, work_title, work_title_art, data_dct, record, manifestation)
            if len(priref_man) == 0:
                LOGGER.warning("Monograph manifestation record creation failed, skipping all further record creations")
                continue
            print(f"PRIREF FOR MANIFESTATION: {priref_man}")
            # Append URLS if present
            if 'watch_url' in data_dct:
                append_url_data(priref_work, priref_man, data_dct)
            # Make monographic item record here
            priref_item = create_item(priref_man, work_title, work_title_art, data_dct, record, item)
            if len(priref_item) == 0:
                LOGGER.warning("Monograph item record creation failed, skipping all further stages")
                continue
            print(f"PRIREF FOR ITEM: {priref_item}")

        elif 'series' in level.lower():
            prog_path = os.path.join(NETFLIX, matched_folders[0])
            json_fpaths = get_json_files(prog_path)
            series_priref = ''
            # Check CID work exists / Make work if needed
            hits, series_priref, work_title, work_title_art, _, alt_type = cid_check_works(patv_id)
            print(f"Work title found: {work_title}")
            if series_priref.isnumeric():
                print(f"Series work already exists for {title}.")
                if 'PATV asset id' in str(alt_type):
                    LOGGER.warning("Series work found is from STORA off-air recording: %s", series_priref)
                if 'PATV Amazon asset id' in str(alt_type):
                    LOGGER.warning("Series work found is from Amazon streaming recording: %s", series_priref)
            else:
                print("Series work does not exist, creating series work now.")
                series_json = [ x for x in os.listdir(prog_path) if x.startswith('series_') and x.endswith('.json')]
                if not len(series_json) == 1:
                    continue

                # Get series ID title and genre
                series_data = retrieve_json(os.path.join(prog_path, series_json[0]))
                series_dct = get_json_data(series_data)
                series_data_dct = make_work_dictionary('', csv_data, None, series_dct)
                record, series_work, work, work_restricted, manifestation, item = build_defaults(series_data_dct)
                work_title, work_title_art = split_title(series_data_dct['title'])


                # Make series work here
                if not series_data_dct:
                    continue
                series_priref = create_series_work(patv_id, series_data_dct, series_work, work_restricted, record)
                if not series_priref:
                    print("Series work creation failure. Skipping episodes...")
                    continue

            season_fpaths = [x for x in json_fpaths if f'season_{season_num}_' in str(x)]

            # Fetch just single episodes
            if episode != 'all':
                if ',' in str(episode):
                    episodes = episode.split(', ')
                    total_eps = len(episodes)
                else:
                    episodes = [episode]
                    total_eps = 1
                count = 0
                for ep in episodes:
                    LOGGER.info("Creating one-off episode record for %s episode number %s", title, ep)
                    success = make_episodes(series_priref, work_title, work_title_art, int(ep), season_fpaths, title, csv_data)
                    if success is None:
                        LOGGER.warning("Failed to make records for episode {num}")
                        continue
                    LOGGER.info("Episode %s made successfully: Work %s Manifestation %s Item %s", num, success[0], success[1], success[2])
                    count += 1
                if total_eps != count:
                    LOGGER.warning("Unable to create all requested records for epsides: %s", episode)
                LOGGER.info("** All records created for %s episodes %s", title, episode)

            # Fetch all episodes in target season
            if episode == 'all':
                episode_count = 0
                for num in range(1, episode_num + 1):
                    success = make_episodes(series_priref, work_title, work_title_art, num, season_fpaths, title, csv_data)
                    if success is None:
                        LOGGER.warning("Failed to make records for episode {num}")
                        continue
                    LOGGER.info("Episode %s made successfully: Work %s Manifestation %s Item %s", num, success[0], success[1], success[2])
                    episode_count += 1

                if episode_count != int(episode_num):
                    LOGGER.warning("Not all episodes created for %s - total episodes %s", title, episode_num)
                    print("============ Episodes found in NETFLIX folder do not match total episodes supplied =============")

    LOGGER.info("=== Document augmented Netflix end =================================")


def make_episodes(series_priref: str, work_title: str, work_title_art: str, num: int, season_fpaths: str, title: str, csv_data: dict[str, list[str]]) -> tuple[str, str, str]:
    '''
    Receive number for episode (individual or
    from range count) and build programme records
    '''

    episode_fpaths = [x for x in season_fpaths if f'episode_{num}_' in str(x) and x.endswith('.json')]
    if not episode_fpaths:
        LOGGER.warning("Cannot find any episode number %s in season path: %s", num, season_fpaths)
        return None

    episode_folder = os.path.basename(os.path.split(episode_fpaths[0])[0])
    episode_id = episode_folder.split('_')[-1]
    print(f"** Episode ID: {episode_id} {title}")

    # Check CID work exists / Make work if needed
    hits, priref_episode, _, _, groupings, alt_type = cid_check_works(episode_id)
    if int(hits) > 0:
        if '400947' in str(groupings):
            print(f"SKIPPING. EPISODE EXISTS IN CID: {priref_episode}")
            LOGGER.info("Skipping episode, already exists in CID: %s", priref_episode)
            return None
        if 'PATV asset id' in str(alt_type):
            LOGGER.warning("Episode work exists from STORA off-air recordings: %s", priref_episode)
        if 'PATV Amazon asset id' in str(alt_type):
            LOGGER.warning("Episode work exists from Amazon streaming platform: %s", priref_episode)
    print("New episode_id found for Work. Linking to series work")

    # Retrieve all available data
    ep_cat_json = [ x for x in episode_fpaths if 'episode_catalogue_' in str(x) ]
    ep_json = [ x for x in episode_fpaths if 'episode_' in str(x) and x.endswith(f"{episode_id}.json") ]
    print(ep_cat_json)
    print(ep_json)

    try:
        ep_cat_data = retrieve_json(ep_cat_json[0])
        ep_cat_dct = get_cat_data(ep_cat_data)
    except (IndexError, TypeError, KeyError) as exc:
        print(exc)
        ep_cat_dct = {}
    try:
        ep_data = retrieve_json(ep_json[0])
        ep_dct = get_json_data(ep_data)
    except (IndexError, TypeError, KeyError) as exc:
        print(exc)
        ep_dct = {}

    # Make episodic work here
    data_dct = make_work_dictionary(num, csv_data, ep_cat_dct, ep_dct)
    print(f"Dictionary for Work creation:\n{data_dct}")
    print('**************')
    record, _, work, work_restricted, manifestation, item = build_defaults(data_dct)
    priref_episode = create_work(series_priref, work_title, work_title_art, data_dct, record, work, work_restricted)
    if len(priref_episode) == 0:
        LOGGER.warning("Episodic Work record creation failed, skipping all further record creations")
        return None
    print(f"Episode work priref: {priref_episode}")

    # Create contributors if supplied / or in addition to solo contributors
    if 'contributors' in data_dct and len(data_dct['contributors']) >= 1:
        print('** Contributor data found')
        success = create_contributors(priref_episode, data_dct['nfa_category'], data_dct['contributors'], 'Netflix')
        if success:
            LOGGER.info("Contributor data written to Work record: %s", priref_episode)
        else:
            LOGGER.warning("Failure to write contributor data to Work record: %s", priref_episode)

    # Make episodic manifestation here
    priref_ep_man = create_manifestation(priref_episode, work_title, work_title_art, data_dct, record, manifestation)
    if len(priref_ep_man) == 0:
        LOGGER.warning("Episodic manifestation record creation failed, skipping all further record creations")
        return None
    print(f"PRIREF EP MANIFESTATION: {priref_ep_man}")

    # Append URLS if present
    if 'watch_url' in data_dct:
        append_url_data(priref_episode, priref_ep_man, data_dct)

    # Make episodic item record here
    priref_ep_item = create_item(priref_ep_man, work_title, work_title_art, data_dct, record, item)
    if len(priref_ep_item) == 0:
        LOGGER.warning("Episodic item record creation failed, skipping onto next stage")
        return None
    print(f"PRIREF FOR ITEM: {priref_ep_item}")
    return priref_episode, priref_ep_man, priref_ep_item


def firstname_split(person: str) -> str:
    '''
    Splits 'firstname surname' and returns 'surname, firstname'
    '''
    name_list = person.split()
    count = len(person.split())
    if count > 2:
        firstname, *rest, surname = name_list
        rest = ' '.join(rest)
        return surname + ", " + firstname + " " + rest
    elif count > 1:
        firstname, surname = name_list
        return surname + ", " + firstname
    else:
        return person


def genre_retrieval_term(category_code: str, description: str, title: str) -> tuple[str]:
    '''
    Check genre yaml to retrieve genre prirefs
    '''
    category_data: list[str, str]  = genre_retrieval(category_code, description, title)
    try:
        genre1 = category_data[0]
    except (IndexError, TypeError, KeyError):
        genre1 = ''
    try:
        genre2 = category_data[1]
    except (IndexError, TypeError, KeyError):
        genre2 = ''
    try:
        subject1 = category_data[2]
    except (IndexError, TypeError, KeyError):
        subject1 = ''
    try:
        subject2 = category_data[3]
    except (IndexError, TypeError, KeyError):
        subject2 = ''

    return (genre1, genre2, subject1, subject2)


def build_defaults(data: dict[str, str]) -> list[dict[str, str]]:
    '''
    Get detailed information
    and build record_defaults dict
    '''
    start_date_str: str = data.get('title_date_start')
    if '-' in start_date_str:
        start_date = datetime.datetime.strptime(start_date_str, FORMAT)
    else:
        start_date = datetime.date.today()
    new_date = start_date + datetime.timedelta(days=2927)
    date_restriction: str = new_date.strftime(FORMAT)

    record: list[dict[str, str]] = ([{'input.name': 'datadigipres'},
        {'input.date': str(datetime.datetime.now())[:10]},
        {'input.time': str(datetime.datetime.now())[11:19]},
        {'input.notes': 'Netflix metadata integration - automated bulk documentation'},
        {'record_access.user': 'BFIiispublic'},
        {'record_access.rights': '0'},
        {'record_access.reason': 'SENSITIVE_LEGAL'},
        {'record_access.user': 'System Management'},
        {'record_access.rights': '3'},
        {'record_access.reason': 'SENSITIVE_LEGAL'},
        {'record_access.user': 'Information Specialist'},
        {'record_access.rights': '3'},
        {'record_access.reason': 'SENSITIVE_LEGAL'},
        {'record_access.user': 'Digital Operations'},
        {'record_access.rights': '2'},
        {'record_access.reason': 'SENSITIVE_LEGAL'},
        {'record_access.user': 'Documentation'},
        {'record_access.rights': '2'},
        {'record_access.reason': 'SENSITIVE_LEGAL'},
        {'record_access.user': 'Curator'},
        {'record_access.rights': '2'},
        {'record_access.reason': 'SENSITIVE_LEGAL'},
        {'record_access.user': 'Special Collections'},
        {'record_access.rights': '2'},
        {'record_access.reason': 'SENSITIVE_LEGAL'},
        {'record_access.user': 'Librarian'},
        {'record_access.rights': '2'},
        {'record_access.reason': 'SENSITIVE_LEGAL'},
        #{'record_access.user': '$REST'},
        #{'record_access.rights': '1'},
        #{'record_access.reason': 'SENSITIVE_LEGAL'},
        {'grouping.lref': '400947'}, # Netflix
        {'language.lref': '74129'},
        {'language.type': 'DIALORIG'}])

    series_work: list[dict[str, str]] = ([{'record_type': 'WORK'},
        {'worklevel_type': 'SERIAL'},
        {'work_type': "T"},
        {'description.type.lref': '100298'},
        {'production_country.lref': '73938'},
        {'nfa_category': data['nfa_category']}])

    work: list[dict[str, str]] = ([{'record_type': 'WORK'},
        {'worklevel_type': 'MONOGRAPHIC'},
        {'work_type': data['work_type']},
        {'description.type.lref': '100298'},
        {'production_country.lref': '73938'},
        {'nfa_category': data['nfa_category']}])

    work_restricted: list[dict[str, str]] = ([{'application_restriction': 'MEDIATHEQUE'},
        {'application_restriction.date': str(datetime.datetime.now())[:10]},
        {'application_restriction.reason': 'STRATEGIC'},
        {'application_restriction.duration': 'PERM'},
        {'application_restriction.review_date': date_restriction},
        {'application_restriction.authoriser': 'kerriganl'},
        {'application_restriction.notes': 'Netflix UK streaming content - pending discussion'}])

    manifestation: list[dict[str, str]] = ([{'record_type': 'MANIFESTATION'},
        {'manifestationlevel_type': 'INTERNET'},
        {'format_high_level': 'Video - Digital'},
        {'format_low_level.lref': '400949'}, # IMF
        {'colour_manifestation': data['colour_manifestation']},
        {'sound_manifestation': 'SOUN'},
        {'transmission_date': data['title_date_start']},
        {'availability.name.lref': '143463'}, # Netflix
        {'transmission_coverage': 'STR'},
        {'vod_service_type.lref': '398712'},
        {'aspect_ratio': '16:9'},
        {'country_manifestation': 'United Kingdom'},
        {'notes': 'Manifestation representing the UK streaming platform release of the Work.'}])

    item: list[dict[str, str]] = ([{'record_type': 'ITEM'},
        {'item_type': 'DIGITAL'},
        {'copy_status': 'M'},
        {'copy_usage.lref': '131560'},
        {'file_type.lref': '401103'}, # IMP
        {'code_type.lref': '400945'}, # Mixed Netflix
        {'accession_date': str(datetime.datetime.now())[:10]},
        {'acquisition.date': data['acquisition_date']},
        {'acquisition.method.lref': '132853'},
        {'acquisition.source.lref': '143463'}, # Netflix
        {'acquisition.source.type': 'DONOR'},
        {'access_conditions': 'Access requests for this collection are subject to an approval process. '\
            'Please raise a request via the Collections Systems Service Desk, describing your specific use.'},
        {'access_conditions.date': str(datetime.datetime.now())[:10]}])

    return (record, series_work, work, work_restricted, manifestation, item)


def create_series_work(patv_id: str, series_dct: dict[str, str], csv_data, series_work: list[dict[str, str]], work_restricted: list[dict[str, str]], record: list[dict[str, str]]) -> dict[str, str]:
    '''
    Build data needed to make
    episodic series work to
    link all episodes to
    [year_release, title, article, nfa, level, season_num, genres, episode_num]
    '''
    series_work_id = None
    series_work_values: list[dict[str, str]] = []
    series_work_values.extend(record)
    series_work_values.extend(series_work)
    series_work_values.extend(work_restricted)

    if 'title' in series_dct:
        title = series_dct['title']
        series_work_values.append({'title': title})
        series_work_values.append({'title.language': 'English'})
        series_work_values.append({'title.type': '05_MAIN'})
    if 'title_article' in series_dct:
        if series_dct['title_article'] != '-' and series_dct['title_article'] != '':
            series_work_values.append({'title.article': series_dct['title_article']})
    if len('patv_id') > 0:
        series_work_values.append({'alternative_number.type': 'PATV Netflix asset ID'})
        series_work_values.append({'alternative_number': patv_id})
    if 'description' in series_dct:
        series_work_values.append({'description': series_dct['description']})
        series_work_values.append({'description.type': 'Synopsis'})
        series_work_values.append({'description.date': str(datetime.datetime.now())[:10]})
    print(f"Series work values:\n{series_work_values}")

    # Start creating CID Work Series record
    series_work_xml = adlib.create_record_data(CID_API, 'works', '', series_work_values)
    try:
        print("Attempting to create CID record")
        work_rec = adlib.post(CID_API, series_work_xml, 'works', 'insertrecord')
        if work_rec:
            try:
                print("Populating series_work_id and object_number variables")
                series_work_id = adlib.retrieve_field_name(work_rec, 'priref')[0]
                object_number = adlib.retrieve_field_name(work_rec, 'object_number')[0]
                print(f'* Series record created with Priref {series_work_id}')
                print(f'* Series record created with Object number {object_number}')
                LOGGER.info('Work record created with priref %s', series_work_id)
            except (IndexError, TypeError, KeyError) as err:
                print("Unable to create series record", err)
                return None
    except Exception as err:
        print(f'* Unable to create Work record for <{title}> {err}')
        LOGGER.critical('Unable to create Work record for <%s>', title)
        return None

    # Append Content genres to record
    series_genres = []
    if 'genres' in series_dct:
        extracted = series_dct['genres']
        for genr in extracted:
            series_genres.append({'content.genre.lref': genr})
    if len(series_genres) > 0:
        genre_xml = adlib.create_grouped_data(series_work_id, 'Content_genre', series_genres)
        print(genre_xml)
        update_rec = adlib.post(CID_API, genre_xml, 'works', 'updaterecord')
        if update_rec is None:
            LOGGER.info("Failed to update genres to Series Work record: %s", series_work_id)
        elif 'Content_genre' in str(update_rec):
            LOGGER.info("Label text successfully updated to Series Work %s", series_work_id)

    # Append Content subject to record
    series_subjects: list[dict[str, str]] = []
    if 'subjects' in series_dct:
        subs = series_dct['subjects']
        for sub in subs:
            series_subjects.append({'content.subject.lref': sub})
    if len(series_subjects) > 0:
        subject_xml = adlib.create_grouped_data(series_work_id, 'Content_subject', series_subjects)
        print(subject_xml)
        update_rec = adlib.post(CID_API, subject_xml, 'works', 'updaterecord')
        if update_rec is None:
            LOGGER.info("Failed to update subjects to Series Work record: %s", series_work_id)
        elif 'Content_subject' in str(update_rec):
            LOGGER.info("Label text successfully updated to Series Work %s", series_work_id)

    # Append Label grouped data to record
    label_fields: list[dict[str, str]] = []
    if 'd_short' in series_dct:
        label_fields.append([{'label.type': 'EPGSHORT'},{'label.text': series_dct['d_short']},{'label.source': 'EBS augmented EPG supply'},{'label.date': str(datetime.datetime.now())[:10]}])
    if 'd_medium' in series_dct:
        label_fields.append([{'label.type': 'EPGMEDIUM'},{'label.text': series_dct['d_medium']},{'label.source': 'EBS augmented EPG supply'},{'label.date': str(datetime.datetime.now())[:10]}])
    if 'd_long' in series_dct:
        label_fields.append([{'label.type': 'EPGLONG'},{'label.text': series_dct['d_long']},{'label.source': 'EBS augmented EPG supply'},{'label.date': str(datetime.datetime.now())[:10]}])
    if len(label_fields) > 0:
        label_xml = adlib.create_grouped_data(series_work_id, 'Label', label_fields)
        print(label_xml)
        update_rec = adlib.post(CID_API, label_xml, 'works', 'updaterecord')
        if update_rec is None:
            LOGGER.info("Failed to update Labels to Series Work record: %s", series_work_id)
        elif 'Label' in str(update_rec):
            LOGGER.info("Label text successfully updated to Series Work %s", series_work_id)

    return series_work_id


def create_work(part_of_priref: str, work_title: str, work_title_art: str, work_dict: list[dict[str, str]], record_def: dict[str,str], work_def: list[dict[str,str]], work_restricted: list[dict[str, str]]) -> str:
    '''
    Build all data needed to make new work.
    work_def from work/series_work defaults
    Hand in series or episode, part_of_priref
    populated as needed.
    '''
    work_id: str = ''
    work_values: list[dict[str, str]] = []
    work_values.extend(record_def)
    work_values.extend(work_def)
    work_values.extend(work_restricted)

    # Add specifics for series/episode or monograph works
    if 'title' in work_dict:
        title_check = work_dict['title']
        if title_check.startswith('Episode ') and len(title_check) < 11:
            work_values.append({'title': f"{work_title} {work_dict['title']}"})
            if work_title_art != '-' and work_title_art != '':
                work_values.append({'title.article': work_title_art})
        else:
            work_values.append({'title': work_dict['title']})
            if 'title_article' in work_dict:
                if work_dict['title_article'] != '-' and work_dict['title_article'] != '':
                    work_values.append({'title.article': work_dict['title_article']})
        work_values.append({'title.language': 'English'})
        work_values.append({'title.type': '05_MAIN'})

    if len(work_dict['title_date_start']) > 0:
        work_values.append({'title_date_start': work_dict['title_date_start']})
        work_values.append({'title_date.type': '03_R'})
    if 'patv_id' in work_dict:
        work_values.append({'alternative_number.type': 'PATV Netflix asset ID'})
        work_values.append({'alternative_number': work_dict['patv_id']})
    if 'cat_id' in work_dict:
        work_values.append({'alternative_number.type': 'PATV Netflix catalogue ID'})
        work_values.append({'alternative_number': work_dict['cat_id']})
    if 'episode_id' in work_dict:
        work_values.append({'alternative_number.type': 'PATV Netflix asset ID'})
        work_values.append({'alternative_number': work_dict['episode_id']})
    if part_of_priref:
        work_values.append({'part_of_reference.lref': part_of_priref})
    if 'episode_num' in work_dict:
        work_values.append({'part_unit': 'EPISODE'})
        work_values.append({'part_unit.value': work_dict['episode_num']})
        work_values.append({'part_unit.valuetotal': work_dict['episode_total']})
    if 'series_num' in work_dict:
        work_values.append({'part_unit': 'SERIES'})
        work_values.append({'part_unit.value': work_dict['series_num']})
    if 'production_year' in work_dict:
        work_values.append({'title_date_start': work_dict['production_year']})
        work_values.append({'title_date.type': '02_P'})
    if 'description' in work_dict:
        work_values.append({'description': work_dict['description']})
        work_values.append({'description.type': 'Synopsis'})
        work_values.append({'description.date': str(datetime.datetime.now())[:10]})
    print(f"Work values:\n{work_values}")

    # Start creating CID Work Series record
    work_xml = adlib.create_record_data(CID_API, 'works', '', work_values)
    try:
        print("Attempting to create CID record")
        work_rec = adlib.post(CID_API, work_xml, 'works', 'insertrecord')
        if work_rec:
            try:
                print("Populating work_id and object_number variables")
                work_id = adlib.retrieve_field_name(work_rec, 'priref')[0]
                object_number = adlib.retrieve_field_name(work_rec, 'object_number')[0]
                print(f'* Work record created with Priref {work_id}')
                print(f'* Work record created with Object number {object_number}')
                LOGGER.info('Work record created with priref %s', work_id)
            except (IndexError, TypeError, KeyError) as err:
                print("Unable to create work record", err)
                return None
    except Exception as err:
        print(f"* Unable to create Work record for <{work_dict['title']}> {err}")
        LOGGER.critical('** Unable to create Work record for <%s>', work_dict['title'])
        return None

    # Append Content genres to record
    work_genres = []
    if 'genres' in work_dict:
        extracted = work_dict['genres']
        for genr in extracted:
            work_genres.append({'content.genre.lref': genr})
    if len(work_genres) > 0:
        genre_xml = adlib.create_grouped_data(work_id, 'Content_genre', work_genres)
        print(genre_xml)
        update_rec = adlib.post(CID_API, genre_xml, 'works', 'updaterecord')
        if update_rec is None:
            LOGGER.info("Failed to update genres to Work record: %s", work_id)
        elif 'Content_genre' in str(update_rec):
            LOGGER.info("Label text successfully updated to Series Work %s", work_id)

    # Append Content subject to record
    work_subjects = []
    if 'subjects' in work_dict:
        subs = work_dict['subjects']
        for sub in subs:
            work_subjects.append({'content.subject.lref': sub})
    if len(work_subjects) > 0:
        subject_xml = adlib.create_grouped_data(work_id, 'Content_subject', work_subjects)
        print(subject_xml)
        update_rec = adlib.post(CID_API, subject_xml, 'works', 'updaterecord')
        if update_rec is None:
            LOGGER.info("Failed to update subjects to Work record: %s", work_id)
        elif 'Content_subject' in str(update_rec):
            LOGGER.info("Label text successfully updated to Series Work %s", work_id)

    # Append Label grouped data to record
    label_fields = []
    if 'd_short' in work_dict:
        label_fields.append([{'label.type': 'EPGSHORT'},{'label.text': work_dict['d_short']},{'label.source': 'EBS augmented EPG supply'},{'label.date': str(datetime.datetime.now())[:10]}])
    if 'd_medium' in work_dict:
        label_fields.append([{'label.type': 'EPGMEDIUM'},{'label.text': work_dict['d_medium']},{'label.source': 'EBS augmented EPG supply'},{'label.date': str(datetime.datetime.now())[:10]}])
    if 'd_long' in work_dict:
        label_fields.append([{'label.type': 'EPGLONG'},{'label.text': work_dict['d_long']},{'label.source': 'EBS augmented EPG supply'},{'label.date': str(datetime.datetime.now())[:10]}])
    if len(label_fields) > 0:
        label_xml = adlib.create_grouped_data(work_id, 'Label', label_fields)
        print(label_xml)
        update_rec = adlib.post(CID_API, label_xml, 'works', 'updaterecord')
        if update_rec is None:
            LOGGER.info("Failed to update Labels to Work record: %s", work_id)
        elif 'Label' in str(update_rec):
            LOGGER.info("Label text successfully updated to Work %s", work_id)

    return work_id


def create_manifestation(work_priref: str, work_title: str, work_title_art: str, work_dict: dict[str, list[str]], record_defaults: list[dict[str, str]], manifestation_defaults: list[dict[str, str]]) -> str:
    '''
    Create a manifestation record,
    linked to work_priref
    '''
    manifestation_id = ''
    print(work_dict)
    manifestation_values = []
    manifestation_values.extend(record_defaults)
    manifestation_values.extend(manifestation_defaults)

    if 'title' in work_dict:
        title_check = work_dict['title']
        if title_check.startswith('Episode ') and len(title_check) < 11:
            manifestation_values.append({'title': f"{work_title} {work_dict['title']}"})
            if len(work_title_art) > 1:
                manifestation_values.append({'title.article': work_title_art})
        else:
            manifestation_values.append({'title': work_dict['title']})
            if 'title_article' in work_dict:
                if work_dict['title_article'] != '-' and work_dict['title_article'] != '':
                    manifestation_values.append({'title.article': work_dict['title_article']})
        manifestation_values.append({'title.language': 'English'})
        manifestation_values.append({'title.type': '05_MAIN'})
    manifestation_values.append({'part_of_reference.lref': work_priref})
    if 'runtime' in work_dict:
        manifestation_values.append({'runtime': int(work_dict['runtime'])})
    if 'episode_id' in work_dict:
        manifestation_values.append({'alternative_number.type': 'PATV Netflix asset ID'})
        manifestation_values.append({'alternative_number': work_dict['episode_id']})
    if 'attribute' in work_dict:
        if work_dict['attribute']:
            atts = ', '.join(work_dict['attribute'])
            manifestation_values.append({'utb.fieldname': 'PATV Netflix attributes'})
            manifestation_values.append({'utb.content': atts})
    if 'certification_netflix' in work_dict:
        manifestation_values.append({'utb.fieldname': 'Netflix certification'})
        manifestation_values.append({'utb.content': work_dict['certification_netflix']})
    if 'certification_bbfc' in work_dict:
        manifestation_values.append({'utb.fieldname': 'BBFC certification'})
        manifestation_values.append({'utb.content': work_dict['certification_bbfc']})
    print(f"Manifestation values:\n{manifestation_values}")

    broadcast_addition = []
    manifestation_xml = adlib.create_record_data(CID_API, 'manifestations', '', manifestation_values)
    try:
        print("Attempting to create CID record")
        man_rec = adlib.post(CID_API, manifestation_xml, 'manifestations', 'insertrecord')
        if man_rec:
            try:
                print("Populating manifestation_id and object_number variables")
                manifestation_id = adlib.retrieve_field_name(man_rec, 'priref')[0]
                object_number = adlib.retrieve_field_name(man_rec, 'object_number')[0]
                print(f'* Manifestation record created with Priref {manifestation_id}')
                print(f'* Manifestation record created with Object number {object_number}')
                LOGGER.info('Manifestation record created with priref %s', manifestation_id)
            except Exception as err:
                print("Unable to create Manifestation record", err)
                return None
    except Exception as err:
        print(f"* Unable to create Manifestation record for <{work_dict['title']}> {err}")
        LOGGER.critical('** Unable to create Manifestation record for <%s>', work_dict['title'])
        return None

    broadcast_addition = [{'broadcast_company.lref': '143463'}]
    broadcast_xml = adlib.create_record_data(CID_API, 'manifestations', manifestation_id, broadcast_addition)
    print("**** Attempting to write work genres to records ****")

    success = adlib.post(CID_API, broadcast_xml, 'manifestations', 'updaterecord')
    if success is None:
        LOGGER.info("Failed to update Broadcast Company data to Manifestation record: %s", manifestation_id)
    LOGGER.info("Broadcast Company data updated to work: %s", manifestation_id)

    return manifestation_id


def append_url_data(work_priref: str, man_priref: str, data=None) -> None:
    '''
    Receive Netflix URLs and priref and append to records
    '''
    url = data['watch_url']
    payload_mid = f"<URL><![CDATA[{url}]]></URL><URL.description>Netflix viewing URL</URL.description>"
    payload_head = f"<adlibXML><recordList><record priref='{man_priref}'><URL>"
    payload_end = "</URL></record></recordList></adlibXML>"
    payload = payload_head + payload_mid + payload_end

    success = adlib.post(CID_API, payload, 'manifestations', 'updaterecord')
    if success is None:
        LOGGER.info("append_url_data(): Failed to update Watch URL data to Manifestation record: %s", man_priref)
    LOGGER.info("append_url_data(): Watch URL data updated to Manifestation: %s", man_priref)

    payload_head = f"<adlibXML><recordList><record priref='{work_priref}'><URL>"
    payload = payload_head + payload_mid + payload_end

    success = adlib.post(CID_API, payload, 'works', 'updaterecord')
    if success is None:
        LOGGER.info("append_url_data(): Failed to update Watch URL data to Work record: %s", work_priref)
    LOGGER.info("append_url_data(): Watch URL data updated to work: %s", work_priref)


def create_item(man_priref: str, work_title: str, work_title_art: str, work_dict: dict[str, list[str]], record_defaults: list[dict[str, str]], item_default: list[dict[str, str]]) -> tuple[str, str]:
    '''
    Create item record,
    link to manifestation
    '''
    item_id: str = ''
    item_object_number: str = ''
    item_values: list[dict[str, str]] = []
    item_values.extend(record_defaults)
    item_values.extend(item_default)
    item_values.append({'part_of_reference.lref': man_priref})
    if 'title' in work_dict:
        title_check = work_dict['title']
        if title_check.startswith('Episode ') and len(title_check) < 11:
            item_values.append({'title': f"{work_title} {work_dict['title']}"})
            if len(work_title_art) > 1:
                item_values.append({'title.article': work_title_art})
        else:
            item_values.append({'title': work_dict['title']})
            if 'title_article' in work_dict:
                if work_dict['title_article'] != '-' and work_dict['title_article'] != '':
                    item_values.append({'title.article': work_dict['title_article']})
        item_values.append({'title.language': 'English'})
        item_values.append({'title.type': '05_MAIN'})

    print(item_values)
    item_xml = adlib.create_record_data(CID_API, 'items','', item_values)
    try:
        print("Attempting to create CID Item record")
        item_rec = adlib.post(CID_API, item_xml, 'items', 'insertrecord')
        if item_rec:
            try:
                print("Populating item_id and object_number variables")
                item_id = adlib.retrieve_field_name(item_rec, 'priref')[0]
                item_object_number = adlib.retrieve_field_name(item_rec, 'object_number')[0]
                print(f'* Item record created with Priref {item_id}')
                print(f'* Item record created with Object number {item_object_number}')
                LOGGER.info('Item record created with priref %s', item_id)
            except Exception as err:
                print("Unable to create Item record", err)
                return None
    except Exception as err:
        print(f"* Unable to create Item record for <{work_dict['title']}> {err}")
        LOGGER.critical('** Unable to create Item record for <%s>', work_dict['title'])
        return None

    return item_object_number, item_id


if __name__ == '__main__':
    main()
