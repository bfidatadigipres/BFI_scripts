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

NOTES: Update so Episode * works prepended
       with Series title
       Acquisition date carried over from CSV
       Only series have work_type T, all other
       have F (inc shorts/monographic docs)

Joanna White
2023
'''

# Public packages
import os
import sys
import json
import logging
import datetime
import requests
import pandas
import yaml

# Local packages
sys.path.append(os.environ['CODE'])
import adlib
from document_augmented_netflix_cast import create_contributors

# Global variables
STORAGE = os.environ.get('QNAP_IMAGEN')
NETFLIX = os.path.join(STORAGE, 'NETFLIX')
CAT_ID = os.environ.get('PA_NETFLIX')
ADMIN = os.environ.get('ADMIN')
LOGS = os.path.join(ADMIN, 'Logs')
CODE = os.environ.get('CODE_PATH')
GENRE_MAP = os.path.join(CODE, 'document_en_15907/EPG_genre_mapping.yaml')
CONTROL_JSON = os.path.join(LOGS, 'downtime_control.json')
CID_API = os.environ.get('CID_API')
CID = adlib.Database(url=CID_API)
CUR = adlib.Cursor(CID)

# Date variables
TODAY = datetime.date.today()
TWO_WEEKS = TODAY - datetime.timedelta(days=14)
START = f"{TWO_WEEKS.strftime('%Y-%m-%d')}T00:00:00"
END = f"{TODAY.strftime('%Y-%m-%d')}T23:59:00"
TITLE_DATA = ''
UPDATE_AFTER = '2022-07-01T00:00:00'

# PATV API details including unique identifiers for Netflix catalogue
URL = os.path.join(os.environ['PATV_NETFLIX_URL'], f'catalogue/{CAT_ID}/')
URL2 = os.path.join(os.environ['PATV_NETFLIX_URL'], 'asset/')
HEADERS = {
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


def read_csv_to_dict(csv_path):
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


def get_folder_title(article, title):
    '''
    Match title to folder naming
    '''

    title = title.replace("/","").replace("'","").replace("&", "and").replace("(","").replace(")","")
    if article != '-':
        title = f'{article}_{title.replace(" ", "_")}_'
    else:
        title = f'{title.replace(" ", "_")}_'
    return title


def split_title(title_article):
    '''
    An exception needs adding for "Die " as German language content
    This list is not comprehensive.
    '''
    if title_article.startswith(("A ", "An ", "Am ", "Al-", "As ", "Az ", "Bir ", "Das ", "De ", "Dei ", "Den ",
                                 "Der ", "Det ", "Di ", "Dos ", "Een ", "Eene", "Ei ", "Ein ", "Eine", "Eit ",
                                 "El ", "el-", "En ", "Et ", "Ett ", "Het ", "Il ", "Na ", "A'", "L'", "La ",
                                 "Le ", "Les ", "Los ", "The ", "Un ", "Une ", "Uno ", "Y ", "Yr ")):
        title_split = title_article.split()
        ttl = title_split[1:]
        title = ' '.join(ttl)
        title_art = title_split[0]
        return title, title_art
    else:
        return title_article, '-'


def get_folder_match(foldername):
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


def get_json_files(fpath):
    '''
    Fetch JSON files in folder
    '''
    json_files = []

    for root, _, files in os.walk(fpath):
        for file in files:
            if file.endswith('.json'):
                json_files.append(os.path.join(root, file))

    return json_files


def retrieve_json(json_pth):
    '''
    One at a time, retrieve metadata for
    a given programme title, and check
    series number match and enough episodes
    present for supplied episode_num
    '''
    with open(json_pth, 'r') as file:
        data = json.load(file)

    return data


def get_cat_data(data=None):
    '''
    Get catalogue data and return as dct
    '''
    if data is None:
        data = {}

    c_data = {}
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
    except:
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
        pass
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


def get_json_data(data=None):
    '''
    Retrieve data from a PATV JSONs
    and return as dictionary
    '''
    if data is None:
        data == {}

    j_data = {}

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
        genres = []
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


def cid_check_works(patv_id):
    '''
    Sends CID request for series_id data
    '''
    hit_count = ""
    priref = ""
    query = {'database': 'works',
             'search': f'alternative_number="{patv_id}"',
             'limit': '1',
             'output': 'json',
             'fields': 'priref, title, title.article'}
    try:
        query_result = CID.get(query)
    except Exception as err:
        print(f"cid_check_works(): Unable to access series data from CID using Series ID: {patv_id} {err}")
        print("cid_check_works(): Series hit count and series priref will return empty strings")
        query_result = None
    try:
        hit_count = query_result.hits
        print(f"cid_check_works(): Hit counts returned for series: {hit_count}")
    except Exception as err:
        hit_count = ''
    try:
        priref = query_result.records[0]['priref'][0]
        print(f"cid_check_works(): Series priref: {priref}")
    except Exception as err:
        priref = ''
    try:
        title = query_result.records[0]['Title']['title']
        print(f"cid_check_works(): Series title: {title}")
    except Exception as err:
        title = ''
    try:
        title_art = query_result.records[0]['Title']['title.article']
        print(f"cid_check_works(): Series title: {title_art}")
    except Exception as err:
        title_art = ''

    return hit_count, priref, title, title_art


def genre_retrieval(category_code, description, title):
    '''
    Retrieve genre data, return as list
    '''
    with open(GENRE_MAP, 'r') as files:
        data = (yaml.load(files, Loader=yaml.FullLoader))
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
                        for key, val in genre_one.items():
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


def make_work_dictionary(episode_no, episode_id, csv_data, cat_dct, json_dct):
    '''
    Build up work data into dictionary for Work creation
    '''
    if not cat_dct:
        cat_dct = {}
    if not json_dct:
        json_dct = {}

    work_dict = {}
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
    desc_list = []
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
    except:
        work_dict['patv_id'] = ''

    try:
        work_dict['cat_id'] = cat_dct['cat_id']
    except:
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
    csv_path = sys.argv[1]
    if not os.path.isfile(csv_path):
        sys.exit(f"Problem with supplied CSV path {csv_path}")

    prog_dct = read_csv_to_dict(csv_path)
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
        print(article, title, nfa, level, season_num, genres, episode_num, platform, year_release, acquisition_date)

        if platform != 'Netflix':
            continue

        LOGGER.info("** Processing item: %s %s", article, title)

        # Make season number a list
        csv_data = [year_release, title, article, nfa, level, season_num, genres, episode_num, acquisition_date]

        # Match NETFLIX folder to article/title
        foldertitle = get_folder_title(article, title)
        matched_folders = get_folder_match(foldertitle)
        if len(matched_folders) > 1:
            print(f"More than one entry found for {article} {title}. Manual assistance needed.\n{matched_folders}")
            continue
        elif len(matched_folders) == 0:
            print(f"No match found: {article} {title}")
            # At some point initiate 'title' search in PATV data
            continue

        print(f"TITLE MATCH: {article} {title} -- {matched_folders[0]}")
        patv_id = matched_folders[0].split('_')[-1]

        # Create Work/Manifestation if film/programme
        if 'film' in level.lower() or 'programme' in level.lower():
            # Check CID work exists / Make work if needed
            hits, priref_work, work_title, work_title_art = cid_check_works(patv_id)
            if int(hits) > 0:
                print(f"SKIPPING PRIREF FOUND: {priref_work}")
                LOGGER.info("Skipping this item, likely already has CID record: %s", priref_work)
                continue
            prog_path = os.path.join(NETFLIX, matched_folders[0])

            print(f"Found priref is for monographic work: {priref_work}")
            if priref_work.isnumeric():
                print(f"SKIPPING: Monograph work already exists for {title}.")
                continue
            # Retrieve all available
            mono_cat = [ x for x in os.listdir(prog_path) if x.startswith('mono_catalogue_') ]
            mono = [ x for x in os.listdir(prog_path) if x.startswith('monographic_') ]
            try:
                cat_data = retrieve_json(os.path.join(prog_path, mono_cat[0]))
                cat_dct = get_cat_data(cat_data)
            except Exception as exc:
                print(exc)
                cat_dct = {}
            try:
                mono_data = retrieve_json(os.path.join(prog_path, mono[0]))
                mono_dct = get_json_data(mono_data)
            except Exception as exc:
                print(exc)
                mono_dct = {}

            if not cat_dct:
                print("SKIPPING: Missing data from JSON files.")
                continue
            # Make monographic work here
            data_dct = make_work_dictionary('', '', csv_data, cat_dct, mono_dct)
            print(f"Dictionary for monograph creation: \n{data_dct}")
            print("*************")
            record, series_work, work, work_restricted, manifestation, item = build_defaults(data_dct)
            priref_work = create_work('', '', '', data_dct, record, work, work_restricted)
            if len(priref_work) == 0:
                LOGGER.warning("Monograph work record creation failed, skipping all further record creations")
                continue
            print(f"PRIREF MONOGRAPH WORK: {priref_work}")

            # Create contributors if supplied / or in addition to solo contributors
            if 'contributors' in data_dct and len(data_dct['contributors']) >= 1:
                print('** Contributor data found')
                success = create_contributors(priref_work, data_dct['nfa_category'], data_dct['contributors'])
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
            hits, series_priref, work_title, work_title_art = cid_check_works(patv_id)
            if series_priref.isnumeric():
                print(f"Series work already exists for {title}.")
            else:
                print("Series work does not exist, creating series work now.")
                series_json = [ x for x in os.listdir(prog_path) if x.startswith('series_') and x.endswith('.json')]
                if not len(series_json) == 1:
                    continue

                # Get series ID title and genre
                series_data = retrieve_json(os.path.join(prog_path, series_json[0]))
                series_dct = get_json_data(series_data)
                series_data_dct = make_work_dictionary('', '', csv_data, None, series_dct)
                record, series_work, work, work_restricted, manifestation, item = build_defaults(series_data_dct)
                work_title, work_title_art = split_title(series_data_dct['title'])

                # Make series work here
                if not series_data_dct:
                    continue
                series_priref = create_series_work(patv_id, series_data_dct, csv_data, series_work, work_restricted, record)
                if not series_priref:
                    print("Series work creation failure. Skipping episodes...")
                    continue

            # Fetch target season data
            season_fpaths = [x for x in json_fpaths if f'season_{season_num}_' in str(x)]
            episode_count = 0
            for num in range(1, episode_num + 1):
                episode_count += 1
                episode_fpaths = [x for x in season_fpaths if f'episode_{num}_' in str(x) and x.endswith('.json')]
                if not episode_fpaths:
                    continue

                episode_folder = os.path.basename(os.path.split(episode_fpaths[0])[0])
                episode_id = episode_folder.split('_')[-1]
                print(f"** Episode ID: {episode_id} {title}")

                # Check CID work exists / Make work if needed
                hits, priref_episode, _, _ = cid_check_works(episode_id)
                if int(hits) > 0:
                    print(f"SKIPPING. EPISODE EXISTS IN CID: {priref_episode}")
                    LOGGER.info("Skipping episode, already exists in CID: %s", priref_episode)
                    continue
                print("New episode_id found for Work. Linking to series work")

                # Retrieve all available data
                ep_cat_json = [ x for x in episode_fpaths if 'episode_catalogue_' in str(x) ]
                ep_json = [ x for x in episode_fpaths if 'episode_' in str(x) and x.endswith(f"{episode_id}.json") ]
                print(ep_cat_json)
                print(ep_json)

                try:
                    ep_cat_data = retrieve_json(ep_cat_json[0])
                    ep_cat_dct = get_cat_data(ep_cat_data)
                except Exception as exc:
                    print(exc)
                    ep_cat_dct = {}
                try:
                    ep_data = retrieve_json(ep_json[0])
                    ep_dct = get_json_data(ep_data)
                except Exception as exc:
                    print(exc)
                    ep_dct = {}

                # Make episodic work here
                data_dct = make_work_dictionary(num, episode_id, csv_data, ep_cat_dct, ep_dct)
                print(f"Dictionary for Work creation:\n{data_dct}")
                print('**************')
                record, series_work, work, work_restricted, manifestation, item = build_defaults(data_dct)
                priref_episode = create_work(series_priref, work_title, work_title_art, data_dct, record, work, work_restricted)
                if len(priref_episode) == 0:
                    LOGGER.warning("Episodic Work record creation failed, skipping all further record creations")
                    continue
                print(f"Episode work priref: {priref_episode}")

                # Create contributors if supplied / or in addition to solo contributors
                if 'contributors' in data_dct and len(data_dct['contributors']) >= 1:
                    print('** Contributor data found')
                    success = create_contributors(priref_episode, data_dct['nfa_category'], data_dct['contributors'])
                    if success:
                        LOGGER.info("Contributor data written to Work record: %s", priref_episode)
                    else:
                        LOGGER.warning("Failure to write contributor data to Work record: %s", priref_episode)

                # Make episodic manifestation here
                priref_ep_man = create_manifestation(priref_episode, work_title, work_title_art, data_dct, record, manifestation)
                if len(priref_ep_man) == 0:
                    LOGGER.warning("Episodic manifestation record creation failed, skipping all further record creations")
                    continue
                print(f"PRIREF EP MANIFESTATION: {priref_ep_man}")
                # Append URLS if present
                append_url_data(priref_episode, priref_ep_man, data_dct)

                # Make episodic item record here
                priref_ep_item = create_item(priref_ep_man, work_title, work_title_art, data_dct, record, item)
                if len(priref_ep_item) == 0:
                    LOGGER.warning("Episodic item record creation failed, skipping onto next stage")
                    continue
                print(f"PRIREF FOR ITEM: {priref_ep_item}")

            if episode_count != int(episode_num):
                print("============ Episodes found in NETFLIX folder do not match total episodes supplied =============")

    LOGGER.info("=== Document augmented Netflix end =================================")


def firstname_split(person):
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


def genre_retrieval_term(category_code, description, title):
    '''
    Check genre yaml to retrieve genre prirefs
    '''
    category_data = genre_retrieval(category_code, description, title)
    try:
        genre1 = category_data[0]
    except Exception:
        genre1 = ''
    try:
        genre2 = category_data[1]
    except Exception:
        genre2 = ''
    try:
        subject1 = category_data[2]
    except Exception:
        subject1 = ''
    try:
        subject2 = category_data[3]
    except Exception:
        subject2 = ''

    return (genre1, genre2, subject1, subject2)


def build_defaults(data):
    '''
    Get detailed information
    and build record_defaults dict
    '''
    record = ([{'input.name': 'datadigipres'},
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
               {'record_access.user': '$REST'},
               {'record_access.rights': '1'},
               {'record_access.reason': 'SENSITIVE_LEGAL'},
               {'grouping.lref': '400947'},
               {'language.lref': '74129'},
               {'language.type': 'DIALORIG'}])

    series_work = ([{'record_type': 'WORK'},
                    {'worklevel_type': 'SERIAL'},
                    {'work_type': "T"},
                    {'description.type.lref': '100298'},
                    {'production_country.lref': '73938'},
                    {'nfa_category': data['nfa_category']}])

    work = ([{'record_type': 'WORK'},
             {'worklevel_type': 'MONOGRAPHIC'},
             {'work_type': data['work_type']},
             {'description.type.lref': '100298'},
             {'production_country.lref': '73938'},
             {'nfa_category': data['nfa_category']}])

    work_restricted = ([{'application_restriction': 'MEDIATHEQUE'},
                        {'application_restriction.date': str(datetime.datetime.now())[:10]},
                        {'application_restriction.reason': 'STRATEGIC'},
                        {'application_restriction.duration': 'PERM'},
                        {'application_restriction.review_date': '2030-01-01'},
                        {'application_restriction.authoriser': 'mcconnachies'},
                        {'application_restriction.notes': 'Netflix UK streaming content - pending discussion'}])

    manifestation = ([{'record_type': 'MANIFESTATION'},
                      {'manifestationlevel_type': 'INTERNET'},
                      {'format_high_level': 'Video - Digital'},
                      {'format_low_level.lref': '400949'},
                      {'colour_manifestation': data['colour_manifestation']},
                      {'sound_manifestation': 'SOUN'},
                      {'transmission_date': data['title_date_start']},
                      {'availability.name.lref': '143463'},
                      {'transmission_coverage': 'STR'},
                      {'vod_service_type.lref': '398712'},
                      {'aspect_ratio': '16:9'},
                      {'country_manifestation': 'United Kingdom'},
                      {'notes': 'Manifestation representing the UK streaming platform release of the Work.'}])

    item = ([{'record_type': 'ITEM'},
             {'item_type': 'DIGITAL'},
             {'copy_status': 'M'},
             {'copy_usage.lref': '131560'},
             {'file_type.lref': '401103'}, # IMP
             {'code_type.lref': '400945'}, # Mixed
             {'accession_date': str(datetime.datetime.now())[:10]},
             {'acquisition.date': data['acquisition_date']}, # Contract date from CSV
             {'acquisition.method.lref': '132853'}, # Donation - with written agreement ACQMETH
             {'acquisition.source.lref': '143463'}, # Netflix
             {'acquisition.source.type': 'DONOR'},
             {'access_conditions': 'Access requests for this collection are subject to an approval process. '\
                                   'Please raise a request via the Collections Systems Service Desk, describing your specific use.'},
             {'access_conditions.date': str(datetime.datetime.now())[:10]}])

    return (record, series_work, work, work_restricted, manifestation, item)


def create_series_work(patv_id, series_dct, csv_data, series_work, work_restricted, record):
    '''
    Build data needed to make
    episodic series work to
    link all episodes to
    [year_release, title, article, nfa, level, season_num, genres, episode_num]
    '''
    series_work_id = None
    series_work_values = []
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
    if 'd_short' in series_dct:
        series_work_values.append({'label.type': 'EPGSHORT'})
        series_work_values.append({'label.text': series_dct['d_short']})
        series_work_values.append({'label.source': 'EBS augmented EPG supply'})
        series_work_values.append({'label.date': str(datetime.datetime.now())[:10]})
    if 'd_medium' in series_dct:
        series_work_values.append({'label.type': 'EPGMEDIUM'})
        series_work_values.append({'label.text': series_dct['d_medium']})
        series_work_values.append({'label.source': 'EBS augmented EPG supply'})
        series_work_values.append({'label.date': str(datetime.datetime.now())[:10]})
    if 'd_long' in series_dct:
        series_work_values.append({'label.type': 'EPGLONG'})
        series_work_values.append({'label.text': series_dct['d_long']})
        series_work_values.append({'label.source': 'EBS augmented EPG supply'})
        series_work_values.append({'label.date': str(datetime.datetime.now())[:10]})
    if 'description' in series_dct:
        series_work_values.append({'description': series_dct['description']})
        series_work_values.append({'description.type': 'Synopsis'})
        series_work_values.append({'description.date': str(datetime.datetime.now())[:10]})
    print(f"Series work values:\n{series_work_values}")

    # Start creating CID Work Series record
    try:
        print("Attempting to create CID record")
        w = CUR.create_record(database='works',
                              data=series_work_values,
                              output='json',
                              write=True)
        if w.records:
            try:
                print("Populating series_work_id and object_number variables")
                series_work_id = w.records[0]['priref'][0]
                object_number = w.records[0]['object_number'][0]
                print(f'* Series record created with Priref {series_work_id}')
                print(f'* Series record created with Object number {object_number}')
                LOGGER.info('Work record created with priref %s', series_work_id)
            except Exception as err:
                print("Unable to create series record", err)
                return None

            try:
                series_genres = []
                if 'genres' in series_dct:
                    extracted = series_dct['genres']
                    for genr in extracted:
                        series_genres.append({'content.genre.lref': genr})
                if 'subjects' in series_dct:
                    subs = series_dct['subjects']
                    for sub in subs:
                        series_genres.append({'content.subject.lref': sub})
                series_genres_filter = [i for n, i in enumerate(series_genres) if i not in series_genres[n + 1:]]
                print(series_genres, series_genres_filter)
                print("**** Attempting to write work genres to records ****")
                g = CUR.create_occurrences(database='works',
                                           priref=series_work_id,
                                           data=series_genres_filter,
                                           output='json')
            except Exception as err:
                print("Unable to write genre", err)
    except Exception as err:
        print(f'* Unable to create Work record for <{title}> {err}')
        LOGGER.critical('Unable to create Work record for <%s>', title)
        return None

    return series_work_id


def create_work(part_of_priref, work_title, work_title_art, work_dict, record_def, work_def, work_restricted):
    '''
    Build all data needed to make new work.
    work_def from work/series_work defaults
    Hand in series or episode, part_of_priref
    populated as needed.
    '''
    work_id = ''
    work_genres = []
    work_values = []
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
    if 'd_short' in work_dict:
        work_values.append({'label.type': 'EPGSHORT'})
        work_values.append({'label.text': work_dict['d_short']})
        work_values.append({'label.source': 'EBS augmented EPG supply'})
        work_values.append({'label.date': str(datetime.datetime.now())[:10]})
    if 'd_medium' in work_dict:
        work_values.append({'label.type': 'EPGMEDIUM'})
        work_values.append({'label.text': work_dict['d_medium']})
        work_values.append({'label.source': 'EBS augmented EPG supply'})
        work_values.append({'label.date': str(datetime.datetime.now())[:10]})
    if 'd_long' in work_dict:
        work_values.append({'label.type': 'EPGLONG'})
        work_values.append({'label.text': work_dict['d_long']})
        work_values.append({'label.source': 'EBS augmented EPG supply'})
        work_values.append({'label.date': str(datetime.datetime.now())[:10]})
    if 'description' in work_dict:
        work_values.append({'description': work_dict['description']})
        work_values.append({'description.type': 'Synopsis'})
        work_values.append({'description.date': str(datetime.datetime.now())[:10]})
    print(f"Work values:\n{work_values}")

    # Start creating CID Work Series record
    try:
        print("Attempting to create CID record")
        w = CUR.create_record(database='works',
                              data=work_values,
                              output='json',
                              write=True)
        if w.records:
            try:
                print("Populating work_id and object_number variables")
                work_id = w.records[0]['priref'][0]
                object_number = w.records[0]['object_number'][0]
                print(f'* Work record created with Priref {work_id}')
                print(f'* Work record created with Object number {object_number}')
                LOGGER.info('** Work record created with priref %s', work_id)
                try:
                    work_genres = []
                    if 'genres' in work_dict:
                        extracted = work_dict['genres']
                        for genr in extracted:
                            work_genres.append({'content.genre.lref': genr})
                    if 'subjects' in work_dict:
                        subs = work_dict['subjects']
                        for sub in subs:
                            work_genres.append({'content.subject.lref': sub})
                    work_genres_filter = [i for n, i in enumerate(work_genres) if i not in work_genres[n + 1:]]
                    print("**** Attempting to write work genres to records ****")
                    print(work_genres)
                    print(work_genres_filter)
                    # BROKEN HERE, try work_append() module below
                    g = CUR.update_record(database='works',
                                          priref=work_id,
                                          data=work_genres_filter,
                                          output='json')
                except Exception as err:
                    print("Unable to write genre", err)
                return work_id
            except Exception as err:
                print("Unable to create work record", err)
                return work_id
    except Exception as err:
        print(f"* Unable to create Work record for <{work_dict['title']}> {err}")
        LOGGER.critical('** Unable to create Work record for <%s>', work_dict['title'])
        return work_id

    return work_id


def create_credit_names(nfa_category, cat_dct):
    '''
    DEPRECATED FUNCTION
    Append cast/credit names from
    catalogue string to credit name fields
    '''

    cast_seq_start = 0
    cred_seq_start = 0
    cast_dct_update = []
    cred_dct_update = []
    if 'cast' in cat_dct:
        if isinstance(cat_dct['cast'], list):
            cast_list = cat_dct['cast']
        else:
            cast_list = cat_dct['cast'].split(',')
        print(f"List of cast found: {cast_list}")
        for ident in cast_list:
            cast_name = firstname_split(ident)
            cast_seq_start += 5
            cast_dct_update.append({'cast.credit_credited_name': f'{cast_name}'})
            cast_dct_update.append({'cast.credit_type': 'cast member'})
            cast_dct_update.append({'cast.sequence': str(cast_seq_start)})
            cast_dct_update.append({'cast.sequence.sort': f"7300{str(cast_seq_start).zfill(4)}"})
            cast_dct_update.append({'cast.section': '[normal cast]'})
    if 'directors' in cat_dct or 'writers' in cat_dct:
        if 'directors' in cat_dct:
            if isinstance(cat_dct['directors'], list):
                cred_list = cat_dct['directors']
            else:
                cred_list = cat_dct['directors'].split(',')
            print(f"Directors found in catalogue data: {cred_list}")
            for ident in cred_list:
                cred_name = firstname_split(ident)
                cred_seq_start += 5
                cred_dct_update.append({'credit.credited_name': f'{cred_name}'})
                cred_dct_update.append({'credit.type': 'Director'})
                cred_dct_update.append({'credit.sequence': str(cred_seq_start)})
                cred_dct_update.append({'credit.sequence.sort': f"500{str(cred_seq_start).zfill(4)}"})
                cred_dct_update.append({'credit.section': '[normal credit]'})
        if 'writers' in cat_dct and nfa_category == 'F':
            if isinstance(cat_dct['writers'], list):
                cred_list = cat_dct['writers']
            else:
                cred_list = cat_dct['writers'].split(',')
            print(f"Writers found in catalogue data: {cred_list}")
            for ident in cred_list:
                cred_name = firstname_split(ident)
                cred_seq_start += 5
                cred_dct_update.append({'credit.credited_name': f'{cred_name}'})
                cred_dct_update.append({'credit.type': 'Screenplay'})
                cred_dct_update.append({'credit.sequence': str(cred_seq_start)})
                cred_dct_update.append({'credit.sequence.sort': f"15000{str(cred_seq_start).zfill(4)}"})
                cred_dct_update.append({'credit.section': '[normal credit]'})
        elif 'writers' in cat_dct and nfa_category == 'D':
            if isinstance(cat_dct['writers'], list):
                cred_list = cat_dct['writers']
            else:
                cred_list = cat_dct['writers'].split(',')
            print(f"Writers found in catalogue data: {cred_list}")
            for ident in cred_list:
                cred_name = firstname_split(ident)
                cred_seq_start += 5
                cred_dct_update.append({'credit.credited_name': f'{cred_name}'})
                cred_dct_update.append({'credit_type': 'Script'})
                cred_dct_update.append({'credit.sequence': str(cred_seq_start)})
                cred_dct_update.append({'credit.sequence.sort': f"15500{str(cred_seq_start).zfill(4)}"})
                cred_dct_update.append({'credit.section': '[normal credit]'})

    return cast_dct_update, cred_dct_update


def append_cred_cast_names(priref, cast_list, cred_list):
    '''
    Appending cast/cred names where no contributor data
    '''

    # Append cast/credit and edit name blocks to work_append_dct
    work_append_dct = []
    work_append_dct.extend(cast_list)
    work_append_dct.extend(cred_list)
    work_edit_data = ([{'edit.name': 'datadigipres'},
                       {'edit.date': str(datetime.datetime.now())[:10]},
                       {'edit.time': str(datetime.datetime.now())[11:19]},
                       {'edit.notes': 'Automated cast and credit update from PATV augmented EPG metadata'}])

    work_append_dct.extend(work_edit_data)
    LOGGER.info("** Appending data to work record now...")
    print("*********************")
    print(work_append_dct)
    print("*********************")

    result = work_append(priref, work_append_dct)
    if result:
        print(f"Work appended successful! {priref}")
        LOGGER.info("Successfully appended additional cast credit EPG metadata to Work record %s\n", priref)
        LOGGER.info("=============== END document_aug_netflix_castcred script END ===============\n")
        return True
    else:
        LOGGER.warning("Writing EPG cast credit metadata to Work %s failed\n", priref)
        print(f"Work append FAILED!! {priref}")
        LOGGER.info("=============== END document_aug_netflix_castcred script END ===============\n")
        return False


def work_append(priref, work_dct=None):
    '''
    Items passed in work_dct for amending to Work record
    '''
    print(work_dct)
    if work_dct is None:
        work_dct = []
        LOGGER.warning("work_append(): work_update_dct passed to function as None")
    try:
        result = CUR.update_record(priref=priref,
                                   database='works',
                                   data=work_dct,
                                   output='json',
                                   write=True)
        print("*** Work append result:")
        print(result)
        return True
    except Exception as err:
        LOGGER.warning("work_append(): Unable to append work data to CID work record %s", err)
        print(err)
        return False


def create_manifestation(work_priref, work_title, work_title_art, work_dict, record_defaults, manifestation_defaults):
    '''
    Create a manifestation record,
    linked to work_priref
    '''
    manifestation_id = ''
    print(work_dict)
    title = work_dict['title']
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
    try:
        m = CUR.create_record(database='manifestations',
                              data=manifestation_values,
                              output='json',
                              write=True)
        if m.records:
            try:
                manifestation_id = m.records[0]['priref'][0]
                object_number = m.records[0]['object_number'][0]
                print(f'* Manifestation record created with Priref {manifestation_id} Object number {object_number}')
                LOGGER.info('Manifestation record created with priref %s', manifestation_id)
            except Exception as err:
                print(f'* Unable to create Manifestation record for <{title}>, {err}')
                LOGGER.critical('Unable to create Manifestation record for <%s>', title)

    except Exception as err:
        print(f"Unable to write manifestation record - error: {err}")

    broadcast_addition = ([{'broadcast_company.lref': '143463'}])

    try:
        b = CUR.update_record(database='manifestations',
                              priref=manifestation_id,
                              data=broadcast_addition,
                              output='json')
    except Exception as err:
        LOGGER.info("Unable to write broadcast company data\n%s", err)
    return manifestation_id


def append_url_data(work_priref, man_priref, data=None):
    '''
    Receive Netflix URLs and priref and append to CID manifestation
    '''

    if 'watch_url' in data:
        # Write to manifest
        payload_mid = f"<URL>{data['watch_url']}</URL><URL.description>Netflix viewing URL</URL.description>"
        payload_head = f"<adlibXML><recordList><record priref='{man_priref}'><URL>"
        payload_end = "</URL></record></recordList></adlibXML>"
        payload = payload_head + payload_mid + payload_end

        write_lock('manifestations', man_priref)
        post_response = requests.post(
            CID_API,
            params={'database': 'manifestations', 'command': 'updaterecord', 'xmltype': 'grouped', 'output': 'json'},
            data={'data': payload})

        if "<error><info>" in str(post_response.text):
            LOGGER.warning("cid_media_append(): Post of data failed: %s - %s", man_priref, post_response.text)
            unlock_record('manifestations', man_priref)
        else:
            LOGGER.info("cid_media_append(): Write of access_rendition data appear successful for Priref %s", man_priref)

        # Write to work
        payload_head = f"<adlibXML><recordList><record priref='{work_priref}'><URL>"
        payload = payload_head + payload_mid + payload_end

        write_lock('works', work_priref)
        post_response = requests.post(
            CID_API,
            params={'database': 'works', 'command': 'updaterecord', 'xmltype': 'grouped', 'output': 'json'},
            data={'data': payload})

        if "<error><info>" in str(post_response.text):
            LOGGER.warning("cid_media_append(): Post of data failed: %s - %s", work_priref, post_response.text)
            unlock_record('works', work_priref)
        else:
            LOGGER.info("cid_media_append(): Write of access_rendition data appear successful for Priref %s", work_priref)


def create_item(man_priref, work_title, work_title_art, work_dict, record_defaults, item_default):
    '''
    Create item record,
    link to manifestation
    '''
    item_id = ''
    item_object_number = ''
    item_values = []
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
    try:
        i = CUR.create_record(database='items',
                              data=item_values,
                              output='json',
                              write=True)

        if i.records:
            try:
                item_id = i.records[0]['priref'][0]
                item_object_number = i.records[0]['object_number'][0]
                print(f'* Item record created with Priref {item_id} Object number {item_object_number}')
                LOGGER.info('Item record created with priref %s', item_id)
            except Exception as err:
                LOGGER.warning("Item data could not be retrieved from the record: %s", err)

    except Exception as err:
        LOGGER.critical('PROBLEM: Unable to create Item record for <%s> manifestation', man_priref)
        print(f"** PROBLEM: Unable to create Item record attached to manifestation: {man_priref}\nError: {err}")

    return item_object_number, item_id


def write_lock(database, priref):
    '''
    Apply a writing lock to the person record before updating metadata to Headers
    '''
    try:
        post_response = requests.post(
            CID_API,
            params={'database': database, 'command': 'lockrecord', 'priref': f'{priref}', 'output': 'json'})
    except Exception as err:
        LOGGER.warning("Lock record wasn't applied to record %s\n%s", priref, err)


def unlock_record(database, priref):
    '''
    Only used if write fails and lock was successful, to guard against file remaining locked
    '''
    try:
        post_response = requests.post(
            CID_API,
            params={'database': database, 'command': 'unlockrecord', 'priref': f'{priref}', 'output': 'json'})
    except Exception as err:
        LOGGER.warning("Post to unlock record failed. Check record %s is unlocked manually\n%s", priref, err)



if __name__ == '__main__':
    main()
