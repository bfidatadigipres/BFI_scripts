#!/usr/bin/env python3

'''
Script to create People records from EPG metadata
and attach to existing CID Work records

1. Iterate through completed/ folder in STORA/completed/
   for yesterday's month/year - allowing to pass items on last day twice
   Where {filename}.json.documented found (and not _castcred in path):
   - Extract basic data from EPG metadata
   - Look in CID using automated STORA statement, title, date time of show
2. Where matching data is found, extract addition data from EPG source file:
   - Credit information including EPG UID
3. Check in CID Person database if EPG UID already exists
   - Yes: Extract priref and other necessary data for step 5
   - No: Skip to part 4
4. Create new Person record in Person database using EPG data, write EPG UID
5. Create dictionary link of each new / found Person record to CID Work
6. Append data to the CID work
7. Update {filename}.json.documented name adding '_castcred' to end of name

TO DO:
name.status returns 5 (rejected). Potential impacts to be discussed

Add function to add +1 GMT to all timings of show data
ready for update of STORA data to correct scheduling times
(not current EPG timings, all -1 GMT to schedule)

Joanna White 2021
Python 3.6+
'''

# Global packages
import os
import sys
import json
import codecs
import logging
import datetime

# Local packages
sys.path.append(os.environ['CODE'])
import adlib_v3 as adlib

# Global vars
TODAY = str(datetime.datetime.now())
TODAY_TIME = TODAY[11:19]
TODAY_DATE = TODAY[:10]
YEST = str(datetime.datetime.today() - datetime.timedelta(days=1))
YEAR = YEST[:4]
MONTH = YEST[5:7]
# YEAR = '2024'
# MONTH = '03'
COMPLETE = os.environ['STORA_COMPLETED']
ARCHIVE_PATH = os.path.join(COMPLETE, YEAR, MONTH)
LOG_PATH = os.environ['LOG_PATH']
CID_API = os.environ['CID_API4']
CODEPTH = os.environ['CODE']
CONTROL_JSON = os.path.join(LOG_PATH, 'downtime_control.json')

# Setup logging
LOGGER = logging.getLogger('document_stora_castcred')
HDLR = logging.FileHandler(os.path.join(LOG_PATH, 'document_stora_castcred.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)

# First val index CID cast.credit_type (Work),  activity_type (Person), term_code for sort.sequence (work)
contributors = {'actor': ['cast member', 'Cast', '73000'],
                'co-host': ['host', 'Presentation', '72700'],
                'coach': ['on-screen participant', 'Miscellaneous', '73010'],
                'Commentator': ['commentator', 'Cast', '71500'],
                'contestant': ['on-screen participant', 'Cast', '73010'],
                'contributor': ['on-screen participant', 'Cast', '73010'],
                'guest': ['on-screen participant', 'Cast', '73010'],
                'host': ['host', 'Presentation', '72700'],
                'judge': ['host', 'Presentation', '72700'],
                'musical-guest': ['music performance', 'Music', '72600'],
                'narrator': ['narrator', 'Cast', '72000'],
                'pannelist': ['on-screen participant', 'Cast', '73010'],
                'panelist': ['on-screen participant', 'Cast', '73010'],
                'performer': ['on-screen participant', 'Cast', '73010'],
                'presenter': ['presenter', 'Presentation', '70000'],
                'reader': ['reader', 'Presentation', '72500'],
                'reporter': ['news reporter', 'Presentation', '71100'],
                'scorekeeper': ['on-screen participant', 'Presentation', '73010'],
                'storyteller': ['narrator', 'Cast', '72000'],
                'team-captain': ['on-screen participant', 'Cast', '73010']}

# First val index credit.type (Work), activity_type (Person), term_code for sort.sequence (work)
production = {'abridged-by': ['Script', 'Scripting', '15500'],
              'adapted-by': ['Script', 'Scripting', '15500'],
              'deputy-editor': ['Deputy Editor', 'Production', '4600'],
              'director': ['Director', 'Direction', '500'],
              'dramatised-by': ['Script', 'Scripting', '15500'],
              'editor': ['Editor', 'Editing', '37500'],
              'executive-editor': ['Executive Story Editor', 'Production', '2500'],
              'executive-producer': ['Executive Producer', 'Production', '2500'],
              'producer': ['Producer', 'Production', '3010'],
              'series-director': ['Series Director', 'Direction', '650'],
              'series-editor': ['Series Editor', 'Production', '4500'],
              'series-producer': ['Series Producer', 'Production', '4500'],
              'writer-nf': ['Script', 'Scripting', '15500'],
              'writer-f': ['Screenplay', 'Scripting', '15000']}


def check_control():
    '''
    Check control json for downtime requests
    '''
    with open(CONTROL_JSON) as control:
        j = json.load(control)
        if not j['stora']:
            LOGGER.info('Script run prevented by downtime_control.json. Script exiting.')
            sys.exit('Script run prevented by downtime_control.json. Script exiting.')


def cid_check():
    '''
    Tests if CID active before all other operations commence
    '''
    try:
        adlib.check(CID_API)
    except KeyError:
        print("* Cannot establish CID session, exiting script")
        LOGGER.critical("* Cannot establish CID session, exiting script")
        sys.exit()


def split_title(title_article):
    '''
    Splits title where it finds a matching article to startswith() list
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
        return None


def title_filter(item_asset_title, item_title):
    '''
    Function to match actions of Py2 title processing
    Returns title, title_art where possible
    '''
    title = ""
    title_art = ""
    title_1 = True
    title_2 = True

    if item_asset_title == "Generic":
        title_1 = False
    elif len(item_asset_title) == 0:
        title_1 = False
    else:
        title_bare = ''.join(str for str in item_asset_title if str.isalnum())
        if title_bare.isnumeric():
            title_1 = False

    if title_1:
        try:
            title = split_title(item_asset_title)[0]
            print(f"Title: {title}")
            title_article = split_title(item_asset_title)[1]
            print(f"Title article: {title_article}")
        except Exception:
            title = item_asset_title
            print(f"Title: {title}")
    elif title_2:
        try:
            title = split_title(item_title)[0]
            print(f"Title: {title}")
            title_article = split_title(item_title)[1]
            print(f"Title article: {title_article}")
        except Exception:
            title = item_title
            print(f"Title: {title}")

    return (title, title_art)


def enum_list(creds):
    '''
    Change list to dictionary pairs for sort order
    Increments of 5, beginning at 50
    '''
    n = 50
    for item in creds:
        yield n, item
        n += 5


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


def retrieve_epg_data(fullpath):
    '''
    Retrieve credits dct, asset/type and asset/category/code list
    '''

    with open(fullpath, 'r') as inf:
        lines = json.load(inf)
        for _ in lines.items():

            # Get titles
            try:
                item_title = lines["item"][0]["title"]
            except (KeyError, IndexError):
                item_title = ''
            try:
                item_asset_title = lines["item"][0]["asset"]["title"]
            except (KeyError, IndexError):
                item_asset_title = ''

            # Get transmission time
            try:
                title_date_start_full = str(lines["item"][0]["dateTime"])
                title_date_start = title_date_start_full[0:10]
                print(f"Date of broadcast: {title_date_start}")
            except (KeyError, IndexError):
                title_date_start = ''
            try:
                time_full = str(lines["item"][0]["dateTime"])
                time = time_full[11:19]
                print(f"Time of broadcast: {time}")
            except (KeyError, IndexError):
                time = ''

            # Get EPG category code
            try:
                code1 = lines["item"][0]["asset"]["category"][0]["code"]
            except (KeyError, IndexError):
                code1 = ''
            try:
                code2 = lines["item"][0]["asset"]["category"][1]["code"]
            except (KeyError, IndexError):
                code2 = ''

            # Filter topics for non-fiction/fiction
            if "factual-topics" in ((code1, code2)):
                nfa_category = "nf"
            elif "movie-drama" in ((code1, code2)):
                nfa_category = "f"
            elif "news-current-affairs" in ((code1, code2)):
                nfa_category = "nf"
            elif "sports:" in ((code1, code2)):
                nfa_category = "nf"
            elif "music-ballet" in ((code1, code2)):
                nfa_category = "nf"
            elif "arts-culture:" in ((code1, code2)):
                nfa_category = "nf"
            elif "social-political-issues" in ((code1, code2)):
                nfa_category = "nf"
            elif "leisure-hobbies" in ((code1, code2)):
                nfa_category = "nf"
            elif "show-game-show" in ((code1, code2)):
                nfa_category = "nf"
            else:
                nfa_category = "f"

            # Get TV/Film
            work = lines["item"][0]["asset"]["type"]
            if work == "movie":
                work_type = "f"
            else:
                work_type = "tv"
            try:
                credit_list = []
                credit_list = lines["item"][0]["asset"]["contributor"]
            except (KeyError, IndexError):
                credit_list = []

        return (item_title, item_asset_title, nfa_category, work_type, title_date_start, time, credit_list)


def retrieve_person(credit_list_raw, nfa_cat):
    '''
    Receives credits dictionary from main(), iterates over entries and creates
    list of dictionaries for credit/cast enumerated (5,10,15) in order retrieved
    Returns dictionary of dictionary containing key role, value list of all other data
    '''
    credit_list = []
    cred_list = []
    cast_list = []
    for dictionary in credit_list_raw:
        try:
            cred_id = dictionary['id']
        except (KeyError, IndexError):
            cred_id = ''
        try:
            name = dictionary['name']
        except (KeyError, IndexError):
            name = ''
        try:
            dofb = dictionary['dob']
        except (KeyError, IndexError):
            dofb = ''
        try:
            dofd = dictionary['dod']
        except (KeyError, IndexError):
            dofd = ''
        try:
            home = dictionary['from']
        except (KeyError, IndexError):
            home = ''
        try:
            gender = dictionary['gender']
        except (KeyError, IndexError):
            gender = ''
        try:
            character_type = dictionary['character'][0]['type']
        except (KeyError, IndexError):
            character_type = ''
        try:
            character_name = dictionary['character'][0]['name']
        except (KeyError, IndexError):
            character_name = ''
        try:
            roles = dictionary['role']  # List of strings
        except (KeyError, IndexError):
            roles = ''
        try:
            meta = dictionary['meta']
        except (KeyError, IndexError):
            meta = ''
        try:
            known_for = meta['best-known-for']
            known_for = known_for.replace("\'", "'")
        except (KeyError, IndexError):
            known_for = ''
        try:
            early_life = meta['early-life']
            early_life = early_life.replace("\'", "'")
        except (KeyError, IndexError):
            early_life = ''
        try:
            career = meta['career']
            career = career.replace("\'", "'")
        except (KeyError, IndexError):
            career = ''
        try:
            trivia = meta['trivia']
            trivia = trivia.replace("\'", "'")
        except (KeyError, IndexError):
            trivia = ''

        # Build lists to generate cast/credit_dct
        credit_list = list(
            [
                str(cred_id),
                str(name),
                str(dofb),
                str(dofd),
                str(home),
                str(gender),
                str(known_for),
                str(early_life),
                str(career),
                str(trivia),
                str(character_type),
                str(character_name),
            ]
        )

        for role in roles:
            if 'writer' in str(role):
                if 'nf' in nfa_cat:
                    role = 'writer-nf'
                elif 'f' in nfa_cat:
                    role = 'writer-f'
            if str(role) in contributors.keys():
                cast_list.append({str(role): credit_list})
            if str(role) in production.keys():
                cred_list.append({str(role): credit_list})

    # Convert list to dict {5: {'actor': '..'}, 10: {'actor': '..'}}
    if len(cast_list) > 0:
        cast_dct = dict(enum_list(cast_list))
    else:
        cast_dct = []
    if len(cred_list) > 0:
        cred_dct = dict(enum_list(cred_list))
    else:
        cred_dct = []

    return (cast_dct, cred_dct)


def cid_person_check(credit_id):
    '''
    Retrieve if Person record with priref already exist for credit_entity_id
    '''
    search = f"(utb.content='{credit_id}' WHEN utb.fieldname='PATV Person ID')"
    try:
        result = adlib.retrieve_record(CID_API, 'people', search, '0', ['name', 'activity_type'])[1]
    except (KeyError, IndexError, TypeError):
        LOGGER.exception("cid_person_check(): Unable to check for person record with credit id: %s", credit_id)
    try:
        name = adlib.retrieve_field_name(result[0], 'name')[0]
        priref = adlib.retrieve_field_name(result[0], 'priref')[0]
    except (KeyError, IndexError):
        name = ''
        priref = ''
    try:
        act_type = adlib.retrieve_field_name(result[0], 'activity_type')
    except (KeyError, IndexError):
        return priref, name, ''

    activity_types = []
    for count in range(0, len(act_type)):
        try:
            activity_types.append(act_type[count])
        except (KeyError, IndexError):
            pass
    return priref, name, activity_types


def cid_work_check(search):
    '''
    Retrieve CID work record priref where search matches
    '''
    prirefs = []
    edit_names = []

    try:
        hits, record = adlib.retrieve_record(CID_API, 'works', search, '0', ['input.notes, edit.name'])
    except (KeyError, IndexError):
        LOGGER.exception("cid_work_check(): Unable to check for person record with search %s", search)
    print("--------------------------------------")
    print(record)
    print("--------------------------------------")
    for num in range(0, int(hits)):
        try:
            priref = adlib.retrieve_field_name(record[num], 'priref')[0]
            input_note = adlib.retrieve_field_name(record[num], 'input.notes')[0]
        except (KeyError, IndexError, TypeError):
            priref = ''
            input_note = ''
        try:
            edit_name = adlib.retrieve_field_name(record[num], 'edit.name')[0]
        except (KeyError, IndexError):
            edit_name = ''

        if 'STORA off-air television capture - automated bulk documentation' in input_note:
            prirefs.append(priref)
            edit_names.append(edit_name)

    return (prirefs, edit_names)


def cid_manifestation_check(priref):
    '''
    Retrieve Manifestation transmission start time from parent priref
    '''
    search = f"(part_of_reference.lref='{priref}')"
    try:
        record = adlib.retrieve_record(CID_API, 'manifestations', search, '0', ['transmission_start_time'])[1]
    except (KeyError, IndexError):
        LOGGER.exception("cid_manifestation_check(): Unable to check for record with priref: %s", priref)
    try:
        start_time = adlib.retrieve_field_name(record[0], 'transmission_start_time')[0]
    except (KeyError, IndexError):
        LOGGER.info("cid_manifestation_check(): Unable to extract start time for manifestation")
        start_time = ''

    return start_time


def main():
    '''
    Find all {filename}.json.document files and store in list
    Iterate list extracting cast/credit and other metadata
    Find CID Work with same name/date and time information
    Check in CID for existing People records and extract priref
    Create new People rec where needed and capture priref
    Link new/existing People priref to CID Work
    '''
    LOGGER.info("============= START document_stora_castcred script START =============")
    check_control()
    cid_check()
    LOGGER.info("Checking path for documented JSON: %s", ARCHIVE_PATH)
    print(ARCHIVE_PATH)

    # Iterate through all historical EPG metadata file
    # Later limit to just 1 week range before yesterday
    for root, _, files in os.walk(ARCHIVE_PATH):
        for file in files:
            if not file.endswith('json.documented'):
                continue

            check_control()
            LOGGER.info("New file found for processing: %s", file)
            fullpath = os.path.join(root, file)
            credit_data = ''
            # Retrieve all data from EPG
            credit_data = retrieve_epg_data(fullpath)
            item_title = credit_data[0]
            item_asset_title = credit_data[1]
            nfa_cat = credit_data[2]
            date = credit_data[4]
            time = credit_data[5]
            credit_list = credit_data[6]

            # Process title data
            try:
                title, title_art = title_filter(item_asset_title, item_title)
                LOGGER.info("Title for search: %s %s", title_art, title)
            except Exception:
                title = ''
                title_art = ''

            # Get people data
            if len(credit_list) > 0:
                LOGGER.info("Cast and credit information available for this record")
                cast_dct, cred_dct = retrieve_person(credit_list, nfa_cat)
                print(cast_dct)
                print(cred_dct)
            else:
                LOGGER.info("SKIPPING: %s - No cast or credit data\n%s", title, fullpath)
                LOGGER.info("Renaming JSON with _castcred appended\n")
                rename(root, file, title)
                continue

            # Check in CID for Work title/date match
            search = f"(title='{title}' AND title_date_start='{date}')"
            print(search)
            work_data = cid_work_check(search)
            print("--------------------------------------")
            print(work_data)
            print("--------------------------------------")
            work_priref = ''
            time_match = False

            # Iterate all potential matches for transmission time match
            if len(work_data[0]) > 0:
                for work_prirefs in work_data[0]:
                    work_priref_check = work_prirefs[0]
                    LOGGER.info("Priref found that matches date/title: %s", work_priref_check)
                    LOGGER.info("Checking work manifestation to see if broadcast times match...")

                    # Check manifestation for matching transmission time
                    transmission_time = cid_manifestation_check(work_priref_check)
                    print(f"If {str(time)} == {str(transmission_time[:8])}:")
                    if str(time) == str(transmission_time)[:8]:
                        LOGGER.info("Programme times match: %s and %s\n", time, transmission_time[0:8])
                        time_match = True
                        work_priref = work_priref_check
                        break
                    else:
                        LOGGER.warning("Programme times DO NOT MATCH this work: %s and %s\n", time, transmission_time[:8])
                        time_match = False
                        work_priref = ''
                        continue
            else:
                LOGGER.info("SKIPPING: Likely repeat as no work record data found for %s transmitted on %s", title, date)
                LOGGER.info("Renaming JSON with _castcred appended\n")
                rename(root, file, title)
                continue

            if len(work_priref) == 0:
                LOGGER.info("PROBLEM: Prirefs found but no transmission times matched for %s %s", title, date)
                LOGGER.info("Renaming JSON with _castcred appended\n")
                rename(root, file, title)
                continue

            print(f"Title: {title}")
            print(f"Priref: {work_priref}")
            print(f"Matching transmission times: {time} {transmission_time[:8]}")
            print(f"Time match = {time_match}\n")

            cast_list = []
            cred_list = []

            # BEGIN CAST DATA GENERATION
            person_priref, person_name, person_act_type = '', '', ''
            if len(cast_dct) > 0 and time_match:
                for key, val in cast_dct.items():
                    cast_sort = str(key)
                    cast_sort.zfill(2)  # 50, 55, 60
                    for k, v in val.items():
                        cast_type = k  # Cast, etc
                        cast_id = v[0]  # EPG ID
                        cast_name = firstname_split(v[1])
                        screen_name = v[11]  # Character name

                        # Check person record exists
                        person_priref, person_name, person_act_type = cid_person_check(cast_id)
                        if len(person_priref) > 0:
                            LOGGER.info("Person record already exists: %s %s", person_name, person_priref)
                            for k_, v_ in contributors.items():
                                if str(cast_type) == k_:
                                    activity_type = v_[1]
                                    if str(activity_type) in str(person_act_type):
                                        LOGGER.info("MATCHED Activity types: %s with %s", activity_type, person_act_type)
                                    else:
                                        LOGGER.info("** Activity type does not match. Appending NEW ACTIVITY TYPE: %s", activity_type)
                                        append_activity_type(person_priref, person_act_type, activity_type)
                            LOGGER.info("Cast Name/Priref extacted and will append to cast_dct_update")
                        else:
                            cast_dct_data = ''
                            # Create data for Person record creation
                            cast_dct_data = make_person_dct(val)
                            cast_dct_formatted = cast_dct_data[0]
                            known_for = cast_dct_data[1]
                            early_life = cast_dct_data[2]
                            bio = cast_dct_data[3]
                            trivia = cast_dct_data[4]

                            # Make Person record
                            person_priref = make_person_record(cast_dct_formatted)
                            if not person_priref:
                                LOGGER.warning("Failure to create person record for %s", cast_name)
                                continue
                            LOGGER.info("** PERSON RECORD CREATION: %s - %s - %s", cast_type, person_priref, cast_name)

                            # Append biography and other data
                            if len(person_priref) > 5:
                                payload = create_payload(person_priref, known_for, early_life, bio, trivia)
                                if len(payload) > 90:
                                    success = write_payload(payload, person_priref)
                                    if success:
                                        LOGGER.info("** Payload data successfully written to Person record %s, %s", person_priref, person_name)
                                        print(f"** PAYLOAD WRITTEN TO PERSON RECORD {person_priref}")
                                    else:
                                        LOGGER.critical("Payload data write failed for %s, %s", person_priref, person_name)
                                        print(f"PAYLOAD NOT WRITTEN TO PERSON RECORD {person_priref}")

                        # Build cred_list for sorting/creation of cred_dct_update to append to CID Work
                        for key_, val_ in contributors.items():
                            if str(cast_type) == str(key_):
                                cast_term_code = val_[2]
                                cast_credit_type = val_[0]
                        cast_sort = str(cast_sort)
                        cast_seq_sort = f"{cast_term_code}{cast_sort.zfill(4)}"
                        cast_data = ([int(cast_seq_sort), int(cast_sort), person_priref, cast_credit_type, screen_name])
                        cast_list.append(cast_data)

            else:
                LOGGER.info("No Cast dictionary information for work %s", title)

            cast_list.sort()
            cast_dct_sorted = sort_cast_dct(cast_list)

            person_priref, person_name, person_act_type = '', '', ''
            # Create credit data records
            if len(cred_dct) > 0 and time_match:
                for key, val in cred_dct.items():
                    cred_sort = str(key)
                    cred_sort.zfill(2)
                    for k, v in val.items():
                        cred_type = k
                        cred_type = cred_type.lower()
                        cred_id = v[0]
                        cred_name = firstname_split(v[1])

                        # Check person record exists
                        person_priref, person_name, person_act_type = cid_person_check(cred_id)
                        if len(person_priref) > 0:
                            for k_, v_ in production.items():
                                if str(cred_type) == k_:
                                    activity_type_cred = v_[1]
                                    if str(activity_type_cred) in str(person_act_type):
                                        print(f"Matched activity type {activity_type_cred} : {person_act_type}")
                                    else:
                                        print(f"Activity types do not match. Appending NEW ACTIVITY TYPE: {activity_type_cred}")
                                        success = append_activity_type(person_priref, person_act_type, activity_type_cred)
                                        if success is True:
                                            LOGGER.info("Activity type appended successfully to person: %s", person_priref)
                                        else:
                                            LOGGER.warning("Activity type was not appended to person: %s", person_priref)
                            print(f"** Person record already exists: {person_name} {person_priref}")
                            LOGGER.info("** Person record already exists for %s: %s", person_name, person_priref)
                            LOGGER.info("Cast Name/Priref extacted and will append to cast_dct_update")
                        else:
                            cred_dct_data = ''
                            # Create data for Person record creation
                            cred_dct_data = make_person_dct(val)
                            cred_dct_formatted = cred_dct_data[0]
                            cred_known_for = cred_dct_data[1]
                            cred_early_life = cred_dct_data[2]
                            cred_bio = cred_dct_data[3]
                            cred_trivia = cred_dct_data[4]

                            # Make Person record
                            person_priref = make_person_record(cred_dct_formatted)
                            if not person_priref:
                                LOGGER.warning("Failure to create person record for %s", person_name)
                                continue

                            LOGGER.info("** PERSON RECORD CREATION: %s - %s - %s", cred_type, person_priref, cred_name)
                            # Append biography and other data
                            if len(person_priref) > 5:
                                payload = create_payload(person_priref, cred_known_for, cred_early_life, cred_bio, cred_trivia)
                            if len(payload) > 90:
                                success = write_payload(payload, person_priref)
                                if success:
                                    LOGGER.info("** Payload data successfully written to Person record %s, %s", person_priref, person_name)
                                    print(f"** PAYLOAD WRITTEN TO PERSON RECORD {person_priref}")
                                else:
                                    LOGGER.critical("Payload data write failed for %s, %s", person_priref, person_name)
                                    print(f"PAYLOAD NOT WRITTEN TO PERSON RECORD {person_priref}")

                        # Build cred_list for sorting/creation of cred_dct_update to append to CID Work
                        for key_, val_ in production.items():
                            if str(cred_type) == str(key_):
                                term_code = val_[2]
                                credit_type = val_[0]
                        cred_sort = str(cred_sort)
                        seq_sort = f"{term_code}{cred_sort.zfill(4)}"
                        cred_data = ([int(seq_sort), int(cred_sort), person_priref, credit_type])
                        cred_list.append(cred_data)

            else:
                LOGGER.info("No Credit dictionary information for work %s", title)

            cred_list.sort()
            cred_dct_sorted = sort_cred_dct(cred_list)
            # Append cast/credit and edit name blocks to work_append_dct
            work_append_dct = []
            work_append_dct.extend(cast_dct_sorted)
            work_append_dct.extend(cred_dct_sorted)
            work_edit_data = ([{'edit.name': 'datadigipres'},
                               {'edit.date': TODAY_DATE},
                               {'edit.time': str(datetime.datetime.now())[11:19]},
                               {'edit.notes': 'Automated cast and credit update from PATV augmented EPG metadata'}])
            work_append_dct.extend(work_edit_data)
            LOGGER.info("** Appending data to work record now...")
            print(work_append_dct)

            work_append(work_priref, work_append_dct)
            LOGGER.info("Checking work_append_dct written to CID Work record")

            edit_name = cid_work_check(f"priref='{work_priref}'")[1]
            if 'datadigipres' in str(edit_name):
                print(f"Work appended successful! {work_priref}")
                LOGGER.info("Successfully appended additional cast credit EPG metadata to Work record %s\n", work_priref)
            else:
                LOGGER.warning("Writing EPG cast credit metadata to Work %s failed\n", work_priref)
                print(f"Work append FAILED!! {work_priref}")
            rename(root, file, work_priref)

    LOGGER.info("=============== END document_stora_castcred script END ===============\n")


def sort_cast_dct(cast_list):
    '''
    Make up new cast dct ordered
    '''
    cast_dct_update = []

    for item in cast_list:
        cast_dct_update.append({'cast.name.lref': item[2]})
        cast_dct_update.append({'cast.credit_type': item[3]})
        cast_dct_update.append({'cast.credit_on_screen': item[4]})
        cast_dct_update.append({'cast.sequence': str(item[1])})
        cast_dct_update.append({'cast.sequence.sort': str(item[0])})
        cast_dct_update.append({'cast.section': '[normal cast]'})

    return cast_dct_update


def sort_cred_dct(cred_list):
    '''
    Make up new credit dct ordered
    '''
    cred_dct_update = []

    for item in cred_list:
        cred_dct_update.append({'credit.name.lref': item[2]})
        cred_dct_update.append({'credit.type': item[3]})
        cred_dct_update.append({'credit.sequence': str(item[1])})
        cred_dct_update.append({'credit.sequence.sort': str(item[0])})
        cred_dct_update.append({'credit.section': '[normal credit]'})

    return cred_dct_update


def append_activity_type(person_priref, old_act_type, activity_type):
    '''
    Append activity type to person record if different
    '''
    act_type = [{'activity_type': activity_type}]
    for act in old_act_type:
        act_type.append({'activity_type': act})

    # Convert dict to xml using adlib
    xml = adlib.create_record_data(person_priref, act_type)
    if xml:
        print(xml)
    else:
        return None

    # Create basic person record
    try:
        LOGGER.info("Attempting to append activity type to Person record %s", person_priref)
        record = adlib.post(CID_API, xml, 'people', 'updaterecord')
        if record is None:
            print(f"Unable to write activity type to Person record")
            return False
        return True
    except Exception as err:
        if 'bool' in str(err):
            LOGGER.critical('append_activity_type():Unable to update People record', err)
            print(f"*** Unable to update activity_type to People record - error: {err}")
            return False
        print(f"*** Unable to update People record: {err}")
        LOGGER.critical('append_activity_type():Unable to update People record', err)
        raise


def make_person_dct(dct=None):
    '''
    Make a new person dct with supplied data
    '''
    if dct is None:
        dct = []
        LOGGER.warning("make_person_dct(): Credit dictionary not received")

    credit_dct = []

    for item in dct:
        key = item
        value = dct[item]
        formatted_name = firstname_split(value[1])
        # Making person dictionary
        credit_dct.append({"name": f"{formatted_name}"})
        credit_dct.append({'name.type': 'CASTCREDIT'})
        credit_dct.append({'name.type': 'PERSON'})
        credit_dct.append({'name.status': '5'})
        if len(value[2]) > 0:
            credit_dct.append({'birth.date.start': value[2]})
        if len(value[3]) > 0:
            credit_dct.append({'death.date.start': value[3]})
        if len(value[4]) > 0:
            credit_dct.append({'birth.place': value[4]})
        if len(value[5]) > 0:
            credit_dct.append({'gender': value[5]})
        credit_dct.append({'party.class': 'PERSON'})
        credit_dct.append({'alternative_number': value[0]})
        credit_dct.append({'alternative_number.type': 'PATV Person ID'})
        credit_dct.append({'utb.content': value[0]})
        credit_dct.append({'utb.fieldname': 'PATV Person ID'})

        # Key definition conversion using dictionary
        for k, v in contributors.items():
            if key == k:
                credit_dct.append({'activity_type': v[1]})
        for k, v in production.items():
            if key == k:
                credit_dct.append({'activity_type': v[1]})
        credit_dct.append({'record_access.user': 'BFIiispublic'})
        credit_dct.append({'record_access.rights': '0'})
        credit_dct.append({'record_access.reason': 'SENSITIVE_LEGAL'})
        credit_dct.append({'input.name': 'datadigipres'})
        credit_dct.append({'input.notes': 'Automated creation from PATV augmented EPG metadata'})
        credit_dct.append({'input.time': str(datetime.datetime.now())[11:19]})
        credit_dct.append({'input.date': TODAY_DATE})
        try:
            known_for = codecs.decode(str(value[6]), 'unicode_escape')
        except (KeyError, IndexError):
            known_for = ''
        try:
            early_life = codecs.decode(str(value[8]), 'unicode_escape')
        except (KeyError, IndexError):
            early_life = ''
        try:
            biography = codecs.decode(str(value[7]), 'unicode_escape')
        except (KeyError, IndexError):
            biography = ''
        try:
            trivia = codecs.decode(str(value[9]), 'unicode_escape')
        except (KeyError, IndexError):
            trivia = ''

    return (credit_dct, known_for, early_life, biography, trivia)


def make_person_record(credit_dct=None):
    '''
    Where person record does not exist create new one
    and return priref for person for addition to work record
    '''
    if credit_dct is None:
        credit_dct = []
        LOGGER.warning("make_person_record(): Person record dictionary not received")

    # Convert dict to xml using adlib
    credit_xml = adlib.create_record_data('', credit_dct)
    if credit_xml:
        print("*************************")
        print(credit_xml)
    else:
        return None

    # Create basic person record
    LOGGER.info("Attempting to create Person record for item")
    try:
        record = adlib.post(CID_API, credit_xml, 'people', 'insertrecord')
    except (IndexError, TypeError, KeyError) as err:
        LOGGER.critical('make_person_record():Unable to create People record', err)
        return None
    try:
        credit_priref = adlib.retrieve_field_name(record, 'priref')[0]
        if not credit_priref:
            print(f"Unable to write Person record")
            return None
    except (IndexError, TypeError, KeyboardInterrupt):
        return None
    return credit_priref


def work_append(priref, work_dct=None):
    '''
    Items passed in work_dct for amending to Work record
    '''
    if work_dct is None:
        LOGGER.warning("work_append(): work_update_dct passed to function as None")
        return False

    work_dct_xml = adlib.create_record_data(priref, work_dct)
    try:
        rec = adlib.post(CID_API, work_dct_xml, 'works', 'updaterecord')
        if rec:
            return True
    except Exception as err:
        LOGGER.warning("work_append(): Unable to append work data to CID work record %s", err)
        return False


def rename(root, file, info):
    '''
    Rename {filename}.json.documented
    to {filename}.json.documented_castcred
    Confirm success
    '''
    fullpath = os.path.join(root, file)
    new_fname = f"{file}_castcred"
    new_path = os.path.join(root, new_fname)
    try:
        os.rename(fullpath, new_path)
        print(f" --- RENAME {fullpath} TO {new_path} ---")
    except OSError:
        LOGGER.critical("%s not renamed %s:\n - %s", file, new_fname, info)


def create_payload(priref, known_for, early_life, bio, trivia):
    '''
    Take string blocks and wrap in xml for appending to CID person record
    '''
    payload = []
    payload1, payload2, payload4 = '', '', ''
    payload_head = f'<adlibXML><recordList><record priref="{priref}">'
    payload = payload_head

    if len(known_for) > 0 and len(early_life) > 0:
        string1 = f"Best known for: {known_for} "
        string2 = f"{early_life}"
        payload1 = f'<name.note><![CDATA[{string1}{string2}]]></name.note>'
        payload = payload + payload1
    elif len(known_for) > 0 and len(early_life) == 0:
        string1 = f"Best known for: {known_for}"
        payload1 = f'<name.note><![CDATA[{string1}]]></name.note>'
        payload = payload + payload1
    elif len(known_for) == 0 and len(early_life) > 0:
        string2 = f"Best known for: {early_life}"
        payload1 = f'<name.note><![CDATA[{string2}]]></name.note>'
        payload = payload + payload1
    if len(bio) > 0:
        payload2 = f'<biography><![CDATA[{bio}]]></biography>'
        payload = payload + payload2
    if len(trivia) > 0:
        string4 = f"Trivia: {trivia}"
        payload4 = f'<general_context><![CDATA[{string4}]]></general_context>'
        payload = payload + payload4

    payload_tail = '</record></recordList></adlibXML>'
    payload = payload + payload_tail
    return payload


def write_payload(payload, person_priref):
    '''
    Removed from main to avoid repetition
    '''
    print('REQUESTS: Sending POST request to people database to lock record')
    
    try:
        record = adlib.post(CID_API, payload, 'people', 'updaterecord')
        print(record)
    except Exception as err:
        LOGGER.warning("write_payload(): WRITE TO CID PERSON RECORD %s FAILED:\n%s", person_priref, err)
    if "error" in str(record):
        return False
    else:
        return True


if __name__ == '__main__':
    main()
