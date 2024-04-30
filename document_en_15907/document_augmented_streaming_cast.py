#!/usr/bin/env python3

'''
Script to create People records from EPG metadata
dict and attach to existing CID Work records

1. Receive EPG dictionary from augmented streaming platform scripts
   plus work priref and nfa_category
   - Extract contributor data from EPG metadata
2. Look in CID for matching people PATV IDs person dB
   Where matching data is found, extract addition data from EPG source file:
   - Append activity type if different
   - Extract Person record priref from record
   Where matching data is not found
   - Create new Person record in Person database using EPG data, write EPG UID
3. Create dictionary link of each new / found Person record to CID Work
4. Appended to the CID work and return boole for success

NOTES: Can in time be used for other streaming platforms
       with update to input note check for non-platform specific name
       Updated for Adlib V3

Joanna White
2023
'''

# Global packages
import os
import sys
import json
import codecs
import logging
import datetime
import requests

# Local packages
sys.path.append(os.environ['CODE'])
import adlib_v3 as adlib

# Global vars
LOG_PATH = os.environ['LOG_PATH']
CID_API = os.environ['CID_API4']

# Setup logging
LOGGER = logging.getLogger('document_streaming_castcred')
HDLR = logging.FileHandler(os.path.join(LOG_PATH, 'document_streaming_castcred.log'))
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


def cid_check():
    '''
    Check CID online or exit
    '''
    try:
        adlib.check(CID_API)
    except KeyError:
        print("* Cannot establish CID session, exiting script")
        LOGGER.critical("* Cannot establish CID session, exiting script")
        sys.exit()


def enum_list(creds):
    '''
    Change list to dictionary pairs for sort order
    Increments of 5, beginning at 5
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

    # Convert list to dict {50: {'actor': '..'}, 55: {'actor': '..'}}
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
    record = adlib.retrieve_record(CID_API, 'people', search, '0', ['priref', 'name', 'activity_type'])
    if not record:
        LOGGER.exception("cid_person_check(): Unable to check for person record with credit id: %s", credit_id)
        return None
    try:
        name = adlib.retrieve_field_name(record[0], 'name')[0]
    except (KeyError, IndexError):
        name = ''
    try:
        priref = adlib.retrieve_field_name(record[0], 'priref')[0]
    except (KeyError, IndexError):
        priref = ''
    try:
        activity_type = adlib.retrieve_field_name(record[0], 'activity_type')[0]
    except (KeyError, IndexError):
        activity_type = ''
    return priref, name, activity_type


def cid_work_check(search, platform):
    '''
    Retrieve CID work record priref where search matches
    '''
    prirefs = []
    edit_names = []
    platform = platform.title()

    records = adlib.retrieve_record(CID_API, 'works', search, '0', ['priref', 'input.notes', 'edit.name'])
    if not records:
        LOGGER.exception("cid_work_check(): Unable to check for person record with search %s", search)
        return '', ''

    print(records)
    for record in records:
        try:
            priref = adlib.retrieve_field_name(record[0], 'priref')[0]
            input_note = adlib.retrieve_field_name(record[0], 'input.notes')[0]
        except (KeyError, IndexError):
            priref = ''
            input_note = ''
        try:
            edit_name = adlib.retrieve_field_name(record[0], 'edit.name')[0]
        except (KeyError, IndexError):
            edit_name = ''

        if f'{platform} metadata integration' in str(input_note):
            prirefs.append(priref)
            edit_names.append(edit_name)

    return prirefs, edit_names


def cid_manifestation_check(priref):
    '''
    Retrieve Manifestation transmission start time from parent priref
    '''
    search = f"(part_of_reference.lref='{priref}')"
    record = adlib.retrieve_record(CID_API, 'manifestations', search, '0', ['transmission_start_time'])
    if not record:
        LOGGER.exception("cid_manifestation_check(): Unable to check for record with priref: %s", priref)
        return ''
    try:
        start_time = adlib.retrieve_field_name(record[0], 'transmission_start_time')[0]
    except (KeyError, IndexError):
        LOGGER.info("cid_manifestation_check(): Unable to extract start time for manifestation")
        start_time = ''

    return start_time


def create_contributors(priref, nfa_cat, credit_list, platform):
    '''
    Iterate dct extracting cast/credit and other metadata
    Check in CID for existing People records and extract priref
    Create new People rec where needed and capture priref,
    return link new/existing People priref to CID Work
    '''
    cid_check()
    if not credit_list:
        return None

    LOGGER.info("============= START document_augmented_streaming_castcred script START =============")
    LOGGER.info("Retrieved contributors for priref %s", priref)

    # Get people data
    cast_dct, cred_dct = retrieve_person(credit_list, nfa_cat)
    print(cast_dct)
    print(cred_dct)

    cast_list = []
    cred_list = []
    name_list = []

    # BEGIN CAST DATA GENERATION
    person_priref, person_name, person_act_type = '', '', ''
    if len(cast_dct) > 0:
        for key, val in cast_dct.items():
            cast_sort = str(key)
            cast_sort.zfill(2)
            for k, v in val.items():
                cast_type = k
                cast_id = v[0]
                cast_name = firstname_split(v[1])
                screen_name = v[11]
                name_list.append(cast_name)

                # Check person record exists
                person_priref, person_name, person_act_type = cid_person_check(cast_id)
                if len(person_priref) > 5:
                    for k_, v_ in contributors.items():
                        if str(cast_type) == k_:
                            activity_type = v_[1]
                            if str(activity_type) in str(person_act_type):
                                LOGGER.info("MATCHED Activity types: %s with %s", activity_type, person_act_type)
                            else:
                                LOGGER.info("** Activity type does not match. Appending NEW ACTIVITY TYPE: %s", activity_type)
                                append_activity_type(person_priref, activity_type, person_act_type)
                    print(f"** Person record already exists: {person_name} {person_priref}")
                    LOGGER.info("** Person record already exists for %s: %s", person_name, person_priref)
                    LOGGER.info("Cast Name/Priref extacted and will append to cast_dct_update")
                else:
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
                        LOGGER.info("Creation of person record failed. Skipping this record for return attempt.")
                        continue
                    LOGGER.info("** PERSON RECORD CREATION: %s - %s - %s", cast_type, person_priref, cast_name)
                    # Append biography and other data
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
        LOGGER.info("No Cast dictionary information for supplied contributors")

    cast_list.sort()
    cast_dct_sorted = sort_cast_dct(cast_list)

    person_priref, person_name, person_act_type = '', '', ''
    # Create credit data records
    if len(cred_dct) > 0:
        for key, val in cred_dct.items():
            cred_sort = str(key)
            cred_sort.zfill(2)
            for k, v in val.items():
                cred_type = k
                cred_type = cred_type.lower()
                cred_id = v[0]
                cred_name = firstname_split(v[1])
                name_list.append(cred_name)

                # Check person record exists
                person_priref, person_name, person_act_type = cid_person_check(cred_id)
                if len(person_priref) > 5:
                    LOGGER.info("Person record already exists: %s %s", person_name, person_priref)
                    for k_, v_ in production.items():
                        if str(cred_type) == k_:
                            activity_type_cred = v_[1]
                            if str(activity_type_cred) in str(person_act_type):
                                print(f"Matched activity type {activity_type_cred} : {person_act_type}")
                            else:
                                print(f"Activity types do not match. Appending NEW ACTIVITY TYPE: {activity_type_cred}")
                                append_activity_type(person_priref, activity_type_cred, person_act_type)
                    print(f"** Person record already exists: {person_name} {person_priref}")
                    LOGGER.info("** Person record already exists for %s: %s", person_name, person_priref)
                    LOGGER.info("Cast Name/Priref extacted and will append to cast_dct_update")
                else:
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
                        LOGGER.info("Creation of person record failed. Skipping this record for return attempt.")
                        continue
                    LOGGER.info("** PERSON RECORD CREATION: %s - %s - %s", cred_type, person_priref, cred_name)
                    # Append biography and other data
                    if len(person_priref) > 5:
                        payload = create_payload(person_priref, cred_known_for, cred_early_life, cred_bio, cred_trivia)
                    if len(payload) > 90:
                        success = write_payload(person_priref, payload)
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
        LOGGER.info("No Credit dictionary information for supplied creditors")

    cred_list.sort()
    cred_dct_sorted = sort_cred_dct(cred_list)

    # Append cast/credit and edit name blocks to work_append_dct
    work_append_dct = []
    work_append_dct.extend(cast_dct_sorted)
    work_append_dct.extend(cred_dct_sorted)
    work_edit_data = ([{'edit.name': 'datadigipres'},
                       {'edit.date': str(datetime.datetime.now())[:10]},
                       {'edit.time': str(datetime.datetime.now())[11:19]},
                       {'edit.notes': 'Automated cast and credit update from PATV augmented EPG metadata'}])
    work_append_dct.extend(work_edit_data)
    LOGGER.info("** Appending data to work record now...")
    print(work_append_dct)

    work_append(priref, work_append_dct)
    LOGGER.info("Checking work_append_dct written to CID Work record")

    edit_name = cid_work_check(f"priref='{priref}'", platform)[1]
    if 'datadigipres' in str(edit_name):
        print(f"Work appended successful! {priref}")
        LOGGER.info("Successfully appended additional cast credit EPG metadata to Work record %s\n", priref)
        LOGGER.info("=============== END document_augmented_streaming_castcred script END ===============\n")
        return (cast_dct, cred_dct)
    else:
        LOGGER.warning("Writing EPG cast credit metadata to Work %s failed\n", priref)
        print(f"Work append FAILED!! {priref}")
        LOGGER.info("=============== END document_augmented_streaming_castcred script END ===============\n")
        return False


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


def append_activity_type(person_priref, activity_type, existing_types):
    '''
    Append activity type to person record if different
    '''
    data = [{'activity_type': activity_type}]
    for act_type in existing_types:
        data.extend([{'activity_type': act_type}])
    act_xml = adlib.create_record_data(person_priref, data)
    try:
        record = adlib.post(CID_API, act_xml, 'people', 'updaterecord')
        print(record)
        return True
    except Exception as err:
        LOGGER.warning("append_activity_type(): Unable to append activity_type to Person record", err)
        return False


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
        credit_dct.append({'input.date': str(datetime.datetime.now())[:10]})
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


def work_append(priref, work_dct=None):
    '''
    Items passed in work_dct for amending to Work record
    '''
    print(work_dct)
    if work_dct is None:
        LOGGER.warning("work_append(): work_update_dct passed to function as None")
        return False
    work_dct_xml = adlib.create_record_data(priref, work_dct)
    record = adlib.post(CID_API, work_dct_xml, 'works', 'updaterecord')
    if record:
        print("*** Work append result:")
        print(record)
        return True
    else:
        LOGGER.warning("work_append(): Unable to append work data to CID work record %s", record)
        return False


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
    if not credit_xml:
        LOGGER.warning("Credit data failed to create XML: %s", credit_dct)
        return None

    # Create basic person record
    LOGGER.info("Attempting to create Person record for item")
    record = adlib.post(CID_API, credit_xml, 'people', 'insertrecord')
    if not record:
        print(f"*** Unable to create People record: {credit_xml}")
        LOGGER.critical('make_person_record():Unable to create People record', err)
    try:
        credit_priref = adlib.retrieve_field_name(record, 'priref')[0]
        return credit_priref
    except Exception as err:
        print(f"*** Unable to create People record: {err}")
        LOGGER.critical('make_person_record():Unable to create People record', err)
        raise


def push_record_create(payload, database, method):
    '''
    Receive adlib formed XML but use
    requests to create the CID record
    '''
    record = adlib.post(CID_API, payload, database, method)
    if not record:
        LOGGER.critical("Unable to create <%s> record with <%s> and payload:\n%s", database, method, payload)
        return None

    try:
        priref = adlib.retrieve_field_name(record, 'priref')[0]
        return priref
    except (IndexError, KeyError):
        LOGGER.critical("Unnable to retrieve priref from new record creation: %s", record)
        return None


def create_payload(priref, known_for, early_life, bio, trivia):
    '''
    Take string blocks, protect any escape characters using !CDATA
    and wrap in xml for appending to CID person record
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
    Receive field data as payload and priref and write to CID person record
    '''
    print(f'======= REQUESTS: Sending POST request to People database {person_priref} ====================')
    LOGGER.info('======= REQUESTS: Sending POST request to People database ==================== %s', person_priref)

    record = adlib.post(CID_API, payload, 'people', 'updaterecord')
    print(record)
    if '@attributes' in str(record):
        return True
    if not record:
        return False
