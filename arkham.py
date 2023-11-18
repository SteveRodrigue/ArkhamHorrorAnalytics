#!/usr/bin/python3

"""Modules required"""
from datetime import datetime
import json
from os import walk
import threading
from queue import Queue
import hashlib
import pickle
import urllib.request
import urllib.error
import re
# import sys  # Keep it for debug

# Init vars
# How many threads to run in parallel (Faster with a value of 1!?!?!)
NB_THREAD = 4
# Location of the root directory of ArkhamDB API cache
DB_PATH = '/home/leko/projects/arkham/db/'
DECKS_PATH = DB_PATH + 'decks/'             # Location of the decks cache
CARDS_PATH = DB_PATH + 'cards/'             # Location of the decks cache
HTML_PATH = 'html/'
TEXT_PATH = 'text/'
ARKHAM_DB_API = 'https://arkhamdb.com/api/public/'
RELEVANCE = 0.10
queue = Queue()                 # Init the empty queue
queue_inv_aff = Queue()         # Init an empty queue for affinities
thread_list = []                # Empty thread list
thread_aff_list = []            # Empty thread list
thread_aff_list_xp = []         # Empty thread list
affinity_investigators = {}
affinity_investigators_xp = {}  #
affinity_cards = {}
decks_grouped_by_hash = {}
card_cache = {}

# FUNCTION DEFINITIONS STARTS HERE


def arkham_db_api(file_name):
    """"Call Arkham DB cache"""
    if file_name.find('cards') > 0:
        oper = 'card/'
        dest_folder = CARDS_PATH
    if file_name.find('decks') > 0:
        oper = 'decklist/'
        dest_folder = DECKS_PATH
    match = re.search(r'[0-9]{5}', file_name)
    with open_url(ARKHAM_DB_API + oper
                  + str(match.group(0)) + '.json') as response:
        json_content = json.loads(response.read())
        json_to_file(json_content, dest_folder + str(match.group(0)) + '.json')
        return json_content


def open_url(request):
    """Return URL content"""
    try:
        return urllib.request.urlopen(request)
    except urllib.error.HTTPError as err:
        # "err" can be treated as a http.client.HTTPResponse object
        return err


def file_to_json(file_name):
    """Return a JSON data structure from a file"""
    try:
        with open(file_name, encoding="utf-8") as file:
            return json.load(file)
    except IOError:
        return arkham_db_api(file_name)


def write_to_file(content, filename):
    """Write (any) content to file"""
    with open(filename, 'w', encoding="utf-8") as file:
        file.write(content)


def json_to_file(json_content, filename):
    """Push json content into a file"""
    write_to_file(json.dumps(json_content, indent=4), filename)


def dict_order_by_keys(dict_to_order):
    """Reorder a dictionary by keys"""
    keys = list(dict_to_order.keys())
    keys.sort()
    return {key: dict_to_order[key] for key in keys}


def fill_queue(filler_list):
    """Fill the queue with content"""
    for temp_item in filler_list:
        queue.put(temp_item)


def deck_deduplicate(slots):
    """Replace duplicate card ID with their original ID"""
    dedup_dict = {}
    for slot in slots:
        if slot in duplicates:
            dedup_dict.update({duplicates[slot]: slots[slot]})
        else:
            dedup_dict.update({slot: slots[slot]})
    # Reorder the list of card from low to high card ID.
    return dict_order_by_keys(dedup_dict)


def filter_out_cards(slots):
    # @todo: Add cleaning/filtering of decks before processing
    #        - remove non player cards
    #        - basic random weakness card
    #        - basic weakness
    #        - scenario cards
    """Filter out useless cards..."""
    output_slots = {}
    for slot in slots:
        reject = False
        if slot == "01000":
            reject = True
        # Reject Encounter cards
        if get_card(slot).get('encounter_code'):
            reject = True
        # Card wasn't rejected...
        if not reject:
            output_slots.update({slot: slots[slot]})
    return output_slots


def get_card(card_id):
    '''Get specific card information'''
    if not card_cache.get(card_id):
        # Load card in cache...
        card_cache.update({card_id: file_to_json(CARDS_PATH + str(card_id)
                                                 + '.json')})
    return card_cache.get(card_id)


def deck_level(deck_data):
    '''Return the XP spent in this deck'''
    total_xp = 0
    for slot in deck_data['slots']:
        try:
            total_xp = total_xp + (int(get_card(slot).get('xp')) *
                                   deck_data['slots'][slot])
        except TypeError:
            pass
    return total_xp


def worker():
    """Main worker function"""
    while not queue.empty():
        deck = queue.get()
        # Open/clost the deck file
        content = file_to_json(DECKS_PATH + deck)
        content['slots'] = filter_out_cards(content['slots'])
        # Check if the deck contains duplicate
        # Replace duplicated cards in deck
        dedup_slots = deck_deduplicate(content['slots'])
        # Make sure the OG deck is in asc order
        deck_slots = dict_order_by_keys(content['slots'])
        # Compute md5 hashes
        dedup_hash = hashlib.md5(pickle.dumps(dedup_slots)).hexdigest()
        deck_hash = hashlib.md5(pickle.dumps(deck_slots)).hexdigest()
        # Compare original deck to deduplicated
        if dedup_hash != deck_hash:
            print('Cards in deck ' + deck +
                  ' were replaced by their original card ID.')
            deck_hash = dedup_hash
            content['slots'] = dedup_slots
        # Delete variables that won't be used anymore
        del dedup_hash
        del dedup_slots
        # The same deck exists...
        if deck_hash in decks_grouped_by_hash:
            print('Deck ' + str(content['id']) + ' is identical to: ' +
                  str(decks_grouped_by_hash[deck_hash]))
            # Build data for duplicated decks...
            # Simple list of deck duplicate of
            # Group duplicated decks together
            if decks_grouped_by_hash.get(deck_hash):
                decks_grouped_by_hash[deck_hash] = \
                    sorted(decks_grouped_by_hash[deck_hash] + [content['id']])
            else:
                decks_grouped_by_hash[deck_hash] = [content['id']]
        else:
            # @todo verify if the deck is legit !!! Example: 27554 is illegal!
            decks_grouped_by_hash[deck_hash] = [content['id']]
            # Process starter decks...
            if deck_level(content) == 0:
                process_base_deck(content)
            # Non-starter decks...
            else:
                process_xp_deck(content)
                # print(str(content['id']) + ' Total XP is ' +
                #       str(deck_level(content)))
                # sys.exit()


def return_file_content(filename):
    """Return the file content of a file"""
    with open(filename, encoding="utf-8") as file:
        content = file.read()
    return content


def value_getter(pass_item):
    """Use to get the value"""
    return pass_item[1]


def worker_inv_aff():
    """Worker for affinities"""
    while not queue_inv_aff.empty():
        inv = queue_inv_aff.get()
        # Make some variables more easily accessible...
        current_aff = affinity_investigators[inv]
        reorg = sorted(current_aff.items(), key=value_getter, reverse=True)
        # Get current investigator information
        get_card(inv)
        # Create the header of the file
        txt_output = '\n==== Investigator ' + card_cache[inv]['name'] \
            + ' ====\n\n'
        html_output = " \
<!doctype html>\n \
<html>\n \
<head>\n \
<title>" + card_cache[inv]['name'] + "</title>\n \
<meta name=\"description\" content=\"Investigator " \
+ card_cache[inv]['name'] + " card affinity\">\n \
<meta name=\"keywords\" content=\"arkham horror card game\">\n \
</head>\n \
<body>\n \
" + card_cache[inv]['back_flavor'] + "<br />\n \
<img src=\"https://arkhamdb.com/bundles/cards/" \
+ card_cache[inv]['code'] + ".png\" /><br />\n"
        max_value = 0  # We set the max value to zero
        for code, value in reorg:
            # Increment max value if necessary...
            if value > max_value:
                max_value = value
                html_output = html_output + "Stats based on " + \
                    str(max_value) + " decks<br />\n"
            # Only keep the cards that are used in more than 10% of the decks
            if value > (max_value * RELEVANCE):
                html_output = html_output + \
                    "<img " + "src=\"https://arkhamdb.com/bundles/cards/" \
                    + str(code) + ".png\" />\n"
                # Without card ID
                # txt_output = txt_output + card_cache[code]['name'] + ' [' + \
                #     str(value) + ', ' + str(round(value*100/max_value, 1)) \
                #     + '%]\n'
                # With card ID
                txt_output = txt_output + get_card(code).get('name') + ' (' \
                    + str(code) + ') [' + str(value) + ', ' + \
                    str(round(value*100/max_value, 1)) + '%]\n'
        print(txt_output)
        write_to_file(txt_output, TEXT_PATH + 'inv_aff_' +
                      card_cache[inv]['name'].replace(" ", "_") + '.txt')
        write_to_file(html_output, HTML_PATH + 'inv_aff_' +
                      card_cache[inv]['name'].replace(" ", "_") + '.html')


def worker_inv_aff_xp():
    """Worker for affinities"""
    while not queue_inv_aff.empty():
        inv = queue_inv_aff.get()
        # Make some variables more easily accessible...
        current_aff = affinity_investigators_xp[inv]
        reorg = sorted(current_aff.items(), key=value_getter, reverse=True)
        # Get current investigator information
        get_card(inv)
        # Create the header of the file
        txt_output = '\n==== Investigator ' + card_cache[inv]['name'] \
            + ' (XP cards) ====\n\n'
        html_output = " \
<!doctype html>\n \
<html>\n \
<head>\n \
<title>" + card_cache[inv]['name'] + "</title>\n \
<meta name=\"description\" content=\"Investigator " \
+ card_cache[inv]['name'] + " XP card affinity\">\n \
<meta name=\"keywords\" content=\"arkham horror card game\">\n \
</head>\n \
<body>\n \
" + card_cache[inv]['back_flavor'] + "<br />\n \
<img src=\"https://arkhamdb.com/bundles/cards/" \
+ card_cache[inv]['code'] + ".png\" /><br />\n"
        max_value = 0  # We set the max value to zero
        for code, value in reorg:
            # Increment max value if necessary...
            if value > max_value:
                max_value = value
                html_output = html_output + "Stats based on " + \
                    str(max_value) + " decks<br />\n"
            # Only keep the cards that are used in more than 10% of the decks
            if 'xp' in card_cache[code].keys():
                if card_cache[code]['xp'] > 0:
                    if value > (max_value * RELEVANCE / 2):
                        html_output = html_output + \
                            "<img " + "src=\"https://arkhamdb.com/bundles/cards/" \
                            + str(code) + ".png\" />\n"
                        # Without card ID
                        # txt_output = txt_output + card_cache[code]['name'] + ' [' + \
                        #     str(value) + ', ' + str(round(value*100/max_value, 1)) \
                        #     + '%]\n'
                        # With card ID
                        txt_output = txt_output + get_card(code).get('name') + ' (' \
                            + str(code) + ') [' + str(value) + ', ' + \
                            str(round(value*100/max_value, 1)) + '%]\n'
        print(txt_output)
        write_to_file(txt_output, TEXT_PATH + 'inv_aff_' +
                      card_cache[inv]['name'].replace(" ", "_") + '_xp.txt')
        write_to_file(html_output, HTML_PATH + 'inv_aff_' +
                      card_cache[inv]['name'].replace(" ", "_") + '_xp.html')


def process_base_deck(deck_data):
    """Process a deck"""
    if not deck_data['investigator_code'] in affinity_investigators:
        inv_affinity = {}
    else:
        inv_affinity = affinity_investigators[deck_data['investigator_code']]
    # Increase investigator affinity value...
    for slot in deck_data['slots']:
        # Increase the value for the current investigator
        if inv_affinity.get(slot):
            new_inv_value = inv_affinity[slot] + 1
        else:
            new_inv_value = 1
        inv_affinity.update({slot: new_inv_value})
        # Process each slot indidually...
        for other_slot in deck_data['slots']:
            # We exclude own...
            if other_slot != slot:
                # We check if affinities already exists for this card...
                if affinity_cards.get(slot):
                    # If an affinity is found...
                    if affinity_cards[slot].get(other_slot):
                        # We increment exising value
                        new_slot_value = affinity_cards[slot][other_slot] + 1
                    else:
                        # Else, value is forced to 1
                        new_slot_value = 1
                else:
                    affinity_cards[slot] = {}
                    new_slot_value = 1
                affinity_cards[slot].update({other_slot: new_slot_value})
        affinity_cards[slot] = dict_order_by_keys(affinity_cards[slot])
    # Processing _after_ all slots were parse
    # Put the investigator value back in the dict...
    affinity_investigators.update(
        {deck_data['investigator_code']: dict_order_by_keys(inv_affinity)})


def process_xp_deck(deck_data):
    """Process a deck"""
    if not deck_data['investigator_code'] in affinity_investigators_xp:
        inv_affinity = {}
    else:
        inv_affinity = \
            affinity_investigators_xp[deck_data['investigator_code']]
    # Increase investigator affinity value...
    for slot in deck_data['slots']:
        # Increase the value for the current investigator
        if inv_affinity.get(slot):
            new_inv_value = inv_affinity[slot] + 1
        else:
            new_inv_value = 1
        inv_affinity.update({slot: new_inv_value})
        # Process each slot indidually...
        for other_slot in deck_data['slots']:
            # We exclude own...
            if other_slot != slot:
                # We check if affinities already exists for this card...
                if affinity_cards.get(slot):
                    # If an affinity is found...
                    if affinity_cards[slot].get(other_slot):
                        # We increment exising value
                        new_slot_value = affinity_cards[slot][other_slot] + 1
                    else:
                        # Else, value is forced to 1
                        new_slot_value = 1
                else:
                    affinity_cards[slot] = {}
                    new_slot_value = 1
                affinity_cards[slot].update({other_slot: new_slot_value})
        affinity_cards[slot] = dict_order_by_keys(affinity_cards[slot])
    # Processing _after_ all slots were parse
    # Put the investigator value back in the dict...
    affinity_investigators_xp.update(
        {deck_data['investigator_code']: dict_order_by_keys(inv_affinity)})


#
# Code begins here...
#
start_time = datetime.now()                 # Start time for statistics only
print('Arkham Horror Data and Stats...')

# Load list of decks...
list_of_deck = []
for (DECKS_PATH, dir_names, file_names) in walk(DECKS_PATH):
    list_of_deck.extend(file_names)

# Load duplicate cards list
duplicates = file_to_json(DB_PATH + 'all/duplicates.json')

# Fill the queue with the deck list
fill_queue(list_of_deck)

#
# Create threads that will execute workers
# This worker builds the generic stats
#
for t in range(NB_THREAD):
    thread = threading.Thread(target=worker)
    thread_list.append(thread)

# Start threads
for thread in thread_list:
    thread.start()

# Make sure all threads are done
for thread in thread_list:
    thread.join()

#
# Based on the raw stats execute workers
# Per investigators stats/data.
#
for item in affinity_investigators:
    queue_inv_aff.put(item)

for t in range(NB_THREAD):
    thread_aff = threading.Thread(target=worker_inv_aff)
    thread_aff_list.append(thread_aff)

# Start threads
for thread in thread_aff_list:
    thread.start()

# Make sure all threads are done
for thread in thread_aff_list:
    thread.join()

#
# Based on the raw stats execute workers
# Per investigators stats/data.
#
for item in affinity_investigators_xp:
    queue_inv_aff.put(item)

for t in range(NB_THREAD):
    thread_aff_xp = threading.Thread(target=worker_inv_aff_xp)
    thread_aff_list_xp.append(thread_aff_xp)

# Start threads
for thread in thread_aff_list_xp:
    thread.start()

# Make sure all threads are done
for thread in thread_aff_list_xp:
    thread.join()

#
# Post processing...
#

json_to_file(dict_order_by_keys(affinity_investigators), 'aff_inv.json')
json_to_file(dict_order_by_keys(affinity_cards), 'aff_cards.json')
json_to_file(dict_order_by_keys(decks_grouped_by_hash),
             'decks_grouped_by_hash.json')

print('\n\n')
print('Unique decks :    ' + str(len(decks_grouped_by_hash)))
print('Duplicated decks: ' + str(len(list_of_deck) -
                                 len(decks_grouped_by_hash)))
print('Total decks:      ' + str(len(list_of_deck)))

print(f"\nNumber of thread(s) used: {NB_THREAD}")
print(f"Runtime {format(datetime.now() - start_time)}.")
