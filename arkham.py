#!/usr/bin/python3

"""Modules required"""
from datetime import datetime
import json
import threading
from queue import Queue
import hashlib
import pickle
import time
import urllib.request
import urllib.error
import os

# Init vars
# How many threads to run in parallel
# (Increasing the value don't improve performance much on my system)
NB_THREAD = 8
ARKHAM_DB_API = 'https://arkhamdb.com/api/public/'
LAST_DECK = 50000  # Maximum deck ID to try to fetch from ArkhamDB
# LAST_DECK = 10  # Use a low value to test the script.
# Location of the root directory of ArkhamDB API cache
DB_PATH = './db/'
# Location of the root where to store html/text files
OUTPUT_PATH = './output/'
HTML_PATH = OUTPUT_PATH + 'html/'
TEXT_PATH = OUTPUT_PATH + 'text/'
JSON_PATH = OUTPUT_PATH + 'json/'
# To be relevant, a card must be present in at least 10% of the decks.
# This can skew data for newer cards/expansions.
# If this value is set to 0, all cards will be shown.
RELEVANCE = 0.10
queue = Queue()                 # Init the empty queue
queue_inv_aff = Queue()         # Init an empty queue for affinities
thread_list = []                # Empty thread list
thread_aff_list = []            # Empty thread list
thread_aff_list_xp = []         # Empty thread list
affinity_investigators = {}     # Inv. Base card affinity
affinity_investigators_xp = {}  # Inv. XP card affinity
affinity_cards = {}             # Card to card affinity
# Hashing is used to deduplicate decks
decks_grouped_by_hash = {}
card_cache = {}                 # This adds card in memory to reduce file read
valid_decks = []

# FUNCTION DEFINITIONS STARTS HERE


class ArkhamHorrorAnalytics(object):
    def __init__(self):
        # Load duplicate cards list
        self.duplicates = self.file_to_json(DB_PATH + 'other/duplicates.json')
        # Contain decks (id) found in ArkhamDB
        self.valid_decks = []

    #
    # @ToDo: I should find a better way to handle "last existing deck has been
    # reached" on ArkhamDB.
    #
    def arkhamdb_cache(self, oper, uid):
        """"Call Arkham DB cache"""
        # If it's already in cache...
        if card_cache.get(str(uid)):
            return card_cache.get(str(uid))
        # We try to open the file...
        filename = DB_PATH + oper + '/' + str(uid) + '.json'
        if os.path.exists(filename):
            json_to_return = self.open_json_file(filename)
        else:
            url = ARKHAM_DB_API + oper + '/' + str(uid) + '.json'
            json_to_return = self.open_arkham_db_url(url)

        # If the current card isn't in the memory cache, add it...
        if oper == 'card':
            # Load card in cache...
            card_cache.update({str(uid): json_to_return})
        return json_to_return

    def open_json_file(self, filename):
        with open(filename, encoding="utf-8") as file:
            json_to_return = json.load(file)
        return json_to_return

    def open_arkham_db_url(self, url):
        with self.open_url(url) as response:
            extracted_response = response.read()
            # We validate if the response is a valid JSON
            if self.is_json(extracted_response):
                json_content = json.loads(extracted_response)
                # We save the file for future use
                self.json_to_file(json_content,
                                  DB_PATH + oper + '/' + str(uid) + '.json')
                json_to_return = json_content
            else:
                json_to_return = {}
        return json_to_return

    def is_json(self, myjson):
        """"Check if it's valid JSON"""
        try:
            json.loads(myjson)
        except ValueError:
            return False
        return True

    def open_url(self, request, max_retries=3, retry_delay=1):
        """Return URL content with retries"""
        for attempt in range(max_retries):
            try:
                return urllib.request.urlopen(request, timeout=5)
            # HTTP error, we retry...
            except urllib.error.HTTPError:
                if attempt < max_retries - 1:
                    print(f'HTTP error: Retrying in {retry_delay} seconds...')
                    time.sleep(retry_delay)
            # OS Error, we retry...
            except OSError:
                if attempt < max_retries - 1:
                    print(f'OS error: Retrying in {retry_delay} seconds...')
                    time.sleep(retry_delay)

    def file_to_json(self, file_name):
        """Return a JSON data structure from a file"""
        try:
            with open(file_name, encoding="utf-8") as file:
                return json.load(file)
        except IOError:
            return False

    def write_to_file(self, content, filename):
        """Write (any) content to file"""
        with open(filename, 'w', encoding="utf-8") as file:
            file.write(content)

    def json_to_file(self, json_content, filename):
        """Push json content into a file"""
        self.write_to_file(json.dumps(json_content, indent=4), filename)

    def dict_order_by_keys(self, dict_to_order):
        """Reorder a dictionary by keys"""
        keys = list(dict_to_order.keys())
        keys.sort()
        return {key: dict_to_order[key] for key in keys}

    def fill_queue(self, filler_list):
        """Fill the queue with content"""
        for temp_item in filler_list:
            queue.put(temp_item)

    def deck_deduplicate(self, slots):
        """Replace duplicate card ID with their original ID"""
        dedup_dict = {}
        for slot in slots:
            # Why Linting fails on the following line... I don't know!
            if slot in self.duplicates:
                dedup_dict.update({self.duplicates[slot]: slots[slot]})
            else:
                dedup_dict.update({slot: slots[slot]})
        # Reorder the list of card from low to high card ID.
        return self.dict_order_by_keys(dedup_dict)

    def filter_out_cards(self, slots):
        # @todo: Add cleaning/filtering of decks before processing
        #        - remove non player cards (Partial!)
        #        - basic random weakness card (DONE!)
        #        - basic weakness
        #        - scenario cards (Partial!)
        """Filter out useless cards..."""
        output_slots = {}
        for slot in slots:
            reject = False
            if slot == "01000":  # Random basic weakness
                reject = True
            # Reject Encounter cards
            if self.arkhamdb_cache('card', slot).get('encounter_code'):
                reject = True
            # Card wasn't rejected...
            if not reject:
                output_slots.update({slot: slots[slot]})
        return output_slots

    def deck_level(self, deck_data):
        '''Return the XP spent in this deck'''
        total_xp = 0
        for slot in deck_data['slots']:
            try:
                total_xp = total_xp + \
                           (int(self.arkhamdb_cache('card', slot).get('xp')) *
                            deck_data['slots'][slot])
            except TypeError:
                pass
        return total_xp

    def worker(self):
        """Main worker function"""
        # We process a queue...
        while not queue.empty():
            deck_id = queue.get()
            # Open/clost the deck file
            content = self.arkhamdb_cache('decklist', deck_id)
            if len(content):
                print('Deck being parsed: ' + str(deck_id))
                self.valid_decks = self.valid_decks + [deck_id]
                content['slots'] = self.filter_out_cards(content['slots'])
                # Check if the deck contains duplicate
                # Replace duplicated cards in deck
                dedup_slots = self.deck_deduplicate(content['slots'])
                # Make sure the OG deck is in asc order
                deck_slots = self.dict_order_by_keys(content['slots'])
                # Compute md5 hashes
                dedup_hash = hashlib.md5(pickle.dumps(dedup_slots)).hexdigest()
                deck_hash = hashlib.md5(pickle.dumps(deck_slots)).hexdigest()
                # Compare original deck to deduplicated
                if dedup_hash != deck_hash:
                    # Display a message when cards we replaced in a deck
                    # after depulication
                    print('Cards in deck ' + str(deck_id).zfill(5) +
                          ' were replaced by their original card ID.')
                    deck_hash = dedup_hash
                    content['slots'] = dedup_slots
                # Delete variables that won't be used anymore
                del dedup_hash
                del dedup_slots
                # The same deck exists...
                if deck_hash in decks_grouped_by_hash:
                    # Diplay a message with duplicated deck IDs
                    print('Deck ' + str(content['id']) + ' is identical to: ' +
                          str(decks_grouped_by_hash[deck_hash]))
                    # Build data for duplicated decks...
                    # Simple list of deck duplicate of
                    # Group duplicated decks together
                    if decks_grouped_by_hash.get(deck_hash):
                        decks_grouped_by_hash[deck_hash] = \
                            sorted(decks_grouped_by_hash[deck_hash] +
                                   [content['id']])
                    else:
                        decks_grouped_by_hash[deck_hash] = [content['id']]
                else:
                    # @todo verify if the deck is legit
                    # !!! Example: 27554 is illegal!
                    decks_grouped_by_hash[deck_hash] = [content['id']]
                    # Process starter decks...
                    if self.deck_level(content) == 0:
                        self.process_base_deck(content)
                    # Non-starter decks...
                    else:
                        self.process_xp_deck(content)

    def return_file_content(self, filename):
        """Return the file content of a file"""
        with open(filename, encoding="utf-8") as file:
            content = file.read()
        return content

    def value_getter(self, pass_item):
        """Use to get the value of a key/value pair"""
        return pass_item[1]

    def worker_inv_aff(self):
        """Worker for affinities"""
        while not queue_inv_aff.empty():
            inv = queue_inv_aff.get()
            # Make some variables more easily accessible...
            current_aff = affinity_investigators[inv]
            reorg = sorted(current_aff.items(), key=self.value_getter, reverse=True)
            # Get current investigator information
            self.arkhamdb_cache('card', inv)
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
                    txt_output = txt_output + \
                        self.arkhamdb_cache('card', code).get('name') + ' (' \
                        + str(code) + ') [' + str(value) + ', ' + \
                        str(round(value*100/max_value, 1)) + '%]\n'
            print(txt_output)
            self.write_to_file(txt_output, TEXT_PATH + 'inv_aff_' +
                          card_cache[inv]['name'].replace(" ", "_") + '.txt')
            self.write_to_file(html_output, HTML_PATH + 'inv_aff_' +
                          card_cache[inv]['name'].replace(" ", "_") + '.html')

    def worker_inv_aff_xp(self):
        """Worker for affinities"""
        while not queue_inv_aff.empty():
            inv = queue_inv_aff.get()
            # Make some variables more easily accessible...
            current_aff = affinity_investigators_xp[inv]
            reorg = sorted(current_aff.items(), key=self.value_getter, reverse=True)
            # Get current investigator information
            self.arkhamdb_cache('card', inv)
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
                                "<img " + "src=\"https://arkhamdb.com/" \
                                + "bundles/cards/" + str(code) + ".png\" />\n"
                            # Display With the ArkhamDB card ID
                            txt_output = txt_output + \
                                self.arkhamdb_cache('card', code).get('name') \
                                + ' (' + str(code) + ') [' + str(value) + ', ' \
                                + str(round(value*100/max_value, 1)) + '%]\n'
            print(txt_output)
            self.write_to_file(txt_output, TEXT_PATH + 'inv_aff_' +
                          card_cache[inv]['name'].replace(" ", "_") + '_xp.txt')
            self.write_to_file(html_output, HTML_PATH + 'inv_aff_' +
                          card_cache[inv]['name'].replace(" ", "_") + '_xp.html')

    def process_base_deck(self, deck_data):
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
            affinity_cards[slot] = self.dict_order_by_keys(affinity_cards[slot])
        # Processing _after_ all slots were parse
        # Put the investigator value back in the dict...
        affinity_investigators.update(
            {deck_data['investigator_code']: self.dict_order_by_keys(inv_affinity)})

    def process_xp_deck(self, deck_data):
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
            affinity_cards[slot] = self.dict_order_by_keys(affinity_cards[slot])
        # Processing _after_ all slots were parse
        # Put the investigator value back in the dict...
        affinity_investigators_xp.update(
            {deck_data['investigator_code']: self.dict_order_by_keys(inv_affinity)})


if __name__ == "__main__":
    #
    # Code begins here...
    #
    # Start time for statistics only
    start_time = datetime.now()
    print('Arkham Horror Analytics')

    analytics = ArkhamHorrorAnalytics()

    list_of_deck = list(range(1, LAST_DECK))

    # Fill the queue with the deck list
    analytics.fill_queue(list_of_deck)

    #
    # Create threads that will execute workers
    # This worker builds the generic stats
    #
    for t in range(NB_THREAD):
        thread = threading.Thread(target=analytics.worker)
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
        thread_aff = threading.Thread(target=analytics.worker_inv_aff)
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
        thread_aff_xp = threading.Thread(target=analytics.worker_inv_aff_xp)
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

    analytics.json_to_file(analytics.dict_order_by_keys(affinity_investigators),
                 JSON_PATH + 'aff_inv.json')
    analytics.json_to_file(analytics.dict_order_by_keys(affinity_cards),
                 JSON_PATH + 'aff_cards.json')
    analytics.json_to_file(analytics.dict_order_by_keys(decks_grouped_by_hash),
                 JSON_PATH + 'decks_grouped_by_hash.json')

    print('\n\n')
    print('Unique decks :    ' + str(len(decks_grouped_by_hash)))
    print('Duplicated decks: ' + str(len(analytics.valid_decks) -
                                     len(decks_grouped_by_hash)))
    print('Total decks:      ' + str(len(analytics.valid_decks)))

    print(f"\nNumber of thread(s) used: {NB_THREAD}")
    print(f"Runtime {format(datetime.now() - start_time)}.")
