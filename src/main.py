#! /usr/bin/env python3
from telethon import TelegramClient, events, utils, Button
from emoji import emojize
from enum import Enum, auto
from tabulate import tabulate

import settings
import logging
import db
import webscraper
import orjson
import asyncio
import os
import re
import ast
import pprint as pp

# Logger
logger = logging.getLogger('client.main')

try:
    import uvloop
    uvloop_imported = True
except ModuleNotFoundError:
    logger.info("Running without uvloop.")  # uvloop currently only works on Linux, see issue 14 on the GitHub page of uvloop.
    uvloop_imported = False
    pass

#############################################
# Project settings
#############################################

# Directory of the project
project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

#############################################
# Telegram bot setup
#############################################

# Default keyboard markup
mk_home = [
    Button.text(emojize(':plus:') + " Track", resize=True),
    Button.text(emojize(':page_with_curl:') + " List", resize=True),
    Button.text(emojize(':wastebasket:') + " Remove", resize=True),
    Button.text(emojize(':gear:') + " Settings", resize=True)
]

# The amount of items to show before paging occures
list_paging = 4


async def generate_message(user, value):
    data = await Database.read_database(user, "Markets", value)
    settings = await Database.read_database(user, "Settings")

    day = "{day}".format(day=data["Day"])
    if "-" in data["Day"]:
        day = "{} {}".format(emojize(':down_arrow:'), day)  # ⬇️
    elif float(data["Day"].replace(",", ".").replace(" %", "")) > 0.01:
        day = "{} {}".format(emojize(':up_arrow:'), day)  # ⬆️

    ref = "{ref1} {ref2}".format(
        ref1=data["Reference"], ref2=data["Reference_perc"])
    if "-" in data["Reference_perc"]:
        ref = "{} {}".format(emojize(':down_arrow:'), ref)  # ⬇️
    elif "+" in data["Reference_perc"]:
        ref = "{} {}".format(emojize(':up_arrow:'), ref)  # ⬆️

   # Need a more elegant solution. Perhaps with tabulate
   # Currently only from ING
    message = "[{sprinter}](https://www.ingsprinters.nl/markten/indices/{sprinter_url})\n".format(
        sprinter=data["Title"], sprinter_url=data["Title"].replace(" ", "-"))

    if settings["Isin"]:
        message += "**isin**               [{Isin}](https://www.ingsprinters.nl/zoeken?q={Isin})\n".format(
            Isin=data["Isin"])
    if settings["Bid"]:
        message += "**Bid**               __{Bid}__\n".format(Bid=data["Bid"])
    if settings["Ask"]:
        message += "**Ask**               __{Ask}__\n".format(Ask=data["Ask"])
    if settings["Day"]:
        message += "**%1 Day**       __{Day}__\n".format(Day=day)
    if settings["Lever"]:
        message += "**Lever**            __{Lever}__\n".format(
            Lever=data["Lever"])
    if settings["StopLoss"]:
        message += "**Stop loss**     __{Stoploss}__\n".format(
            Stoploss=data["Stoploss"])
    if settings["Reference"]:
        message += "**Reference**  __{Reference}__\n\n".format(Reference=ref)

    return message


def create_paged_buttons(offset, list_length, cb):
    mk = []
    if offset <= list_length and offset > 1:
        mk.append(Button.inline("Previous", "%d_%s" % ((offset - 1), cb)))
    if offset < list_length and offset >= 1:
        mk.append(Button.inline("Next", "%d_%s" % ((offset + 1), cb)))
    return mk


async def main():
    with open(project_dir + '/credentials.json') as f:
        data = orjson.loads(f.read())
        API_ID = data["API_ID"]
        API_HASH = data["API_HASH"]
        TOKEN = data["TOKEN"]
        NAME = data["NAME"]

    await Database._init()
    await Database.create_database()

    client = TelegramClient(NAME, API_ID, API_HASH)

    # Handlers
    client.add_event_handler(start)
    client.add_event_handler(stop)
    client.add_event_handler(welcome_back)
    client.add_event_handler(track)
    client.add_event_handler(current_list)
    client.add_event_handler(remove)
    client.add_event_handler(user_settings)
    client.add_event_handler(callback_confirm)
    client.add_event_handler(callback_cancel)
    client.add_event_handler(callback_close)
    client.add_event_handler(callback_remove)
    client.add_event_handler(callback_current_list)
    client.add_event_handler(callback_settings)

    # remove once complete
    client.add_event_handler(database)
    client.parse_mode = 'md'

    # Make a try except with ConnectionError
    await client.start(bot_token=TOKEN)
    # await client.catch_up()  # Broken
    try:
        await client.run_until_disconnected()
    finally:
        await Database._close()

#############################################
# Telegram bot functions
#############################################


@events.register(events.NewMessage(pattern=r'(?i).*\b(start)\b', incoming=True))
async def start(event):
    sender = await event.get_sender()
    name = utils.get_display_name(sender)
    user = utils.get_input_user(sender)
    message = "Hi %s,\nI will update you on the stock exchange market with data from [ING Sprinters](https://www.ingsprinters.nl/)!\nAdd your first sprinter by tapping the 'Track' button on your keyboard." % (
        name)

    await Database.new_user(user)

    markup = event.client.build_reply_markup(mk_home)

    await event.reply(message, buttons=markup)


@events.register(events.NewMessage(pattern=r'(?i).*\b(stop)\b', incoming=True))
async def stop(event):
    sender = await event.get_sender()
    name = utils.get_display_name(sender)
    user = utils.get_input_user(sender)
    message = "Hi %s,\nI removed you from the database!" % (name)

    await Database.delete_user(user)

    await event.reply(message)


#############################################
# Callback events
#############################################

@events.register(events.CallbackQuery(pattern=r'(?i)(Cancel)'))
async def callback_cancel(event):
    sender = await event.get_sender()
    user = utils.get_input_user(sender)
    callback_data = event.data.decode("utf-8")
    regex_data = re.findall(r"(?i)(Cancel)|_([a-z]+)", callback_data)

    message = "Action cancelled."

    cb_type = ''
    try:
        cb_type = regex_data[1][1]
    except IndexError:
        pass

    if cb_type == "del":
        isin_dict = await Database.read_database(user, "client_markets")

        for key, val in isin_dict.items():
            if not val:
                continue
            await Database.update_database(user, "client_markets", {key: 0})

    elif cb_type == "conv":
        await event.client.conversation(user.user_id).cancel_all()

    await event.edit(message)


@events.register(events.CallbackQuery(pattern=r'(?i).*\b(Close)\b'))
async def callback_close(event):
    sender = await event.get_sender()
    user = utils.get_input_user(sender)
    mk = [[Button.inline("Open", "1_Settings")]]

    message = "Closed the settings page."

    markup = event.client.build_reply_markup(mk)
    await event.edit(message, buttons=markup)


@events.register(events.CallbackQuery(pattern=r'(?i)(Confirm)'))
async def callback_confirm(event):
    sender = await event.get_sender()
    user = utils.get_input_user(sender)
    callback_data = event.data.decode("utf-8")
    regex_data = re.findall(r"(?i)(Confirm)|_([a-z]+)", callback_data)

    message = "Action confirmed.\n"

    # Try deleting marked ISINs
    try:
        if regex_data[1][1] == "del":
            isin_dict = await Database.read_database(user, "client_markets")
            message += "Deleted:\n"

            for key, val in isin_dict.items():
                if val:
                    # Currently only from ING
                    message += "[{sprinter}](https://www.ingsprinters.nl/markten/indices/{sprinter_url})\n".format(
                            sprinter=key, sprinter_url=key.replace(" ", "-"))
                    payload = {"Isin": key}
                    await Database.delete_from_database(user, "client_markets", payload)
    except IndexError:
        pass

    await event.edit(message, link_preview=False)


@events.register(events.CallbackQuery(pattern=r"(?i)([0-9]+)(_Remove)"))
async def callback_remove(event):
    sender = await event.get_sender()
    user = utils.get_input_user(sender)
    callback_data = event.data.decode("utf-8")
    regex_data = re.findall(r"(?i)([0-9]+)(_Remove)|((nl)[0-9, A-Z]{10})", callback_data)
    offset = int(regex_data[0][0])
    remove_paging = (list_paging * 2)

    # Read database
    isin_dict = await Database.read_database(user, "client_markets")
    isin_list = list(isin_dict.keys())
    paged_list = list(webscraper.chunks(isin_list, remove_paging))


    # Create buttons
    mk = [ [
        Button.inline(emojize(":cross_mark:") + " Cancel", b'Cancel_del'),
        Button.inline(emojize(":check_mark:") + " Confirm", b'Confirm_del')
    ] ]
    mk.insert(0, create_paged_buttons(offset, len(paged_list), "Remove"))

    # Try marking the deletion of an ISIN
    try:
        isin = regex_data[1][2]
        isin_dict[isin] = int(not bool(isin_dict[isin]))
        await Database.update_database(user, "client_markets", {isin: isin_dict[isin]})
    except IndexError:
        pass

    title_list = []
    # Could eventually optimize this by including the title in the return of client_markets read
    async def iterations(index):
        data = await Database.read_database(user, "Markets", isin_list[index])
        title_list.append(data["Title"])
        return

    coros = [iterations(index) for index in range(len(isin_list))]
    await asyncio.gather(*coros)

    # Create buttons of all ISINs
    for index, value in enumerate(paged_list[offset - 1]):
        if isin_dict[value]:
            item = "{} {} {}".format(emojize(":cross_mark:"), title_list[index], value)
        else:
            item = "{} {}".format( title_list[index], value)
        mk.insert(0, [Button.inline(item, "{}_Remove_{}".format(offset, value))])

    pages = int((len(isin_list)/remove_paging) +
                (len(isin_list) % remove_paging > 0))
    message = "📦 I'm ready. Tap on the sprinters that you would like to delete.\nPage %d of %d" % (
        offset, pages)

    markup = event.client.build_reply_markup(mk)
    await event.edit(message, buttons=markup, link_preview=False)


@events.register(events.CallbackQuery(pattern=r"(?i)([0-9]+)(_List)"))
async def callback_current_list(event):
    sender = await event.get_sender()
    user = utils.get_input_user(sender)
    callback_data = event.data.decode("utf-8")
    offset = int(re.search(r"(?i)([0-9]+)(_List)", callback_data).group(1))
    mk = []

    isin_dict = await Database.read_database(user, "client_markets")
    isin_list = list(isin_dict.keys())
    paged_list = list(webscraper.chunks(isin_list, list_paging))
    results = []

    coros = [generate_message(user, value) for value in paged_list[offset-1]]
    messages = await asyncio.gather(*coros)
    message = ''.join(map(str, messages))

    # Create buttons
    mk.append(create_paged_buttons(offset, len(paged_list), "List"))

    markup = event.client.build_reply_markup(mk)
    await event.edit(message, buttons=markup, link_preview=False)


@events.register(events.CallbackQuery(pattern=r"(?i)([0-9]+)(_Settings)"))
async def callback_settings(event):
    sender = await event.get_sender()
    user = utils.get_input_user(sender)
    callback_data = event.data.decode("utf-8")
    regex_data = re.findall(
        r"(?i)([0-9]+)(_Settings)|_([a-z]+)_([0-9])", callback_data)
    offset = int(regex_data[0][0])
    message = "Tap on a button to toggle the setting."
    mk = [[Button.inline("Close", "Close")]]

    try:
        payload = {regex_data[1][2]: regex_data[1][3]}
        await Database.update_database(user, "Settings", payload)
    except IndexError:
        pass

    data = await Database.read_database(user, "Settings")

    for key, value in data.items():
        if key == "user_id":
            continue
        if int(value) == 1:
            enabled = emojize(':check_mark_button:')
            val = 0
        else:
            enabled = emojize(':cross_mark_button:')
            val = 1

        item = "{} {}".format(key, enabled)
        mk.insert(0, [Button.inline(item, "1_Settings_%s_%d" % (key, val))])

    markup = event.client.build_reply_markup(mk)
    await event.edit(message, buttons=markup, link_preview=False)


#############################################
# Main keyboard functions
#############################################
# Case insensitive matching with all other patterns
@events.register(events.NewMessage(pattern=r'(^((?i)(?!Cancel|Close|Track|List|Remove|Settings|database|db|Confirm|NL).)*$)', incoming=True))
async def welcome_back(event):
    sender = await event.get_sender()
    user = utils.get_input_user(sender)
    message = "Welcome back!"
    markup = event.client.build_reply_markup(mk_home)

    await event.client.send_message(user.user_id, message, buttons=markup)

@events.register(events.NewMessage(pattern=r'(?i).*\b(Track)\b', incoming=True))
async def track(event):
    sender = await event.get_sender()
    user = utils.get_input_user(sender)
    message = "📦 I'm ready. Tell me the sprinter's isin."
    markup = event.client.build_reply_markup(mk_home)

    async with event.client.conversation(user.user_id) as conv:
        cancelButton = Button.inline(
            emojize(":cross_mark:") + " Cancel", b'Cancel_conv')
        msg = await conv.send_message(message, buttons=cancelButton)

        response = await conv.get_response(timeout=120)

        # Remove the button
        await event.client.edit_message(msg, message)

        # Check if isin is valid
        valid = await webscraper.isValidIsin(response.text)
        try:
            isin = re.search(
                r"(?i)((nl)[0-9, A-Z]{10})", response.text).group(0)
        except AttributeError:
            isin = response.text

        results, unavailable = await webscraper.getSprinterDataHTML([isin])

        if valid:
            await Database.insert_to_database(user, "Markets", results[0])
            await Database.insert_to_database(user, "client_markets", results[0])
            message = "Sprinter added!"
        else:
            message = "Invalid isin."

        await event.client.send_message(user.user_id, message, buttons=markup)


@events.register(events.NewMessage(pattern=r'(?i).*\b(List)\b', incoming=True))
async def current_list(event):
    sender = await event.get_sender()
    user = utils.get_input_user(sender)
    mk = None  # Initialize markup
    async with event.client.action(user.user_id, 'typing'):

        isin_dict = await Database.read_database(user, "client_markets")
        isin_list = list(isin_dict.keys())

        if isin_list:
            available_sprinters, unavailable_sprinters = await webscraper.getSprinterDataHTML(isin_list)
            # Only update the data from the first four sprinters
            await Database.update_database(user, "Markets", [available_sprinters[:list_paging], unavailable_sprinters])

            if len(isin_list) > list_paging:
                mk = Button.inline("Next", "2_List")  # Keyboard
            else:
                mk = mk_home
            coros = [generate_message(user, value)
                    for value in isin_list[:list_paging]]
            messages = await asyncio.gather(*coros)
            message = ''.join(map(str, messages))
        else:
            message = "Your list is empty."
            mk = mk_home

        markup = event.client.build_reply_markup(mk)
        await event.client.send_message(user.user_id, message, buttons=markup, link_preview=False)
    # Update the rest of the sprinters
    await Database.update_database(user, "Markets", [available_sprinters[list_paging:]])


@events.register(events.NewMessage(pattern=r'(?i).*\b(Remove)\b', incoming=True))
async def remove(event):
    sender = await event.get_sender()
    user = utils.get_input_user(sender)
    mk = [ [
        Button.inline(emojize(":cross_mark:") + " Cancel", b'Cancel_del'),
        Button.inline(emojize(":check_mark:") + " Confirm", b'Confirm_del')
    ] ]

    isin_dict = await Database.read_database(user, "client_markets")
    isin_list = list(isin_dict.keys())
    title_list = []

    async def iterations(index):
        data = await Database.read_database(user, "Markets", isin_list[index])
        title_list.append(data["Title"])
        return

    coros = [iterations(index) for index in range(len(isin_list))]
    await asyncio.gather(*coros)

    remove_paging = (list_paging * 2)

    if len(isin_list) > remove_paging:
        mk.insert(0, [Button.inline("Next", "2_Remove")])  # Keyboard

    for index, value in enumerate(isin_list[:remove_paging]):
        if isin_dict[value]:
            item = "{} {} {}".format(emojize(":cross_mark:"), title_list[index], value)
        else:
            item = "{} {}".format( title_list[index], value)
        mk.insert(0, [Button.inline(item, "1_Remove_{}".format(value))])

    pages = int((len(isin_list)/remove_paging) +
                (len(isin_list) % remove_paging > 0))
    message = "📦 I'm ready. Tap on the sprinters that you would like to delete.\nPage 1 of %d" % pages
    markup = event.client.build_reply_markup(mk)
    await event.client.send_message(user.user_id, message, buttons=markup)


@events.register(events.NewMessage(pattern=r'(?i).*\b(Settings)\b', incoming=True))
async def user_settings(event):
    sender = await event.get_sender()
    user = utils.get_input_user(sender)
    mk = [[Button.inline("Close", "Close")]]
    message = "Tap on a button to toggle the setting."

    data = await Database.read_database(user, "Settings")

    for key, value in data.items():
        if key == "user_id":
            continue

        enabled = emojize(':check_mark_button:') if int(
            value) == 1 else emojize(':cross_mark_button:')

        item = "{} {}".format(key, enabled)
        mk.insert(0, [Button.inline(item, "1_Settings_%s_%d" %
                                    (key, int(not bool(value))))])

    markup = event.client.build_reply_markup(mk)
    await event.client.send_message(user.user_id, message, buttons=markup)


@events.register(events.NewMessage(pattern=r'(?i).*\b(database|db)\b', incoming=True))
async def database(event):
    # For debugging purposes only
    await Database.print_database()
    await event.reply("Check the console output")

if __name__ == '__main__':
    Database = db.Database(project_dir)

    if uvloop_imported:
        uvloop.install()

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
