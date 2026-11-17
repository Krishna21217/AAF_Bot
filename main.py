import os
import json
import sqlite3
import asyncio
from aiohttp import web

# Python 3.14 Event Loop Fix
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

from telethon import TelegramClient, events, Button
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError

# Server Environment Variables
API_ID = int(os.environ.get("API_ID", "0").strip())
API_HASH = os.environ.get("API_HASH", "").strip()
SESSION = os.environ.get("SESSION", "").strip()
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()

user_client = TelegramClient(StringSession(SESSION), API_ID, API_HASH, loop=loop)
bot_client = TelegramClient('bot_session', API_ID, API_HASH, loop=loop)

CONFIG_FILE = "tasks_config.json"
DB_FILE = "msg_map.db"
BACKUP_TAG = "#AUTO_CONFIG_DB"

# ---------------- SQLITE DATABASE SETUP (For Live Edit Sync) ----------------
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS msg_map (
    src_chat INTEGER,
    src_msg INTEGER,
    dest_chat INTEGER,
    dest_msg INTEGER
)
""")
conn.commit()

def save_mapping(src_chat, src_msg, dest_chat, dest_msg):
    cursor.execute("DELETE FROM msg_map WHERE src_chat=? AND src_msg=? AND dest_chat=?", (src_chat, src_msg, dest_chat))
    cursor.execute("INSERT INTO msg_map VALUES (?, ?, ?, ?)", (src_chat, src_msg, dest_chat, dest_msg))
    cursor.execute("DELETE FROM msg_map WHERE rowid IN (SELECT rowid FROM msg_map ORDER BY rowid DESC LIMIT -1 OFFSET 3000)")
    conn.commit()

def get_mappings(src_chat, src_msg):
    cursor.execute("SELECT dest_chat, dest_msg FROM msg_map WHERE src_chat=? AND src_msg=?", (src_chat, src_msg))
    return cursor.fetchall()

# ---------------- TASK CONFIG & 100% AUTO-RESTORE SYSTEM ----------------
data = {"tasks": {}, "owner_id": None}
OWNER_ID = None
USER_CONNECTED = False
cached_dialogs = []
msg_queue = None

def save_local():
    with open(CONFIG_FILE, "w") as f:
        json.dump(data, f, indent=4)

async def sync_to_cloud():
    save_local()
    if not USER_CONNECTED:
        return
    try:
        backup_str = f"{BACKUP_TAG}\n`{json.dumps(data)}`"
        async for msg in user_client.iter_messages("me", search=BACKUP_TAG, limit=5):
            if msg.raw_text.startswith(BACKUP_TAG):
                await msg.edit(backup_str)
                return
        await user_client.send_message("me", backup_str)
    except Exception as e:
        print(f"Cloud Sync Error: {e}")

async def auto_restore_from_cloud():
    global data, OWNER_ID
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r") as f:
                local_data = json.load(f)
                if local_data.get("tasks"):
                    data = local_data
                    if data.get("owner_id"):
                        OWNER_ID = data["owner_id"]
                    return
        except:
            pass

    if not USER_CONNECTED:
        return

    try:
        async for msg in user_client.iter_messages("me", search=BACKUP_TAG, limit=5):
            if msg.raw_text and "`" in msg.raw_text:
                json_part = msg.raw_text.split("`")[1]
                data = json.loads(json_part)
                if data.get("owner_id"):
                    OWNER_ID = data["owner_id"]
                save_local()
                print("✅ 100% Data Restored from Saved Messages!")
                return
    except Exception as e:
        print(f"Auto-Restore Error: {e}")

# ---------------- EARNKARO SMART BRIDGE CONVERTER ----------------
async def send_with_retry(func, *args, **kwargs):
    while True:
        try:
            return await func(*args, **kwargs)
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds + 5)
        except Exception as ex:
            print(f"Send Error: {ex}")
            return None

async def convert_via_bot(conv_id, message, is_edit=False):
    """EarnKaro se convert karayega aur 'We could not locate an affiliate URL' aane par rok dega"""
    if not conv_id:
        return message.text, message.media, True
        
    raw_msg = message.raw_text or ""
    # Agar message me link hi nahi hai (jaise 'Deal Over' edit me), to EarnKaro ko bhejne ki zaroorat nahi
    if "http://" not in raw_msg and "https://" not in raw_msg:
        if is_edit:
            return message.text, message.media, True
        else:
            return None, None, False

    try:
        async with user_client.conversation(int(conv_id), timeout=12) as conv:
            await conv.send_message(message)
            resp = await conv.get_response()
            
            lower_resp = (resp.raw_text or "").lower()
            
            # Exact EarnKaro Error Filter
            blocked_phrases = [
                "we could not locate an affiliate url",
                "verify if the seller is available",
                "could not locate",
                "no link",
                "not convert",
                "invalid",
                "sorry"
            ]
            
            # Agar EarnKaro ne ye error message bheja hai YA reply me koi link nahi hai
            if any(phrase in lower_resp for phrase in blocked_phrases) or ("http://" not in lower_resp and "https://" not in lower_resp):
                if is_edit:
                    # Agar purana message edit hokar 'Deal Over' hua hai, to original edited text से update kar do
                    return message.text, message.media, True
                else:
                    # Naya message hai aur link convert nahi hua, to bilkul forward MAT karo!
                    print("🚫 Blocked: EarnKaro could not locate an affiliate URL.")
                    return None, None, False
                
            final_media = resp.media if resp.media else message.media
            return resp.text, final_media, True
            
    except Exception as e:
        print(f"Converter Timeout/Skip: {e}")
        if is_edit:
            return message.text, message.media, True
        return None, None, False

# ---------------- QUEUE WORKER (NEW, ALBUM & EDIT) ----------------
async def queue_worker():
    while True:
        item_type, event, task_list = await msg_queue.get()
        try:
            if item_type == "single":
                for t_info in task_list:
                    conv_id = t_info.get("converter")
                    conv_text, conv_media, should_send = await convert_via_bot(conv_id, event.message, is_edit=False)
                    
                    if not should_send:
                        continue # Error message ko skip kar diya!
                    
                    for dest in t_info.get("destinations", []):
                        if conv_id:
                            sent = await send_with_retry(user_client.send_message, int(dest), conv_text, file=conv_media)
                        else:
                            sent = await send_with_retry(user_client.send_message, int(dest), event.message)
                        if sent:
                            save_mapping(event.chat_id, event.message.id, int(dest), sent.id)
                        await asyncio.sleep(2.5)

            elif item_type == "album":
                for t_info in task_list:
                    conv_id = t_info.get("converter")
                    cap_msg = next((m for m in event.messages if m.text), event.messages[0])
                    conv_text, _, should_send = await convert_via_bot(conv_id, cap_msg, is_edit=False)
                    
                    if not should_send:
                        continue
                    
                    for dest in t_info.get("destinations", []):
                        sent_list = await send_with_retry(user_client.send_file, int(dest), event.messages, caption=conv_text)
                        if sent_list:
                            first_sent = sent_list[0] if isinstance(sent_list, list) else sent_list
                            save_mapping(event.chat_id, event.messages[0].id, int(dest), first_sent.id)
                        await asyncio.sleep(3.0)

            elif item_type == "edit":
                targets = get_mappings(event.chat_id, event.message.id)
                if targets:
                    conv_id = None
                    for t_info in data["tasks"].values():
                        if t_info.get("active") and event.chat_id in t_info.get("sources", []) and t_info.get("converter"):
                            conv_id = t_info.get("converter")
                            break
                    
                    conv_text, conv_media, should_send = await convert_via_bot(conv_id, event.message, is_edit=True)
                    
                    if should_send:
                        for dest_chat, dest_msg in targets:
                            await send_with_retry(
                                user_client.edit_message,
                                int(dest_chat),
                                int(dest_msg),
                                text=conv_text,
                                file=conv_media
                            )
                            await asyncio.sleep(2.0)

        except Exception as e:
            print(f"Worker Error: {e}")
        finally:
            msg_queue.task_done()

def get_matching_tasks(chat_id):
    matched = []
    for t_id, t_info in data["tasks"].items():
        if t_info.get("active", False) and chat_id in t_info.get("sources", []):
            matched.append(t_info)
    return matched

# ---------------- EVENT HANDLERS ----------------
@user_client.on(events.NewMessage())
async def handle_new_message(event):
    if event.grouped_id:
        return
    matched_tasks = get_matching_tasks(event.chat_id)
    if matched_tasks and msg_queue:
        await msg_queue.put(("single", event, matched_tasks))

@user_client.on(events.Album())
async def handle_album(event):
    matched_tasks = get_matching_tasks(event.chat_id)
    if matched_tasks and msg_queue:
        await msg_queue.put(("album", event, matched_tasks))

@user_client.on(events.MessageEdited())
async def handle_edit_message(event):
    targets = get_mappings(event.chat_id, event.message.id)
    if targets and msg_queue:
        await msg_queue.put(("edit", event, []))

# ---------------- UI HELPER FUNCTIONS ----------------
async def refresh_top_20_chats():
    global cached_dialogs
    cached_dialogs = []
    if not USER_CONNECTED:
        return
    async for dialog in user_client.iter_dialogs(limit=20):
        name = dialog.name[:24] if dialog.name else "Unknown"
        cached_dialogs.append({"id": dialog.id, "name": name})

def get_main_menu():
    conn_status = "✅ Connected" if USER_CONNECTED else "❌ SESSION ERROR"
    text = (f"🤖 **PRO AUTO-FORWARDER CONTROL PANEL**\n"
            f"🔗 **User Account:** {conn_status}\n\n"
            f"Apne Tasks yahan se manage karein (🟢 = Chalu, 🔴 = Band):")
    buttons = []
    for t_id, t_info in data["tasks"].items():
        status = "🟢 ON" if t_info.get("active") else "🔴 OFF"
        src_cnt = len(t_info.get("sources", []))
        dst_cnt = len(t_info.get("destinations", []))
        conv_mark = "🔄" if t_info.get("converter") else "➡️"
        btn_text = f"{t_info['name']} [{status}] ({src_cnt} {conv_mark} {dst_cnt})"
        buttons.append([Button.inline(btn_text, f"view_{t_id}".encode())])
    
    buttons.append([Button.inline("➕ Create New Task", b"new_task")])
    return text, buttons

def get_task_menu(t_id):
    t_info = data["tasks"][t_id]
    status = "🟢 CHALU HAI (Running)" if t_info.get("active") else "🔴 BAND HAI (Stopped)"
    
    src_names = [f"`{x}`" for x in t_info.get("sources", [])]
    dst_names = [f"`{x}`" for x in t_info.get("destinations", [])]
    conv_id = t_info.get("converter")
    conv_name = f"`{conv_id}`" if conv_id else "Direct (Bina Converter Ke)"

    for d in cached_dialogs:
        for i, s_id in enumerate(t_info.get("sources", [])):
            if d["id"] == s_id: src_names[i] = f"{d['name']}"
        for i, d_id in enumerate(t_info.get("destinations", [])):
            if d["id"] == d_id: dst_names[i] = f"{d['name']}"
        if conv_id and d["id"] == conv_id:
            conv_name = f"🤖 {d['name']}"

    text = (f"⚙️ **Task Manage: {t_info['name']}**\n\n"
            f"📊 **Status:** {status}\n\n"
            f"📥 **Sources (Jahan se deal aayegi):**\n" + (", ".join(src_names) if src_names else "Koi select nahi hai") + "\n\n"
            f"🔄 **Converter Bot (EarnKaro):**\n{conv_name}\n\n"
            f"📤 **Destinations (Apna Main Channel):**\n" + (", ".join(dst_names) if dst_names else "Koi select nahi hai"))

    toggle_text = "🔴 Band Karein (Stop)" if t_info.get("active") else "🟢 Chalu Karein (Start)"
    buttons = [
        [Button.inline(toggle_text, f"toggle_{t_id}".encode())],
        [Button.inline("📥 Edit Sources (Top 20)", f"sel_src_{t_id}".encode())],
        [Button.inline("🔄 Select Converter Bot (Top 20)", f"sel_conv_{t_id}".encode())],
        [Button.inline("📤 Edit Destinations (Top 20)", f"sel_dst_{t_id}".encode())],
        [Button.inline("🗑️ Delete Task", f"del_{t_id}".encode()),
         Button.inline("🔙 Back to Main Menu", b"main_menu")]
    ]
    return text, buttons

def get_selection_menu(t_id, mode):
    t_info = data["tasks"][t_id]
    if mode == "src":
        selected_list = t_info.get("sources", [])
        title = "📥 **SOURCES SELECT KAREIN (Jahan se deal leni hai)**"
    elif mode == "dst":
        selected_list = t_info.get("destinations", [])
        title = "📤 **DESTINATIONS SELECT KAREIN (Apna Main Channel)**"
    else:
        conv_val = t_info.get("converter")
        selected_list = [conv_val] if conv_val else []
        title = "🔄 **CONVERTER BOT SELECT KAREIN (Sirf EarnKaro Bot chunein)**"
    
    lines = [title, "\n📋 **Aapke Telegram ke Top 20 Chats:**"]
    for idx, d in enumerate(cached_dialogs, 1):
        mark = "✅" if d["id"] in selected_list else "☑️"
        lines.append(f"**{idx}.** {mark} {d['name']}")
    
    lines.append("\n👇 *Neeche Number dabakar select/unselect karein, fir Done dabayein:*")
    text = "\n".join(lines)

    buttons = []
    row = []
    for idx, d in enumerate(cached_dialogs, 1):
        mark = "✅" if d["id"] in selected_list else ""
        row.append(Button.inline(f"{idx}{mark}", f"togchat_{mode}_{t_id}_{idx-1}".encode()))
        if len(row) == 5:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    
    buttons.append([Button.inline("🔄 Refresh Top 20 List", f"ref_{mode}_{t_id}".encode())])
    buttons.append([Button.inline("✅ DONE (Save & Back)", f"done_{t_id}".encode())])
    return text, buttons

# ---------------- BOT INTERACTION HANDLERS ----------------
@bot_client.on(events.NewMessage(pattern="/start"))
async def start_cmd(event):
    global OWNER_ID
    if OWNER_ID is None:
        OWNER_ID = event.sender_id
        data["owner_id"] = OWNER_ID
        await sync_to_cloud()
        
    if event.sender_id != OWNER_ID:
        return await event.reply("⛔ Ye Private Bot hai! Sirf iska Owner ise chala sakta hai.")
    text, buttons = get_main_menu()
    await event.reply(text, buttons=buttons)

@bot_client.on(events.CallbackQuery())
async def callback_handler(event):
    global OWNER_ID
    if OWNER_ID is None:
        OWNER_ID = event.sender_id
    if event.sender_id != OWNER_ID:
        return await event.answer("Unauthorized!", alert=True)
    
    cmd = event.data.decode("utf-8")

    if cmd == "main_menu":
        text, buttons = get_main_menu()
        await event.edit(text, buttons=buttons)

    elif cmd == "new_task":
        t_id = str(len(data["tasks"]) + 1)
        while t_id in data["tasks"]:
            t_id = str(int(t_id) + 1)
        data["tasks"][t_id] = {
            "name": f"Task #{t_id}",
            "active": False,
            "sources": [],
            "converter": None,
            "destinations": []
        }
        await sync_to_cloud()
        await refresh_top_20_chats()
        text, buttons = get_task_menu(t_id)
        await event.edit(text, buttons=buttons)

    elif cmd.startswith("view_"):
        t_id = cmd.split("_")[1]
        if not cached_dialogs:
            await refresh_top_20_chats()
        text, buttons = get_task_menu(t_id)
        await event.edit(text, buttons=buttons)

    elif cmd.startswith("done_"):
        t_id = cmd.split("_")[1]
        await sync_to_cloud()
        text, buttons = get_task_menu(t_id)
        await event.edit(text, buttons=buttons)

    elif cmd.startswith("toggle_"):
        t_id = cmd.split("_")[1]
        curr = data["tasks"][t_id].get("active", False)
        data["tasks"][t_id]["active"] = not curr
        await sync_to_cloud()
        status_msg = "Task Chalu Ho Gaya! 🟢" if not curr else "Task Band Kar Diya Gaya! 🔴"
        await event.answer(status_msg, alert=True)
        text, buttons = get_task_menu(t_id)
        await event.edit(text, buttons=buttons)

    elif cmd.startswith("del_"):
        t_id = cmd.split("_")[1]
        if t_id in data["tasks"]:
            del data["tasks"][t_id]
            await sync_to_cloud()
        await event.answer("🗑️ Task Deleted!", alert=True)
        text, buttons = get_main_menu()
        await event.edit(text, buttons=buttons)

    elif cmd.startswith("sel_"):
        _, mode, t_id = cmd.split("_")
        await refresh_top_20_chats()
        text, buttons = get_selection_menu(t_id, mode)
        await event.edit(text, buttons=buttons)

    elif cmd.startswith("ref_"):
        _, mode, t_id = cmd.split("_")
        await refresh_top_20_chats()
        await event.answer("Top 20 Chats Refreshed! 🔄")
        text, buttons = get_selection_menu(t_id, mode)
        await event.edit(text, buttons=buttons)

    elif cmd.startswith("togchat_"):
        _, mode, t_id, idx_str = cmd.split("_")
        idx = int(idx_str)
        if idx < len(cached_dialogs):
            chat_id = cached_dialogs[idx]["id"]
            if mode == "conv":
                if data["tasks"][t_id].get("converter") == chat_id:
                    data["tasks"][t_id]["converter"] = None
                else:
                    data["tasks"][t_id]["converter"] = chat_id
            else:
                target_list = data["tasks"][t_id]["sources"] if mode == "src" else data["tasks"][t_id]["destinations"]
                if chat_id in target_list:
                    target_list.remove(chat_id)
                else:
                    target_list.append(chat_id)
            save_local()
        text, buttons = get_selection_menu(t_id, mode)
        await event.edit(text, buttons=buttons)

# ---------------- 24/7 WEB SERVER ----------------
async def web_handle(request):
    return web.Response(text="Pro Auto-Forwarder Bot is Running 24/7!")

async def main():
    global OWNER_ID, USER_CONNECTED, msg_queue
    msg_queue = asyncio.Queue()
    
    try:
        await user_client.connect()
        if await user_client.is_user_authorized():
            USER_CONNECTED = True
            me = await user_client.get_me()
            OWNER_ID = me.id
            print(f"✅ User Connected! Owner ID: {OWNER_ID}")
            await auto_restore_from_cloud()
        else:
            print("❌ WARNING: SESSION String is invalid!")
    except Exception as e:
        print(f"❌ Session Connect Error: {e}")
    
    await bot_client.start(bot_token=BOT_TOKEN)
    asyncio.create_task(queue_worker())
    
    app = web.Application()
    app.router.add_get('/', web_handle)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    
    print("Smart Bridge Production Bot is Live!")
    await bot_client.run_until_disconnected()

if __name__ == '__main__':
    loop.run_until_complete(main())
