import os
import json
import sqlite3
import asyncio
from aiohttp import web
from telethon import TelegramClient, events, Button
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError

# Server Environment Variables
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION = os.environ.get("SESSION", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)


user_client = TelegramClient(StringSession(SESSION), API_ID, API_HASH)
bot_client = TelegramClient('bot_session', API_ID, API_HASH)

CONFIG_FILE = "tasks_config.json"
DB_FILE = "msg_map.db"
BACKUP_TAG = "#AUTO_CONFIG_DB"

# ---------------- SQLITE DATABASE SETUP (For Live Edit Sync Only) ----------------
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
    cursor.execute("INSERT INTO msg_map VALUES (?, ?, ?, ?)", (src_chat, src_msg, dest_chat, dest_msg))
    cursor.execute("DELETE FROM msg_map WHERE rowid IN (SELECT rowid FROM msg_map ORDER BY rowid DESC LIMIT -1 OFFSET 3000)")
    conn.commit()

def get_mappings(src_chat, src_msg):
    cursor.execute("SELECT dest_chat, dest_msg FROM msg_map WHERE src_chat=? AND src_msg=?", (src_chat, src_msg))
    return cursor.fetchall()

# ---------------- TASK CONFIG & 100% AUTO-RESTORE SYSTEM ----------------
data = {"tasks": {}}
OWNER_ID = None
cached_dialogs = []
msg_queue = asyncio.Queue()

def save_local():
    with open(CONFIG_FILE, "w") as f:
        json.dump(data, f, indent=4)

async def sync_to_cloud():
    """Jab bhi koi change hoga, Saved Messages me backup apne aap update ho jayega"""
    save_local()
    try:
        backup_str = f"{BACKUP_TAG}\n`{json.dumps(data)}`"
        # Pehle check karo ki purana backup message hai ya nahi
        async for msg in user_client.iter_messages("me", search=BACKUP_TAG, limit=5):
            if msg.raw_text.startswith(BACKUP_TAG):
                await msg.edit(backup_str)
                return
        # Agar nahi hai to naya bhej do
        await user_client.send_message("me", backup_str)
    except Exception as e:
        print(f"Cloud Sync Error: {e}")

async def auto_restore_from_cloud():
    """Server start hote hi apne aap Saved Messages se sara data wapas le aayega"""
    global data
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r") as f:
                local_data = json.load(f)
                if local_data.get("tasks"):
                    data = local_data
                    return
        except:
            pass

    print("Checking Saved Messages for Auto-Restore...")
    try:
        async for msg in user_client.iter_messages("me", search=BACKUP_TAG, limit=5):
            if msg.raw_text and "`" in msg.raw_text:
                json_part = msg.raw_text.split("`")[1]
                data = json.loads(json_part)
                save_local()
                print("✅ 100% Data Automatically Restored from Saved Messages!")
                return
    except Exception as e:
        print(f"Auto-Restore Check Error: {e}")

# ---------------- ANTI-BAN QUEUE WORKER WITH FLOOD-RETRY ----------------
async def send_with_retry(func, *args, **kwargs):
    while True:
        try:
            return await func(*args, **kwargs)
        except FloodWaitError as e:
            print(f"FloodWait: Waiting {e.seconds}s before retrying...")
            await asyncio.sleep(e.seconds + 5)
        except Exception as ex:
            print(f"Send Error: {ex}")
            return None

async def queue_worker():
    while True:
        item_type, event, dest_list = await msg_queue.get()
        try:
            if item_type == "single":
                for dest in dest_list:
                    sent = await send_with_retry(user_client.send_message, int(dest), event.message)
                    if sent:
                        save_mapping(event.chat_id, event.message.id, int(dest), sent.id)
                    await asyncio.sleep(2.5) # Anti-Ban Delay

            elif item_type == "album":
                for dest in dest_list:
                    caption = [m.text for m in event.messages if m.text]
                    cap_text = caption[0] if caption else ""
                    sent_list = await send_with_retry(user_client.send_file, int(dest), event.messages, caption=cap_text)
                    if sent_list:
                        first_sent = sent_list[0] if isinstance(sent_list, list) else sent_list
                        save_mapping(event.chat_id, event.messages[0].id, int(dest), first_sent.id)
                    await asyncio.sleep(3.0)
        except Exception as e:
            print(f"Worker Error: {e}")
        finally:
            msg_queue.task_done()

def get_active_destinations(chat_id):
    dest_to_send = set()
    for t_id, t_info in data["tasks"].items():
        if t_info.get("active", False) and chat_id in t_info.get("sources", []):
            for d in t_info.get("destinations", []):
                dest_to_send.add(d)
    return list(dest_to_send)

# ---------------- 1. NEW SINGLE MESSAGE (Photo/Video/Text/Link) ----------------
@user_client.on(events.NewMessage())
async def handle_new_message(event):
    if event.grouped_id:
        return
    dest_list = get_active_destinations(event.chat_id)
    if dest_list:
        await msg_queue.put(("single", event, dest_list))

# ---------------- 2. NEW ALBUM (Multiple Photos/Videos Together) ----------------
@user_client.on(events.Album())
async def handle_album(event):
    dest_list = get_active_destinations(event.chat_id)
    if dest_list:
        await msg_queue.put(("album", event, dest_list))

# ---------------- 3. LIVE MESSAGE EDIT SYNC (No Delete Sync) ----------------
@user_client.on(events.MessageEdited())
async def handle_edit_message(event):
    targets = get_mappings(event.chat_id, event.message.id)
    for dest_chat, dest_msg in targets:
        await send_with_retry(
            user_client.edit_message,
            dest_chat,
            dest_msg,
            text=event.message.text,
            file=event.message.media
        )
        await asyncio.sleep(1.5)

# ---------------- UI HELPER FUNCTIONS (Top 20 Buttons) ----------------
async def refresh_top_20_chats():
    global cached_dialogs
    cached_dialogs = []
    async for dialog in user_client.iter_dialogs(limit=20):
        name = dialog.name[:24] if dialog.name else "Unknown"
        cached_dialogs.append({"id": dialog.id, "name": name})

def get_main_menu():
    text = "🤖 **PRO AUTO-FORWARDER CONTROL PANEL**\n\nApne Tasks yahan se manage karein (🟢 = Chalu, 🔴 = Band):"
    buttons = []
    for t_id, t_info in data["tasks"].items():
        status = "🟢 ON" if t_info.get("active") else "🔴 OFF"
        src_cnt = len(t_info.get("sources", []))
        dst_cnt = len(t_info.get("destinations", []))
        btn_text = f"{t_info['name']} [{status}] ({src_cnt} ➡️ {dst_cnt})"
        buttons.append([Button.inline(btn_text, f"view_{t_id}".encode())])
    
    buttons.append([Button.inline("➕ Create New Task", b"new_task")])
    return text, buttons

def get_task_menu(t_id):
    t_info = data["tasks"][t_id]
    status = "🟢 CHALU HAI (Running)" if t_info.get("active") else "🔴 BAND HAI (Stopped)"
    
    src_names = [f"`{x}`" for x in t_info.get("sources", [])]
    dst_names = [f"`{x}`" for x in t_info.get("destinations", [])]
    for d in cached_dialogs:
        for i, s_id in enumerate(t_info.get("sources", [])):
            if d["id"] == s_id: src_names[i] = f"{d['name']}"
        for i, d_id in enumerate(t_info.get("destinations", [])):
            if d["id"] == d_id: dst_names[i] = f"{d['name']}"

    text = (f"⚙️ **Task Manage: {t_info['name']}**\n\n"
            f"📊 **Status:** {status}\n\n"
            f"📥 **Sources (Jahan se aayega):**\n" + (", ".join(src_names) if src_names else "Koi select nahi hai") + "\n\n"
            f"📤 **Destinations (Jahan jayega):**\n" + (", ".join(dst_names) if dst_names else "Koi select nahi hai"))

    toggle_text = "🔴 Band Karein (Stop)" if t_info.get("active") else "🟢 Chalu Karein (Start)"
    buttons = [
        [Button.inline(toggle_text, f"toggle_{t_id}".encode())],
        [Button.inline("📥 Edit Sources (Top 20)", f"sel_src_{t_id}".encode()),
         Button.inline("📤 Edit Destinations (Top 20)", f"sel_dst_{t_id}".encode())],
        [Button.inline("🗑️ Delete Task", f"del_{t_id}".encode()),
         Button.inline("🔙 Back to Main Menu", b"main_menu")]
    ]
    return text, buttons

def get_selection_menu(t_id, mode):
    t_info = data["tasks"][t_id]
    selected_list = t_info["sources"] if mode == "src" else t_info["destinations"]
    title = "📥 **SOURCES SELECT KAREIN (Jahan se msg lena hai)**" if mode == "src" else "📤 **DESTINATIONS SELECT KAREIN (Jahan msg bhejna hai)**"
    
    lines = [title, "\n📋 **Aapke Telegram ke Top 20 Chats:**"]
    for idx, d in enumerate(cached_dialogs, 1):
        mark = "✅" if d["id"] in selected_list else "☑️"
        lines.append(f"**{idx}.** {mark} {d['name']}")
    
    lines.append("\n👇 *Neeche Number dabakar multiple select/unselect karein, fir Done dabayein:*")
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
    if event.sender_id != OWNER_ID:
        return await event.reply("⛔ Ye Private Bot hai! Sirf iska Owner ise chala sakta hai.")
    text, buttons = get_main_menu()
    await event.reply(text, buttons=buttons)

@bot_client.on(events.CallbackQuery())
async def callback_handler(event):
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
    global OWNER_ID
    await user_client.start()
    me = await user_client.get_me()
    OWNER_ID = me.id
    
    # Server chalu hote hi apne aap Saved Messages se tasks restore karega
    await auto_restore_from_cloud()
    
    await bot_client.start(bot_token=BOT_TOKEN)
    asyncio.create_task(queue_worker())
    
    app = web.Application()
    app.router.add_get('/', web_handle)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    
    print("Zero-Touch Production Bot is Live!")
    await asyncio.gather(
        user_client.run_until_disconnected(),
        bot_client.run_until_disconnected()
    )

if __name__ == '__main__':
    loop.run_until_complete(main())
