import asyncio
import io
import json
import logging
import os
import re
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

import asyncpg
import httpx
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandObject
from aiogram.types import (
    BusinessConnection,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from anthropic import AsyncAnthropic

import guard

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("replybot")

# ---------- config ----------
BOT_TOKEN = os.environ["BOT_TOKEN"]
def _db_url() -> str:
    url = (os.getenv("DATABASE_URL") or "").strip()
    if url.startswith(("postgres://", "postgresql://")):
        return url
    h, u, pw, db = (os.getenv(k) for k in ("PGHOST", "PGUSER", "PGPASSWORD", "PGDATABASE"))
    if h and u and pw and db:
        return f"postgresql://{u}:{pw}@{h}:{os.getenv('PGPORT', '5432')}/{db}"
    raise SystemExit(
        "DATABASE_URL пустой или неверный (получено: %r). В Railway у сервиса бота: Variables -> "
        "Add Reference -> сервис PostgreSQL -> DATABASE_URL." % url[:40]
    )


DATABASE_URL = _db_url()
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
OWNER_ID = int(os.environ["OWNER_ID"])  # твой Telegram user id
MODEL = os.getenv("MODEL", "claude-sonnet-5-5")
HISTORY_LIMIT = int(os.getenv("HISTORY_LIMIT", "30"))
DEBOUNCE = float(os.getenv("DEBOUNCE_SECONDS", "6"))
TAKEOVER_PAUSE_MIN = int(os.getenv("TAKEOVER_PAUSE_MIN", "15"))  # меняется командой /pause N прямо в Telegram
N_EXAMPLES = int(os.getenv("N_EXAMPLES", "10"))
BISHKEK = timezone(timedelta(hours=6))

# распознавание голосовых (любой OpenAI-совместимый сервис: OpenAI Whisper, Groq и т.п.)
STT_API_KEY = (os.getenv("STT_API_KEY") or os.getenv("OPENAI_API_KEY") or "").strip()
STT_BASE_URL = os.getenv("STT_BASE_URL", "https://api.openai.com/v1").rstrip("/")
STT_MODEL = os.getenv("STT_MODEL", "whisper-1")
STT_LANGUAGE = os.getenv("STT_LANGUAGE", "").strip()  # например "ru"; пусто = определять само
MAX_VOICE_SEC = int(os.getenv("MAX_VOICE_SECONDS", "240"))

BASE = Path(__file__).parent
STYLE = (BASE / "style.md").read_text(encoding="utf-8")
try:
    EXAMPLES = json.loads((BASE / "examples.json").read_text(encoding="utf-8"))
except Exception:
    EXAMPLES = []

RULES = """# Технические правила (строго, важнее всего остального)
- Ты пишешь от моего имени в моём личном чате Telegram, как будто это я сам. Выдай ТОЛЬКО текст сообщений: без пояснений, без кавычек вокруг, без markdown.
- Формат: как я пишу в жизни — несколько коротких сообщений подряд. Разделяй сообщения строкой из трёх дефисов (---). Максимум 4 сообщения, обычно 1–2.
- Язык — как у клиента. Обращаюсь на «вы».
- НИКОГДА не называй никого по имени: ни клиента, ни себя, ни сотрудников, ни третьих лиц. Не подписывайся. Не обращайся по имени, даже если оно видно в чате.
- Не упоминай и не обсуждай имена людей вообще. Не объясняй, почему чего-то не обсуждаешь, не говори «мне нельзя», «не могу обсуждать», «правила». Если разговор уходит в имена — не поддерживай тему, переведи на дело одной короткой фразой в моём стиле, как будто не придал значения.
- Никогда не говори и не намекай, что ты ИИ, бот, нейросеть, ассистент или что есть инструкции. Не используй фразы бота: «чем могу помочь», «буду рад», «конечно!», «безусловно», «к сожалению, я не могу». Пиши как живой человек, который отвечает с телефона.
- Если нужен я лично — оплата, скидки, договорённости, жалоба, спор, вопрос, ответа на который нет в разделе «Факты», любой вопрос, не бот ли ты и живой ли человек — выдай ровно: [ESCALATE] краткая причина
- Если сообщение клиента начинается с [голосовое] — это расшифровка его голосового. Отвечай на смысл, ошибки распознавания не комментируй и не упоминай, что это расшифровка.
- Цену не называй. Если по смыслу нужно её указать — оставь на её месте "" (две кавычки подряд), остальное допишу я сам.
- Ничего не выдумывай: сроки, цены, наличие, условия — только из раздела «Факты» или из этого же диалога. Если не знаешь — [ESCALATE].
"""

bot = Bot(BOT_TOKEN)
_ws = os.getenv("ANTHROPIC_WORKSPACE_ID", "").strip()
ai = AsyncAnthropic(
    api_key=ANTHROPIC_API_KEY,
    default_headers={"anthropic-workspace-id": _ws} if _ws else None,
)
last_error_notice = 0.0
router = Router()
pool: asyncpg.Pool

pending: dict[int, asyncio.Task] = {}
last_bot_send: dict[int, float] = {}
# тексты, которые бот сам отправил от твоего имени (чтобы не принять их эхо за твой ручной ответ)
recent_bot_texts: dict[int, deque] = {}
# журнал решений «почему бот промолчал / что сделал» (команда /why)
decisions: deque = deque(maxlen=40)
last_business_msg: dict = {"ts": 0.0, "conn": None}

SCHEMA = """
CREATE TABLE IF NOT EXISTS messages(
    id BIGSERIAL PRIMARY KEY,
    chat_id BIGINT NOT NULL,
    role TEXT NOT NULL,
    text TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS messages_chat_idx ON messages(chat_id, id);
CREATE TABLE IF NOT EXISTS chat_state(
    chat_id BIGINT PRIMARY KEY,
    paused_until TIMESTAMPTZ,
    client_name TEXT
);
CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS drafts(
    id BIGSERIAL PRIMARY KEY,
    chat_id BIGINT NOT NULL,
    connection_id TEXT NOT NULL,
    text TEXT NOT NULL,
    notify_msg_id BIGINT,
    created TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


# ---------- db helpers ----------
async def get_setting(key: str, default: str) -> str:
    v = await pool.fetchval("SELECT value FROM settings WHERE key=$1", key)
    return v if v is not None else default


async def set_setting(key: str, value: str):
    await pool.execute(
        "INSERT INTO settings(key,value) VALUES($1,$2) "
        "ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value",
        key, value,
    )


async def save(chat_id: int, role: str, text: str):
    await pool.execute(
        "INSERT INTO messages(chat_id,role,text) VALUES($1,$2,$3)", chat_id, role, text
    )


async def load_history(chat_id: int):
    rows = await pool.fetch(
        "SELECT role,text,ts FROM messages WHERE chat_id=$1 ORDER BY id DESC LIMIT $2",
        chat_id, HISTORY_LIMIT,
    )
    return list(reversed(rows))


async def pause_minutes() -> int:
    v = await get_setting("pause_min", str(TAKEOVER_PAUSE_MIN))
    try:
        return max(0, int(v))
    except ValueError:
        return TAKEOVER_PAUSE_MIN


async def pause_chat(chat_id: int, minutes: int) -> bool:
    """True, если пауза началась только что (раньше чат не был на паузе)."""
    was = await is_paused(chat_id)
    await pool.execute(
        "INSERT INTO chat_state(chat_id,paused_until) "
        "VALUES($1, now() + make_interval(mins => $2)) "
        "ON CONFLICT (chat_id) DO UPDATE SET paused_until=EXCLUDED.paused_until",
        chat_id, minutes,
    )
    return not was


async def is_paused(chat_id: int) -> bool:
    v = await pool.fetchval("SELECT paused_until > now() FROM chat_state WHERE chat_id=$1", chat_id)
    return bool(v)


def note(chat_id: int, name: str, what: str):
    """Запоминаем, что бот решил по этому чату (видно в /why и в логах Railway)."""
    log.info("decision chat=%s (%s): %s", chat_id, name, what)
    decisions.append((datetime.now(BISHKEK), chat_id, name, what))


async def notify_owner(text: str, **kw) -> bool:
    """Пишет тебе в личку с ботом. Если не вышло, это видно в логах (раньше глоталось молча)."""
    try:
        await bot.send_message(OWNER_ID, text, **kw)
        return True
    except Exception as e:
        log.error(
            "НЕ МОГУ НАПИСАТЬ ВЛАДЕЛЬЦУ (OWNER_ID=%s): %s: %s. Проверь OWNER_ID и нажми /start у бота.",
            OWNER_ID, type(e).__name__, e,
        )
        return False


# ---------- prompt building ----------
def tokens(s: str) -> set[str]:
    return set(re.findall(r"\w{3,}", guard.norm(s)))


def pick_examples(text: str) -> list[dict]:
    if not EXAMPLES:
        return []
    t = tokens(text)
    scored = []
    for e in EXAMPLES:
        et = tokens(e["q"])
        scored.append((len(t & et) / ((len(et) or 1) ** 0.5), e))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [e for _, e in scored[:N_EXAMPLES]]


def human_gap(delta: timedelta) -> str:
    mins = int(delta.total_seconds() // 60)
    if mins < 60:
        return f"{max(mins, 1)} мин"
    if mins < 60 * 24:
        return f"{mins // 60} ч"
    return f"{mins // (60 * 24)} дн"


def context_note(rows) -> str:
    now = datetime.now(BISHKEK)
    h = now.hour
    greet = "Доброе утро" if 4 <= h < 12 else "Добрый день" if 12 <= h < 17 else "Добрый вечер" if 17 <= h < 23 else "Здравствуйте"
    i = len(rows)
    while i > 0 and rows[i - 1]["role"] == "user":
        i -= 1
    if i == 0:
        gap = "Это первое сообщение клиента в этом чате."
    else:
        d = datetime.now(timezone.utc) - rows[i - 1]["ts"]
        gap = f"Предыдущее сообщение в чате (до этих) было {human_gap(d)} назад."
    return (
        f"# Контекст\nСейчас у меня {now:%H:%M}. Уместное приветствие сейчас: «{greet}». {gap}\n"
        "Здороваюсь отдельным коротким сообщением только после паузы в несколько часов или если клиент сам поздоровался; "
        "в середине живого разговора не здороваюсь."
    )


def build_system(rows, extra: str = "") -> str:
    last_q = trailing_client_text(rows)
    parts = [STYLE]
    ex = pick_examples(last_q)
    if ex:
        parts.append(
            "# Примеры, как я реально отвечал (копируй манеру, ритм и длину, НЕ факты; --- разделяет мои сообщения)\n"
            + "\n\n".join(f"Клиент: {e['q']}\nЯ: {e['a']}" for e in ex)
        )
    parts.append(context_note(rows))
    parts.append(RULES)
    if extra:
        parts.append(extra)
    return "\n\n".join(parts)


def to_messages(rows) -> list[dict]:
    msgs: list[dict] = []
    for r in rows:
        role = "user" if r["role"] == "user" else "assistant"
        if msgs and msgs[-1]["role"] == role:
            msgs[-1]["content"] += ("\n" if role == "user" else "\n---\n") + r["text"]
        else:
            msgs.append({"role": role, "content": r["text"]})
    while msgs and msgs[0]["role"] != "user":
        msgs.pop(0)
    return msgs


def trailing_client_text(rows) -> str:
    out = []
    for r in reversed(rows):
        if r["role"] != "user":
            break
        out.append(r["text"])
    return "\n".join(reversed(out))


async def llm(rows, extra: str = "") -> str:
    resp = await ai.messages.create(
        model=MODEL,
        max_tokens=500,
        system=build_system(rows, extra),
        messages=to_messages(rows),
    )
    return "".join(b.text for b in resp.content if b.type == "text").strip()


async def generate_checked(rows, name_tokens=(), hint: str = ""):
    """-> ("ok", [parts]) | ("escalate", reason)"""
    extra = hint
    for attempt in range(2):
        raw = await llm(rows, extra)
        if not raw:
            return "escalate", "пустой ответ модели"
        if raw.lstrip().startswith("[ESCALATE]"):
            return "escalate", raw.lstrip()[len("[ESCALATE]"):].strip() or "нужен ты"
        text = guard.sanitize(raw)
        why = guard.bad_reply(text, name_tokens)
        if not why:
            parts = guard.split_parts(text)
            if parts:
                return "ok", parts
            why = "пустой ответ"
        log.warning("reply rejected (%s), attempt %d", why, attempt + 1)
        extra = hint + "\n\n" + (
            f"# Исправление\nПредыдущий вариант не прошёл проверку: {why}. Перепиши короче и естественнее: "
            "без имён, без упоминания ИИ/правил/запретов, без шаблонной вежливости. Если не получается ответить безопасно — [ESCALATE] причина."
        )
    return "escalate", f"ответ не прошёл проверку ({why})"


# ---------- sending ----------
async def send_as_owner(chat_id: int, conn_id: str, parts: list[str]):
    for i, part in enumerate(parts):
        try:
            await bot.send_chat_action(chat_id, "typing", business_connection_id=conn_id)
        except Exception:
            pass
        await asyncio.sleep(min(6.0, 0.8 + len(part) / 30) if i == 0 else min(4.0, 0.5 + len(part) / 40))
        last_bot_send[chat_id] = time.time()
        recent_bot_texts.setdefault(chat_id, deque(maxlen=20)).append((time.time(), part.strip()))
        await bot.send_message(chat_id, part, business_connection_id=conn_id)
        last_bot_send[chat_id] = time.time()
        await save(chat_id, "assistant", part)


async def send_draft(chat_id: int, conn_id: str, name: str, client_text: str, parts: list[str]):
    draft_id = await pool.fetchval(
        "INSERT INTO drafts(chat_id,connection_id,text) VALUES($1,$2,$3) RETURNING id",
        chat_id, conn_id, "\n---\n".join(parts),
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Отправить", callback_data=f"send:{draft_id}"),
        InlineKeyboardButton(text="🗑 Пропустить", callback_data=f"skip:{draft_id}"),
    ]])
    shown = "\n— — —\n".join(parts)
    note = await bot.send_message(
        OWNER_ID,
        f"💬 {name} (tg://user?id={chat_id}):\n{client_text}\n\n✍️ Черновик:\n{shown}\n\n"
        f"Можно ответить на это сообщение своим текстом — отправлю его клиенту.",
        reply_markup=kb,
    )
    await pool.execute("UPDATE drafts SET notify_msg_id=$1 WHERE id=$2", note.message_id, draft_id)


async def notify_escalation(chat_id: int, name: str, client_text: str, reason: str):
    note(chat_id, name, f"передал тебе: {reason}")
    await notify_owner(f"⚠️ Нужен ты: {name} (tg://user?id={chat_id})\nПричина: {reason}\n\n{client_text}")


def is_bot_echo(chat_id: int, text: str | None) -> bool:
    """Это эхо сообщения, которое бот сам отправил от твоего имени (а не твой ручной ответ)?"""
    if not text:
        return False
    q = recent_bot_texts.get(chat_id)
    if not q:
        return False
    now = time.time()
    t = text.strip()
    for item in list(q):
        ts, sent = item
        if now - ts > 180:
            q.remove(item)
        elif sent == t:
            q.remove(item)
            return True
    return False


# ---------- голосовые ----------
def media_name_mime(m: Message) -> tuple[str, str]:
    if m.video_note:
        return "video.mp4", "video/mp4"
    if m.audio:
        mime = m.audio.mime_type or "audio/mpeg"
        fn = m.audio.file_name or ""
        if "." not in fn:
            fn = "audio." + (mime.split("/")[-1].replace("mpeg", "mp3") or "mp3")
        return fn, mime
    return "voice.ogg", "audio/ogg"


async def transcribe(file_id: str, filename: str, mime: str) -> str:
    buf = io.BytesIO()
    await bot.download(file_id, destination=buf)
    data = {"model": STT_MODEL}
    if STT_LANGUAGE:
        data["language"] = STT_LANGUAGE
    async with httpx.AsyncClient(timeout=90) as c:
        r = await c.post(
            f"{STT_BASE_URL}/audio/transcriptions",
            headers={"Authorization": f"Bearer {STT_API_KEY}"},
            data=data,
            files={"file": (filename, buf.getvalue(), mime)},
        )
    r.raise_for_status()
    return (r.json().get("text") or "").strip()


async def client_content(m: Message) -> str:
    """Текст сообщения клиента; голосовые расшифровываются. Неудача -> «[вложение: …]» (уйдёт тебе)."""
    text = (m.text or m.caption or "").strip()
    media = m.voice or m.audio or m.video_note
    if not media:
        return text or "[вложение: фото/файл]"
    if not STT_API_KEY:
        return "[вложение: голосовое, распознавание не настроено (нет STT_API_KEY)]"
    if (getattr(media, "duration", 0) or 0) > MAX_VOICE_SEC:
        return "[вложение: голосовое слишком длинное]"
    try:
        name, mime = media_name_mime(m)
        heard = await transcribe(media.file_id, name, mime)
    except Exception as e:
        log.exception("transcribe failed")
        why = f"{type(e).__name__}"
        resp = getattr(e, "response", None)
        if resp is not None:
            why += f" {resp.status_code}"
        return f"[вложение: голосовое не удалось расшифровать ({why}); проверь STT_API_KEY / STT_BASE_URL / STT_MODEL]"
    if not heard:
        return "[вложение: в голосовом не расслышал речь]"
    return f"[голосовое] {heard}" + (f"\n{text}" if text else "")


# ---------- core flow ----------
async def process(chat_id: int, conn_id: str, name: str, name_tokens: tuple):
    try:
        await asyncio.sleep(DEBOUNCE)
        if await get_setting("enabled", "1") != "1":
            note(chat_id, name, "молчу: бот выключен (/off). Включить: /on")
            return
        if await is_paused(chat_id):
            left = await pool.fetchval(
                "SELECT ceil(extract(epoch FROM (paused_until - now())) / 60) FROM chat_state WHERE chat_id=$1", chat_id
            )
            note(chat_id, name, f"молчу: в чате пауза (ты писал сам), ещё ~{int(left or 0)} мин. Снять: /resume")
            return
        rows = await load_history(chat_id)
        if not rows or rows[-1]["role"] != "user":
            note(chat_id, name, "молчу: последним в чате написал ты, отвечать не на что")
            return
        client_text = trailing_client_text(rows)

        hint = ""
        cls = guard.classify_client(client_text)
        if cls:
            kind, info = cls
            if kind == "escalate":
                await notify_escalation(chat_id, name, client_text, info)
                return
            hint = info

        status, result = await generate_checked(rows, name_tokens, hint)
        if status == "escalate":
            await notify_escalation(chat_id, name, client_text, result)
            return

        parts = result
        mode = await get_setting("mode", "draft")
        if mode == "auto" and not any('""' in p for p in parts):
            note(chat_id, name, "отвечаю клиенту сам (режим /auto)")
            # отправку не отменяем, даже если пришло новое сообщение клиента
            await asyncio.shield(send_as_owner(chat_id, conn_id, parts))
        else:
            why = "режим /draft" if mode != "auto" else "в ответе цена (\"\")"
            note(chat_id, name, f"прислал тебе черновик ({why}), клиенту ничего не ушло")
            await send_draft(chat_id, conn_id, name, client_text, parts)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.exception("process failed for chat %s", chat_id)
        note(chat_id, name, f"ОШИБКА: {type(e).__name__}: {str(e)[:200]}")
        global last_error_notice
        if time.time() - last_error_notice > 600:
            last_error_notice = time.time()
            await notify_owner(
                f"❗ Бот не смог ответить клиенту ({name}): {type(e).__name__}: {str(e)[:300]}\n"
                "Ответь сам; подробности в логах Railway."
            )


def schedule(chat_id: int, conn_id: str, name: str, name_tokens: tuple):
    t = pending.get(chat_id)
    if t and not t.done():
        t.cancel()
    pending[chat_id] = asyncio.create_task(process(chat_id, conn_id, name, name_tokens))


@router.business_message()
async def on_business_message(m: Message):
    chat_id = m.chat.id
    last_business_msg["ts"] = time.time()
    last_business_msg["conn"] = m.business_connection_id
    from_owner = bool(m.from_user and m.from_user.id == OWNER_ID)
    echo_flag = getattr(m, "sender_business_bot", None) is not None
    log.info(
        "business_message chat=%s from=%s from_owner=%s bot_echo_flag=%s type=%s",
        chat_id, m.from_user.id if m.from_user else None, from_owner, echo_flag,
        "voice" if m.voice else "video_note" if m.video_note else "audio" if m.audio else "text" if m.text else "other",
    )
    # эхо сообщений, которые отправил сам бот от твоего имени
    if echo_flag:
        return
    if from_owner:
        text = m.text or m.caption
        if is_bot_echo(chat_id, text):
            return  # это эхо моего же ответа, не твой ручной
        # это твой ручной ответ из приложения Telegram
        t = pending.get(chat_id)
        if t and not t.done():
            t.cancel()
        if text:
            await save(chat_id, "assistant", text)
        minutes = await pause_minutes()
        if minutes <= 0:
            note(chat_id, "", "ты написал сам, пауза выключена (/pause 0), отвечу на следующее сообщение клиента")
            return
        if await pause_chat(chat_id, minutes):
            note(chat_id, "", f"ты написал сам, чат на паузе {minutes} мин")
            await notify_owner(
                f"⏸ Бот молчит в этом чате {minutes} мин: ты написал сам со своего аккаунта.\n"
                "Снять паузу: /resume. Изменить или выключить паузу: /pause 5 или /pause 0"
            )
        return
    text = await client_content(m)
    u = m.from_user
    name = u.full_name if u else "клиент"
    name_tokens = tuple(
        t for t in re.findall(r"\w{3,}", guard.norm(f"{u.first_name or ''} {u.last_name or ''} {u.username or ''}"))
    ) if u else ()
    await pool.execute(
        "INSERT INTO chat_state(chat_id,client_name) VALUES($1,$2) "
        "ON CONFLICT (chat_id) DO UPDATE SET client_name=EXCLUDED.client_name",
        chat_id, name,
    )
    await save(chat_id, "user", text)
    schedule(chat_id, m.business_connection_id, name, name_tokens)


@router.business_connection()
async def on_connection(bc: BusinessConnection):
    state = "подключён" if bc.is_enabled else "отключён"
    await bot.send_message(OWNER_ID, f"🔗 Бизнес-подключение {state}.")


# ---------- owner controls ----------
owner = F.from_user.id == OWNER_ID


@router.message(Command("id"))
async def cmd_id(m: Message):
    await m.answer(f"Твой id: {m.from_user.id}")


@router.message(Command("start"), owner)
async def cmd_start(m: Message):
    await m.answer(
        "Команды:\n/draft — присылать черновики мне\n/auto — отвечать клиентам самому\n"
        "/on /off — включить/выключить\n/status — состояние и проверка связи\n/why — почему бот молчал (последние решения)\n"
        "/resume — снять паузу во всех чатах\n/pause N — на сколько минут бот замолкает, когда ты пишешь сам (0 = не замолкать)\n"
        "Голосовое мне в личку — покажу расшифровку и как бы ответил клиенту\n"
        "/try текст — проверить, как бот ответит на такое сообщение клиента (никому не уходит)"
    )


@router.message(Command("draft"), owner)
async def cmd_draft(m: Message):
    await set_setting("mode", "draft")
    await m.answer("Режим: черновики. Ничего не уходит клиентам без твоей кнопки.")


@router.message(Command("auto"), owner)
async def cmd_auto(m: Message):
    await set_setting("mode", "auto")
    await m.answer("Режим: авто. Цены, оплата и спорное всё равно приходят тебе.")


@router.message(Command("on"), owner)
async def cmd_on(m: Message):
    await set_setting("enabled", "1")
    await m.answer("Включён.")


@router.message(Command("off"), owner)
async def cmd_off(m: Message):
    await set_setting("enabled", "0")
    await m.answer("Выключен.")


@router.message(Command("status"), owner)
async def cmd_status(m: Message):
    mode = await get_setting("mode", "draft")
    en = await get_setting("enabled", "1")
    minutes = await pause_minutes()
    paused = await pool.fetch(
        "SELECT chat_id, client_name, ceil(extract(epoch FROM (paused_until - now())) / 60) AS left "
        "FROM chat_state WHERE paused_until > now()"
    )
    lines = [
        f"Включён: {'да' if en == '1' else 'нет'}",
        f"Режим: {'авто (отвечаю сам)' if mode == 'auto' else 'черновики (клиентам не уходит без кнопки)'}",
        f"Пауза после твоих сообщений: {minutes} мин" if minutes else "Пауза после твоих сообщений: выключена",
        f"Чатов на паузе сейчас: {len(paused)}" + "".join(
            f"\n  • {r['client_name'] or r['chat_id']}: ещё ~{int(r['left'])} мин" for r in paused
        ),
        f"Голосовые: {'расшифровываю (' + STT_MODEL + ')' if STT_API_KEY else 'НЕ настроены (нет STT_API_KEY)'}",
        f"Модель: {MODEL}; примеров стиля: {len(EXAMPLES)}",
    ]
    if last_business_msg["ts"]:
        ago = int(time.time() - last_business_msg["ts"])
        lines.append(f"Последнее сообщение из бизнес-чатов получено {human_gap(timedelta(seconds=ago))} назад")
        try:
            bc = await bot.get_business_connection(last_business_msg["conn"])
            rights = getattr(bc, "rights", None)
            can_reply = getattr(rights, "can_reply", None) if rights is not None else getattr(bc, "can_reply", None)
            lines.append(
                f"Подключение: {'включено' if bc.is_enabled else 'ОТКЛЮЧЕНО'}; "
                f"право отвечать: {'да' if can_reply else 'НЕТ (включи в Настройки → Telegram для бизнеса → Чат-боты)' if can_reply is not None else 'не удалось проверить'}"
            )
        except Exception as e:
            lines.append(f"Подключение проверить не удалось: {type(e).__name__}")
    else:
        lines.append(
            "С момента запуска бот не получил НИ ОДНОГО сообщения из бизнес-чатов. Проверь: Настройки → Telegram для бизнеса → "
            "Чат-боты: бот выбран, нужный чат не в исключениях, включено «Отвечать на сообщения»."
        )
    await m.answer("\n".join(lines))


@router.message(Command("why"), owner)
async def cmd_why(m: Message):
    if not decisions:
        return await m.answer(
            "Пока пусто: с момента запуска бот не принял ни одного решения по клиентским сообщениям. "
            "Значит, сообщения до него не доходят. Смотри /status."
        )
    out = []
    for ts, chat_id, name, what in list(decisions)[-12:]:
        who = f" {name}" if name else ""
        out.append(f"{ts:%H:%M:%S}{who} [{chat_id}]: {what}")
    await m.answer("Последние решения бота:\n\n" + "\n".join(out))


@router.message(Command("pause"), owner)
async def cmd_pause(m: Message, command: CommandObject):
    arg = (command.args or "").strip()
    if not arg:
        n = await pause_minutes()
        return await m.answer(
            f"Сейчас: {n} мин. Когда ты пишешь клиенту сам, бот молчит в этом чате столько минут.\n"
            "Изменить: /pause 5. Не замолкать вообще: /pause 0."
        )
    if not arg.isdigit() or int(arg) > 1440:
        return await m.answer("Напиши число минут от 0 до 1440, например /pause 10")
    await set_setting("pause_min", arg)
    await m.answer("Пауза выключена: бот не замолкает, когда ты пишешь сам." if arg == "0" else f"Пауза после твоих сообщений: {arg} мин.")


@router.message(Command("resume"), owner)
async def cmd_resume(m: Message):
    n = await pool.fetchval("SELECT count(*) FROM chat_state WHERE paused_until > now()")
    await pool.execute("UPDATE chat_state SET paused_until=NULL")
    await m.answer(f"▶️ Пауза снята во всех чатах (было на паузе: {n}).")


@router.message(owner, F.voice | F.video_note | F.audio)
async def owner_voice_test(m: Message):
    """Тест голосовых: пришли боту голосовое в личку, он покажет расшифровку и как бы ответил."""
    if not STT_API_KEY:
        return await m.answer("Распознавание не настроено: добавь в Railway переменную STT_API_KEY (см. README).")
    text = await client_content(m)
    if text.startswith("[вложение"):
        return await m.answer("⚠️ " + text)
    await m.answer("🎙 Расшифровка:\n" + text.replace("[голосовое] ", "", 1))
    cls = guard.classify_client(text)
    hint = ""
    if cls:
        kind, info = cls
        if kind == "escalate":
            return await m.answer(f"⚠️ Бот сам не ответил бы, передал бы тебе: {info}")
        hint = info
    rows = [{"role": "user", "text": text, "ts": datetime.now(timezone.utc)}]
    status, result = await generate_checked(rows, (), hint)
    if status == "escalate":
        return await m.answer(f"⚠️ Передал бы тебе: {result}")
    await m.answer("Так бы ушло клиенту:\n\n" + "\n— — —\n".join(result))


@router.message(Command("try"), owner)
async def cmd_try(m: Message, command: CommandObject):
    text = (command.args or "").strip()
    if not text:
        return await m.answer("Напиши так: /try Добрый день, сколько стоит боди?")
    hint = ""
    cls = guard.classify_client(text)
    if cls:
        kind, info = cls
        if kind == "escalate":
            return await m.answer(f"⚠️ Бот сам не ответил бы, передал бы тебе: {info}")
        hint = info
    rows = [{"role": "user", "text": text, "ts": datetime.now(timezone.utc)}]
    status, result = await generate_checked(rows, (), hint)
    if status == "escalate":
        return await m.answer(f"⚠️ Передал бы тебе: {result}")
    await m.answer("Так бы ушло клиенту:\n\n" + "\n— — —\n".join(result))


@router.callback_query(F.data.startswith("send:"))
async def cb_send(c: CallbackQuery):
    if c.from_user.id != OWNER_ID:
        return await c.answer()
    row = await pool.fetchrow("DELETE FROM drafts WHERE id=$1 RETURNING *", int(c.data.split(":")[1]))
    if not row:
        return await c.answer("Уже обработано")
    await send_as_owner(row["chat_id"], row["connection_id"], guard.split_parts(row["text"]))
    await c.message.edit_text(c.message.text + "\n\n✅ Отправлено")
    await c.answer()


@router.callback_query(F.data.startswith("skip:"))
async def cb_skip(c: CallbackQuery):
    if c.from_user.id != OWNER_ID:
        return await c.answer()
    await pool.execute("DELETE FROM drafts WHERE id=$1", int(c.data.split(":")[1]))
    await c.message.edit_text(c.message.text + "\n\n🗑 Пропущено")
    await c.answer()


@router.message(owner, F.reply_to_message, F.text)
async def custom_reply(m: Message):
    row = await pool.fetchrow(
        "SELECT * FROM drafts WHERE notify_msg_id=$1", m.reply_to_message.message_id
    )
    if not row:
        return
    await pool.execute("DELETE FROM drafts WHERE id=$1", row["id"])
    await send_as_owner(row["chat_id"], row["connection_id"], guard.split_parts(m.text) or [m.text])
    await m.answer("✅ Отправлено твоим текстом")


# ---------- main ----------
async def main():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL)
    async with pool.acquire() as conn:
        await conn.execute(SCHEMA)
    dp = Dispatcher()
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=False)
    log.info("started, model=%s, examples=%d", MODEL, len(EXAMPLES))
    ok = await notify_owner(
        f"✅ Бот запущен. Режим: {await get_setting('mode', 'draft')}. "
        f"Голосовые: {'да' if STT_API_KEY else 'нет (нет STT_API_KEY)'}. Проверка связи: /status"
    )
    if not ok:
        log.error("Владелец не получает сообщения от бота: проверь OWNER_ID и нажми /start в чате с ботом.")
    await dp.start_polling(
        bot,
        allowed_updates=["message", "callback_query", "business_connection", "business_message"],
    )


if __name__ == "__main__":
    asyncio.run(main())
