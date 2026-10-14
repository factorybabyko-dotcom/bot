"""Собирает examples.json (пары «сообщение клиента -> твои ответы») из экспорта Telegram Desktop.

Поддерживает и HTML (messages.html), и JSON (result.json).

    pip install beautifulsoup4
    python import_chats.py --me "Адилет" messages.html [messages2.html ...]
    python import_chats.py --me 123456789 result.json

--me: часть твоего имени в экспорте (для HTML) или твой числовой user id (для JSON).
Файл examples.json дополняется: старые примеры сохраняются, дубли убираются.

Из примеров автоматически выбрасывается всё, где есть: имена людей, запрещённые слова,
e-mail, телефоны, длинные коды (КИЗ/GTIN), ссылки.
"""
import argparse
import json
import re
from pathlib import Path

from guard import mentions_forbidden, mentions_name

PHONE = re.compile(r"\+?\d[\d\s\-()]{8,}\d")
EMAIL = re.compile(r"\S+@\S+\.\S+")
LONGCODE = re.compile(r"\S{25,}")
URL = re.compile(r"https?://|www\.|t\.me/")
# темы, которые не должны попадать в примеры: ИИ/боты/автоматизация (чтобы бот не палился) и посторонний трёп про VPN
NOISE = re.compile(r"нейро|\bбот\w*|\bии\b|gpt|claude|автоматиз|скрипт|\bвпн\b|vpn|wireguard|\bсервер\w*|блокир|тестирую", re.I)
SEP = "\n---\n"


def flat(t) -> str:
    if isinstance(t, str):
        return t
    return "".join(p if isinstance(p, str) else p.get("text", "") for p in t)


def load_html(path: Path) -> list[list[dict]]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(path.read_text(encoding="utf-8"), "html.parser")
    msgs, prev = [], ""
    for d in soup.select("div.message.default"):
        fn = d.select_one("div.from_name")
        if fn:
            for sp in fn.select("span"):
                sp.extract()
            prev = fn.get_text(" ", strip=True)
        t = d.select_one("div.text")
        msgs.append({
            "from": prev,
            "text": t.get_text("\n", strip=True) if t else "",
            "fwd": bool(d.select_one("div.forwarded")),
        })
    return [msgs]


def load_json(path: Path) -> list[list[dict]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    chats = data["chats"]["list"] if "chats" in data else [data]
    out = []
    for chat in chats:
        if chat.get("type") != "personal_chat":
            continue
        out.append([
            {"from": m.get("from") or "", "from_id": m.get("from_id", ""),
             "text": flat(m.get("text", "")), "fwd": "forwarded_from" in m}
            for m in chat.get("messages", []) if m.get("type") == "message"
        ])
    return out


def is_me(m: dict, me: str) -> bool:
    if me.isdigit():
        return m.get("from_id") == f"user{me}"
    return me.lower() in (m.get("from") or "").lower()


def dirty(s: str) -> bool:
    return bool(PHONE.search(s) or EMAIL.search(s) or LONGCODE.search(s) or URL.search(s)
                or NOISE.search(s) or mentions_forbidden(s) or mentions_name(s))


def extract(msgs: list[dict], me: str) -> list[dict]:
    pairs, q, a = [], [], []

    def flush():
        nonlocal q, a
        if q and a:
            pairs.append({"q": "\n".join(q), "a": SEP.join(a)})
        q, a = [], []

    for m in msgs:
        t = (m["text"] or "").strip()
        if is_me(m, me):
            if m["fwd"] or not t:
                continue
            if q:
                a.append(t)
        else:
            if not t or m["fwd"]:
                t = "[вложение]"
            if a:
                flush()
            q.append(t)
    flush()
    return pairs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--me", required=True)
    ap.add_argument("--out", default="examples.json")
    ap.add_argument("files", nargs="+")
    args = ap.parse_args()

    out_path = Path(args.out)
    existing = json.loads(out_path.read_text(encoding="utf-8")) if out_path.exists() else []
    pairs = list(existing)
    for f in args.files:
        p = Path(f)
        chats = load_html(p) if p.suffix.lower() in (".html", ".htm") else load_json(p)
        for msgs in chats:
            pairs += extract(msgs, args.me)

    seen, res, dropped = set(), [], 0
    for e in pairs:
        if (e["q"], e["a"]) in seen:
            continue
        seen.add((e["q"], e["a"]))
        only_att = e["q"].replace("[вложение]", "").strip() == ""
        parts = e["a"].split(SEP)
        if (len(parts) > 4 or any("\n" in x for x in parts) or len(e["q"]) > 500 or len(e["a"]) > 600 or len(e["a"].replace(SEP, "")) < 2
                or only_att or dirty(e["a"]) or dirty(e["q"])):
            dropped += 1
            continue
        res.append(e)
    out_path.write_text(json.dumps(res, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"Готово: {len(res)} примеров -> {out_path} (отброшено {dropped}: имена, контакты, коды, длинные, только вложения)")


if __name__ == "__main__":
    main()
