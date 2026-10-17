"""Passwort-Richtlinie, zentral für alle Stellen, an denen ein Passwort festgelegt wird.

Orientiert sich an NIST SP 800-63B (Rev. 4) und den BSI-Empfehlungen: lieber Länge und eine
Sperrliste als Pflicht-Sonderzeichen ("Passwort1!" erfüllt jede Zeichenregel und ist trotzdem
sofort erraten).
  - mindestens 8 Zeichen (von Menschen gewählte Passwörter), höchstens 128
  - keine Zusammensetzungsregeln (Groß-/Kleinbuchstaben, Ziffern, Sonderzeichen)
  - Leerzeichen und alle Unicode-Zeichen sind erlaubt, nichts wird abgeschnitten oder gekürzt
  - Sperrliste: häufige Passwörter, Tastaturreihen/Zahlenfolgen, Wiederholungen, Benutzername und
    Begriffe aus dem Umfeld (Dienstbuch, Feuerwehr) - auch mit angehängten Ziffern/Zeichen
static/js/password-policy.js spiegelt diese Regeln für die Live-Anzeige; maßgeblich ist immer diese Prüfung.
"""
import re

MIN_LEN = 8
MAX_LEN = 128

CONTEXT_WORDS = ["dienstbuch", "feuerwehr", "ffw", "floriansjuenger"]

COMMON = {
    "password", "passwort", "passw0rd", "p4ssw0rd", "pa55word", "pa55wort", "passwort1", "password1", "geheim", "geheim123", "secret",
    "123456", "1234567", "12345678", "123456789", "1234567890", "0123456789", "987654321", "87654321", "11111111", "00000000",
    "123123123", "12341234", "123321123", "112233445", "1q2w3e4r", "1q2w3e4r5t", "q1w2e3r4", "qwertz", "qwertz123", "qwertzui",
    "qwertzuiop", "qwerty", "qwerty123", "qwertyui", "qwertyuiop", "asdfghjk", "asdfghjkl", "asdf1234", "yxcvbnm", "zxcvbnm",
    "abc12345", "abcd1234", "abcdefgh", "abcdefg1", "iloveyou", "ichliebedich", "willkommen", "hallo123", "hallo1234", "hallowelt",
    "sommer", "winter", "sommer2020", "winter2020", "frühling", "fruehling", "herbst", "sonne123", "schatz", "schatzi", "liebe",
    "fussball", "fussball1", "bayern", "bayern123", "borussia", "dortmund", "schalke04", "hamburg", "berlin", "berlin123", "munchen",
    "master", "master123", "monkey", "dragon", "letmein", "welcome", "welcome1", "login", "admin", "admin123", "admin1234", "administrator",
    "root", "root1234", "toor", "test", "test123", "test1234", "testtest", "user", "user1234", "demo", "demo1234", "changeme", "change_me",
    "default", "guest", "gast", "gast1234", "start123", "start1234", "init1234", "zuhause", "mutter", "vater", "hund", "katze",
    "feuerwehr", "feuerwehr1", "feuerwehr112", "feuer112", "dienstbuch", "dienstbuch1", "florian", "florian112", "florian123",
    "112112112", "notruf112", "brandschutz", "loeschzug", "atemschutz", "wehrfuehrer", "kamerad", "kameraden", "kamerad123",
    "mannschaft", "gruppenfuehrer", "zugfuehrer", "wehrleiter", "gemeinde", "stadtwerke", "rettung", "retter", "sirene", "alarm", "alarm112",
    "passwort123", "passwort12", "password12", "password123", "password1234", "passwort1234", "geheim1234", "iloveyou1", "sunshine",
    "baseball", "football", "superman", "batman", "trustno1", "whatever", "freedom", "shadow", "michael", "jennifer", "charlie",
}

_SEQUENCES = ["0123456789", "abcdefghijklmnopqrstuvwxyz", "qwertzuiopü", "qwertyuiop", "asdfghjklöä", "yxcvbnm", "zxcvbnm"]


def _core(pw: str) -> str:
    """Kleinbuchstaben, ohne angehängte Ziffern/Zeichen ("Passwort2024!" -> "passwort")."""
    return re.sub(r"[\d\W_]+$", "", pw.lower())


def _is_run(low: str) -> bool:
    if len(set(low)) == 1:
        return True
    for seq in _SEQUENCES:
        if low in seq or low in seq[::-1]:
            return True
    return False


def _is_repeated_block(low: str) -> bool:
    # "abcabcabc", "12341234", "aaaabbbb"-artig: ein kurzer Block, der sich wiederholt
    for n in range(1, len(low) // 2 + 1):
        if len(low) % n == 0 and low[:n] * (len(low) // n) == low:
            return True
    return False


def check_password(pw, username=None, old=None):
    """Gibt None zurück, wenn das Passwort passt - sonst eine verständliche Meldung (deutsch)."""
    if not isinstance(pw, str) or len(pw) < MIN_LEN:
        return f"Das Passwort muss mindestens {MIN_LEN} Zeichen lang sein."
    if len(pw) > MAX_LEN:
        return f"Das Passwort darf höchstens {MAX_LEN} Zeichen lang sein."
    low = pw.lower()
    core = _core(pw)
    u = (username or "").strip().lower()
    if u and (low == u or (len(u) >= 4 and core == u)):
        return "Das Passwort darf nicht dem Benutzernamen entsprechen."
    if low in COMMON or (len(core) >= 4 and core in COMMON) or core in CONTEXT_WORDS:
        return "Dieses Passwort ist zu bekannt und leicht zu erraten. Bitte etwas Eigenes wählen, z. B. einen Satz aus mehreren Wörtern."
    if _is_run(low) or _is_repeated_block(low):
        return "Das Passwort besteht nur aus Wiederholungen oder einer einfachen Reihenfolge (z. B. 12345678, aaaaaaaa, abcdefgh)."
    if old is not None and pw == old:
        return "Das neue Passwort muss sich vom bisherigen unterscheiden."
    # Optional (Verwaltung -> System -> Sicherheit): Abgleich mit bekannten Datenlecks
    n = pwned_count(pw) if _pwned_enabled() else 0
    if n:
        return f"Dieses Passwort taucht in bekannten Datenlecks auf ({n:,}-mal) und ist damit nicht mehr sicher. Bitte etwas anderes wählen.".replace(",", ".")
    return None


def _pwned_enabled() -> bool:
    try:
        from core.security import get_int_setting
        return get_int_setting("pwned_check", 0) == 1
    except Exception:
        return False


def pwned_count(pw: str) -> int:
    """Wie oft das Passwort in bekannten Datenlecks vorkommt (haveibeenpwned.com, "Pwned Passwords").
    Datenschutz: es werden nur die ersten 5 Zeichen des SHA-1-Hashes gesendet (k-Anonymität) - weder das Passwort
    noch der volle Hash verlassen den Server. Bei Netzwerkproblemen: 0 (die Prüfung blockiert nie die Anmeldung)."""
    import hashlib
    import requests
    sha = hashlib.sha1(pw.encode("utf-8")).hexdigest().upper()
    prefix, suffix = sha[:5], sha[5:]
    try:
        r = requests.get(f"https://api.pwnedpasswords.com/range/{prefix}", headers={"Add-Padding": "true", "User-Agent": "Dienstbuch-Passwortpruefung"}, timeout=3)
        if r.status_code != 200:
            return 0
        for line in r.text.splitlines():
            h, _, c = line.partition(":")
            if h.strip() == suffix:
                return int(c.strip() or 0)
    except Exception:
        return 0
    return 0


def policy_for_client():
    return {"min": MIN_LEN, "max": MAX_LEN, "common": sorted(COMMON), "context": CONTEXT_WORDS, "sequences": _SEQUENCES}
