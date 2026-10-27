"""Ausrückeordnung (AAO) fürs Hallen-Display: Stichwort-Muster -> Fahrzeuge in Reihenfolge."""
import json


def _terms(pattern: str):
    return [t.strip().lower() for t in (pattern or "").split(",") if t.strip()]


def match_score(pattern: str, stichwort: str):
    """None = passt nicht. Sonst die Länge des längsten passenden Begriffs (spezifischer = höher); "*" passt immer, Wert 0.
    Ein Begriff passt, wenn er im Stichwort vorkommt (Groß-/Kleinschreibung egal), "B2" also auch auf "B2 - Wohnungsbrand"."""
    st = (stichwort or "").lower()
    best = None
    for t in _terms(pattern):
        if t == "*":
            score = 0
        elif t in st:
            score = len(t)
        else:
            continue
        best = score if best is None else max(best, score)
    return best


def parse_entries(raw) -> list:
    try:
        data = json.loads(raw) if isinstance(raw, str) else (raw or [])
    except ValueError:
        return []
    return [{"vehicle": str(e.get("vehicle", "")), "role": str(e.get("role", ""))} for e in data if isinstance(e, dict) and e.get("vehicle")]


def pick_rule(rules: list, stichwort: str):
    """Die spezifischste passende Regel; bei Gleichstand die, die in der Liste weiter oben steht."""
    best, best_score = None, None
    for rule in rules:
        score = match_score(rule["pattern"], stichwort)
        if score is not None and (best_score is None or score > best_score):
            best, best_score = rule, score
    return best
