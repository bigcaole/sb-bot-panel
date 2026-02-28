import re


SENSITIVE_TEXT_MASK_PATTERNS = (
    re.compile(r"(?i)(authorization\s*:\s*bearer\s+)([A-Za-z0-9._\-~+/=]{6,})()"),
    re.compile(r"(?i)(\bbearer\s+)([A-Za-z0-9._\-~+/=]{12,})()"),
    re.compile(
        r'(?i)("?(?:auth[_-]?token|token|password|secret|api[_-]?key|private[_-]?key)"?\s*[:=]\s*")([^"]*)(")'
    ),
    re.compile(
        r'(?i)("?(?:auth[_-]?token|token|password|secret|api[_-]?key|private[_-]?key)"?\s*[:=]\s*)([^",\s]+)()'
    ),
    re.compile(r'(?i)("?authorization"?\s*[:=]\s*")(?!Bearer\s)([^"]*)(")'),
    re.compile(r'(?i)("?authorization"?\s*[:=]\s*)(?!Bearer\s)([^",\s]+)()'),
)


def mask_sensitive_text(value: object) -> str:
    masked = str(value or "")
    for pattern in SENSITIVE_TEXT_MASK_PATTERNS:
        masked = pattern.sub(r"\1***\3", masked)
    return masked
