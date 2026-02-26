import ast
import re
import unittest
from pathlib import Path
from typing import List, Set


BOT_FILE = Path(__file__).resolve().parents[1] / "bot" / "bot.py"


def _joinedstr_to_template(node: ast.JoinedStr) -> str:
    parts: List[str] = []
    for item in node.values:
        if isinstance(item, ast.Constant) and isinstance(item.value, str):
            parts.append(item.value)
        else:
            parts.append("{X}")
    return "".join(parts)


def _literal_or_template(node: ast.AST) -> str:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return _joinedstr_to_template(node)
    return ""


def _sample_from_template(value: str) -> str:
    return re.sub(r"\{[^{}]*\}", "X", value)


def _regex_literal_prefix(pattern: str) -> str:
    text = pattern[1:] if pattern.startswith("^") else pattern
    out: List[str] = []
    meta = set(".^$*+?{}[]|()\\")
    for ch in text:
        if ch in meta:
            break
        out.append(ch)
    return "".join(out)


def _extract_callback_emitters(tree: ast.AST) -> Set[str]:
    callbacks: Set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Name) and node.func.id == "InlineKeyboardButton":
            for kw in node.keywords:
                if kw.arg == "callback_data":
                    token = _literal_or_template(kw.value)
                    if token:
                        callbacks.add(token)
        if isinstance(node.func, ast.Name) and node.func.id == "build_back_keyboard":
            if node.args:
                token = _literal_or_template(node.args[0])
                if token:
                    callbacks.add(token)
    return callbacks


def _extract_handle_callback_coverage(tree: ast.AST) -> (Set[str], Set[str]):
    exact: Set[str] = set()
    startswith: Set[str] = set()
    handle_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "handle_callback":
            handle_node = node
            break
    if handle_node is None:
        return exact, startswith

    for node in ast.walk(handle_node):
        if isinstance(node, ast.Compare):
            if (
                isinstance(node.left, ast.Name)
                and node.left.id == "callback_data"
                and len(node.ops) == 1
                and isinstance(node.ops[0], ast.Eq)
                and len(node.comparators) == 1
                and isinstance(node.comparators[0], ast.Constant)
                and isinstance(node.comparators[0].value, str)
            ):
                exact.add(node.comparators[0].value)
        if isinstance(node, ast.Call):
            if (
                isinstance(node.func, ast.Attribute)
                and node.func.attr == "startswith"
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "callback_data"
                and node.args
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, str)
            ):
                startswith.add(node.args[0].value)
    return exact, startswith


def _extract_callback_patterns(tree: ast.AST) -> List[re.Pattern]:
    patterns: List[re.Pattern] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not (isinstance(node.func, ast.Name) and node.func.id == "CallbackQueryHandler"):
            continue
        for kw in node.keywords:
            if kw.arg != "pattern":
                continue
            pattern = _literal_or_template(kw.value)
            if not pattern:
                continue
            patterns.append(re.compile(pattern))
    return patterns


class BotCallbackCoverageTestCase(unittest.TestCase):
    def test_no_orphan_callbacks(self) -> None:
        source = BOT_FILE.read_text(encoding="utf-8")
        tree = ast.parse(source)

        emitted = _extract_callback_emitters(tree)
        exact, startswith = _extract_handle_callback_coverage(tree)
        patterns = _extract_callback_patterns(tree)

        orphan: List[str] = []
        for token in sorted(emitted):
            sample = _sample_from_template(token)
            covered = sample in exact or any(sample.startswith(prefix) for prefix in startswith)
            if not covered:
                covered = any(pattern.fullmatch(sample) for pattern in patterns)
            if not covered:
                for pattern in patterns:
                    literal_prefix = _regex_literal_prefix(pattern.pattern)
                    if literal_prefix and sample.startswith(literal_prefix):
                        covered = True
                        break
            if not covered:
                orphan.append(token)

        self.assertEqual([], orphan, msg="orphan callbacks: {0}".format(orphan))


if __name__ == "__main__":
    unittest.main()
