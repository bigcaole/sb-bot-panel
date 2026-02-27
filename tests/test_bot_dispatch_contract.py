import ast
import unittest
from pathlib import Path
from typing import Set, Tuple


BOT_FILE = Path(__file__).resolve().parents[1] / "bot" / "bot.py"


def extract_command_handlers(tree: ast.AST) -> Set[str]:
    commands: Set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not (isinstance(node.func, ast.Name) and node.func.id == "CommandHandler"):
            continue
        if not node.args:
            continue
        first_arg = node.args[0]
        if isinstance(first_arg, ast.Constant) and isinstance(first_arg.value, str):
            commands.add(first_arg.value)
    return commands


def extract_handle_callback_dispatch(tree: ast.AST) -> Tuple[Set[str], Set[str]]:
    exact: Set[str] = set()
    prefixes: Set[str] = set()
    handle_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "handle_callback":
            handle_node = node
            break
    if handle_node is None:
        return exact, prefixes

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
                prefixes.add(node.args[0].value)
    return exact, prefixes


class BotDispatchContractTestCase(unittest.TestCase):
    def setUp(self) -> None:
        source = BOT_FILE.read_text(encoding="utf-8")
        self._tree = ast.parse(source)

    def test_critical_command_handlers_exist(self) -> None:
        commands = extract_command_handlers(self._tree)
        for command in ("start", "menu", "cancel", "whoami"):
            self.assertIn(command, commands)

    def test_critical_callback_dispatch_exists(self) -> None:
        exact, prefixes = extract_handle_callback_dispatch(self._tree)

        required_exact = {
            "usernodes:manual_input",
            "wizard:cancel",
            "wizard:create_confirm",
            "wizard:nodes_create_confirm",
            "action:maintain_status",
            "action:maintain_smoke",
            "action:maintain_security_events",
            "action:maintain_sub_policy",
            "action:maintain_log_archive",
        }
        for token in required_exact:
            self.assertIn(token, exact)

        required_prefixes = {
            "maintain:subpolicy:",
            "maintain:logsdate:",
            "node:detail:",
            "sb:ba:",
        }
        for token in required_prefixes:
            self.assertIn(token, prefixes)


if __name__ == "__main__":
    unittest.main()
