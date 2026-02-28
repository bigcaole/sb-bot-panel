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


def extract_function_calls(tree: ast.AST, function_name: str) -> Set[str]:
    calls: Set[str] = set()
    target = None
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == function_name:
            target = node
            break
    if target is None:
        return calls
    for node in ast.walk(target):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            calls.add(node.func.id)
    return calls


def extract_clear_all_wizard_pop_keys(tree: ast.AST) -> Set[str]:
    keys: Set[str] = set()
    target = None
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "clear_all_wizard_state":
            target = node
            break
    if target is None:
        return keys
    for node in ast.walk(target):
        if not isinstance(node, ast.Call):
            continue
        if not (isinstance(node.func, ast.Attribute) and node.func.attr == "pop"):
            continue
        if not node.args:
            continue
        first_arg = node.args[0]
        if isinstance(first_arg, ast.Name):
            keys.add(first_arg.id)
    return keys


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
            "action:maintain_ops_audit",
            "action:maintain_smoke",
            "action:maintain_sync_node_time",
            "action:maintain_security_events",
            "action:maintain_sub_policy",
            "action:maintain_log_archive",
            "action:backup_stop",
            "backup:stop:confirm",
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

    def test_start_and_menu_clear_state_before_render(self) -> None:
        start_calls = extract_function_calls(self._tree, "start")
        menu_calls = extract_function_calls(self._tree, "menu")
        self.assertIn("clear_all_wizard_state", start_calls)
        self.assertIn("clear_all_wizard_state", menu_calls)

    def test_clear_state_covers_node_ops_config(self) -> None:
        pop_keys = extract_clear_all_wizard_pop_keys(self._tree)
        self.assertIn("NODE_OPS_CONFIG_KEY", pop_keys)


if __name__ == "__main__":
    unittest.main()
