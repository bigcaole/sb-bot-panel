#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

REQUIRE_DOCS=("README.md" "docs/零基础部署-测试-使用-排障手册.md")
BASE_REF=""
CI_AUTO=0

msg() { echo -e "\033[1;32m[信息]\033[0m $*"; }
warn() { echo -e "\033[1;33m[警告]\033[0m $*"; }
err() { echo -e "\033[1;31m[错误]\033[0m $*" >&2; }

usage() {
  cat <<'EOF'
用法：
  bash scripts/admin/check_docs_sync.sh
  bash scripts/admin/check_docs_sync.sh --base-ref origin/main
  bash scripts/admin/check_docs_sync.sh --ci-auto

说明：
  - 当改动包含“功能代码”时，要求同时更新：
    1) README.md
    2) docs/零基础部署-测试-使用-排障手册.md
  - 仅改测试/文档/数据文件时，不强制要求更新上述文档。
EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --base-ref)
        [[ $# -ge 2 ]] || { err "--base-ref 需要参数"; exit 2; }
        BASE_REF="$2"
        shift 2
        ;;
      --ci-auto)
        CI_AUTO=1
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        err "未知参数: $1"
        usage
        exit 2
        ;;
    esac
  done
}

ensure_repo() {
  if ! git -C "$PROJECT_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    warn "当前目录不是 git 仓库，跳过文档同步检查。"
    exit 0
  fi
}

resolve_base_ref() {
  if [[ -n "$BASE_REF" ]]; then
    echo "$BASE_REF"
    return
  fi

  if (( CI_AUTO == 1 )); then
    if [[ -n "${GITHUB_BASE_REF:-}" ]]; then
      echo "origin/${GITHUB_BASE_REF}"
      return
    fi
    if git -C "$PROJECT_DIR" rev-parse --verify HEAD~1 >/dev/null 2>&1; then
      echo "HEAD~1"
      return
    fi
    echo ""
    return
  fi

  if git -C "$PROJECT_DIR" rev-parse --verify origin/main >/dev/null 2>&1; then
    echo "origin/main"
    return
  fi
  if git -C "$PROJECT_DIR" rev-parse --verify HEAD~1 >/dev/null 2>&1; then
    echo "HEAD~1"
    return
  fi
  echo ""
}

collect_changed_files() {
  local base_ref="$1"
  local -a files=()
  local f

  if [[ -z "$base_ref" ]]; then
    :
  elif [[ "$base_ref" == "HEAD~1" ]]; then
    while IFS= read -r f; do
      [[ -n "$f" ]] && files+=("$f")
    done < <(git -C "$PROJECT_DIR" diff --name-only HEAD~1..HEAD)
  elif ! git -C "$PROJECT_DIR" rev-parse --verify "$base_ref" >/dev/null 2>&1; then
    warn "基线 ${base_ref} 不存在，回退到 HEAD~1。"
    if git -C "$PROJECT_DIR" rev-parse --verify HEAD~1 >/dev/null 2>&1; then
      while IFS= read -r f; do
        [[ -n "$f" ]] && files+=("$f")
      done < <(git -C "$PROJECT_DIR" diff --name-only HEAD~1..HEAD)
    fi
  else
    local merge_base
    merge_base="$(git -C "$PROJECT_DIR" merge-base "$base_ref" HEAD)"
    while IFS= read -r f; do
      [[ -n "$f" ]] && files+=("$f")
    done < <(git -C "$PROJECT_DIR" diff --name-only "${merge_base}"..HEAD)
  fi

  while IFS= read -r f; do
    [[ -n "$f" ]] && files+=("$f")
  done < <(git -C "$PROJECT_DIR" diff --name-only)

  while IFS= read -r f; do
    [[ -n "$f" ]] && files+=("$f")
  done < <(git -C "$PROJECT_DIR" diff --name-only --cached)

  printf '%s\n' "${files[@]}" | awk 'NF {if (!seen[$0]++) print $0}'
}

is_doc_required_file() {
  local f="$1"
  for item in "${REQUIRE_DOCS[@]}"; do
    if [[ "$f" == "$item" ]]; then
      return 0
    fi
  done
  return 1
}

is_functional_change_file() {
  local f="$1"

  if [[ "$f" == README.md || "$f" == docs/* ]]; then
    return 1
  fi
  if [[ "$f" == data/* || "$f" == *.db || "$f" == *.db-wal || "$f" == *.db-shm ]]; then
    return 1
  fi
  if [[ "$f" == tests/* ]]; then
    return 1
  fi

  case "$f" in
    controller/*|bot/*|agent/*|scripts/*|requirements.txt)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

main() {
  parse_args "$@"
  ensure_repo

  local base_ref
  base_ref="$(resolve_base_ref)"

  local -a changed_files=()
  local line
  while IFS= read -r line; do
    [[ -n "$line" ]] || continue
    changed_files+=("$line")
  done < <(collect_changed_files "$base_ref")

  if (( ${#changed_files[@]} == 0 )); then
    msg "未检测到可比较变更，跳过文档同步检查。"
    exit 0
  fi

  local functional_changed=0
  local docs_changed=0
  local -a functional_files=()
  local -a missing_docs=()

  for line in "${changed_files[@]}"; do
    if is_functional_change_file "$line"; then
      functional_changed=1
      functional_files+=("$line")
    fi
  done

  if (( functional_changed == 0 )); then
    msg "本次变更不含功能代码，文档同步检查通过。"
    exit 0
  fi

  for line in "${changed_files[@]}"; do
    if is_doc_required_file "$line"; then
      docs_changed=1
      break
    fi
  done

  if (( docs_changed == 1 )); then
    msg "检测到功能变更且文档已更新，检查通过。"
    exit 0
  fi

  missing_docs=("${REQUIRE_DOCS[@]}")
  err "检测到功能变更，但未同步 README/小白教程。"
  echo "功能变更文件："
  printf '  - %s\n' "${functional_files[@]}"
  echo "请至少更新以下文档："
  printf '  - %s\n' "${missing_docs[@]}"
  exit 20
}

main "$@"
