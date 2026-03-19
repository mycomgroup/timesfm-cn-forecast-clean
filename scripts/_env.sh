#!/bin/bash
set -euo pipefail

if [ -n "${BASH_SOURCE[0]:-}" ]; then
  _CODEX_ENV_SOURCE="${BASH_SOURCE[0]}"
elif [ -n "${ZSH_VERSION:-}" ]; then
  _CODEX_ENV_SOURCE="$(eval 'printf "%s" "${(%):-%N}"')"
else
  _CODEX_ENV_SOURCE="$0"
fi

SCRIPT_DIR="$(cd "$(dirname "${_CODEX_ENV_SOURCE}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
export PROJECT_ROOT
unset _CODEX_ENV_SOURCE

case ":${PYTHONPATH:-}:" in
  *":${PROJECT_ROOT}/src:"*) ;;
  *) export PYTHONPATH="${PROJECT_ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" ;;
esac

_python_has_modules() {
  local candidate="$1"
  shift || true
  if [ ! -x "${candidate}" ]; then
    return 1
  fi
  "${candidate}" - "$@" <<'PY' >/dev/null 2>&1
import importlib.util
import sys

mods = sys.argv[1:]
missing = [mod for mod in mods if importlib.util.find_spec(mod) is None]
raise SystemExit(1 if missing else 0)
PY
}

setup_project_env() {
  local required_modules=("$@")
  local -a candidates=()

  if [ -n "${PYTHON_BIN:-}" ]; then
    candidates+=("${PYTHON_BIN}")
  fi
  if command -v python >/dev/null 2>&1; then
    candidates+=("$(command -v python)")
  fi
  if command -v python3 >/dev/null 2>&1; then
    candidates+=("$(command -v python3)")
  fi
  if [ -x "/opt/anaconda3/bin/python" ]; then
    candidates+=("/opt/anaconda3/bin/python")
  fi

  local candidate
  local seen=":"
  for candidate in "${candidates[@]}"; do
    case "${seen}" in
      *:"${candidate}":*) continue ;;
    esac
    seen="${seen}${candidate}:"
    if [ "${#required_modules[@]}" -eq 0 ] || _python_has_modules "${candidate}" "${required_modules[@]}"; then
      export PYTHON_BIN="${candidate}"
      return 0
    fi
  done

  echo "Unable to find a usable Python interpreter. Tried: ${candidates[*]}" >&2
  return 1
}
