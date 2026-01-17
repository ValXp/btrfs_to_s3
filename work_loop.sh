#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROMPT_FILE="${ROOT_DIR}/prompt.md"
STOP_FILE="${ROOT_DIR}/stop.md"

CODEX_CMD="${CODEX_CMD:-codex}"
CODEX_ARGS="${CODEX_ARGS:---dangerously-bypass-approvals-and-sandbox exec}"
CODEX_PROMPT_FLAG="${CODEX_PROMPT_FLAG:---prompt}"
CODEX_SLEEP_SECONDS="${CODEX_SLEEP_SECONDS:-1}"

if [[ ! -f "$PROMPT_FILE" ]]; then
  echo "prompt.md not found at ${PROMPT_FILE}" >&2
  exit 1
fi

if ! command -v "$CODEX_CMD" >/dev/null 2>&1; then
  echo "codex command not found: ${CODEX_CMD}" >&2
  exit 1
fi

while [[ ! -f "$STOP_FILE" ]]; do
  PROMPT_CONTENT="$(cat "$PROMPT_FILE")"
  read -r -a ARGS <<<"$CODEX_ARGS"
  if [[ "$CODEX_PROMPT_FLAG" == "-" ]]; then
    printf '%s' "$PROMPT_CONTENT" | "$CODEX_CMD" "${ARGS[@]}" </dev/null
  else
    "$CODEX_CMD" "${ARGS[@]}" "$CODEX_PROMPT_FLAG" "$PROMPT_CONTENT" </dev/null
  fi
  if [[ ! -f "$STOP_FILE" ]]; then
    sleep "$CODEX_SLEEP_SECONDS"
  fi
done
