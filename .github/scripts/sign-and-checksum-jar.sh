#!/usr/bin/env bash
# Create a detached GPG ASCII signature (.asc) and Maven-style .sha1 / .sha256
# sidecars for a release JAR and for the .asc.
#
# Usage:
#   ./.github/scripts/sign-and-checksum-jar.sh path/to/artifact.jar [gpg-key-id-or-email]
#
# Optional env:
#   GPG_KEY_ID   Same as optional second argument (signing subkey or email).
#   GPG_PASS     Passphrase for non-interactive CI signing.
#
# Requires: gpg, shasum (macOS) or sha256sum/sha1sum (Linux).

set -euo pipefail

usage() {
  echo "usage: $0 <path-to.jar> [gpg-key-id-or-email]" >&2
  echo "  env GPG_KEY_ID can be used instead of the second argument." >&2
  exit 1
}

[[ "${1:-}" ]] || usage
jar=$1
key_id="${2:-${GPG_KEY_ID:-}}"

[[ -f "$jar" ]] || {
  echo "error: file not found: $jar" >&2
  exit 1
}

case "$jar" in
  *.jar) ;;
  *)
    echo "error: expected a .jar path, got: $jar" >&2
    exit 1
    ;;
esac

dir=$(cd "$(dirname -- "$jar")" && pwd)
base=$(basename -- "$jar")
cd "$dir"

asc="${base}.asc"

gpg_opts=(--batch --armor --detach-sign --output "$asc")
if [[ -n "${GPG_PASS:-}" ]]; then
  gpg_opts+=(--pinentry-mode loopback --passphrase "$GPG_PASS")
fi
if [[ -n "$key_id" ]]; then
  gpg_opts+=(-u "$key_id")
fi

echo "Signing: $base -> $asc"
gpg "${gpg_opts[@]}" -- "$base"

checksum_one() {
  local f=$1
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 1 "$f" | awk '{print $1}' >"${f}.sha1"
    shasum -a 256 "$f" | awk '{print $1}' >"${f}.sha256"
  elif command -v sha256sum >/dev/null 2>&1; then
    sha1sum "$f" | awk '{print $1}' >"${f}.sha1"
    sha256sum "$f" | awk '{print $1}' >"${f}.sha256"
  else
    echo "error: need shasum (macOS) or sha256sum/sha1sum (Linux)" >&2
    exit 1
  fi
}

for f in "$base" "$asc"; do
  echo "Checksumming: $f"
  checksum_one "$f"
done

echo "Done. Created in $dir:"
ls -1 "${base}"* 2>/dev/null | sed "s|^|  |"
