#!/usr/bin/env bash
# Install the Go toolchain Cloud Agents share with CI.
#
# go.mod says `go 1.26`. actions/setup-go resolves that, the 1.26.x test
# matrix entry, the format job, and the simd job to the latest 1.26 patch.
# The simd pin is the same toolchain, so there is no second Go to install.
# The 1.27.x matrix entry is selected by setup-go in CI and is not a
# separate agent toolchain.
#
# Workflows do not install a linter. `make format` uses gofmt and the
# modernize tool declared in go.tools.mod. Race tests need a C compiler,
# which ubuntu-latest provides and this script installs when it is missing.
set -euo pipefail

GO_VERSION=1.26.11
# https://go.dev/dl/go1.26.11.linux-amd64.tar.gz
GO_SHA256_AMD64=9849f4e04ee35303714e34fbc3ac8ab44ea3e448a95aa182f91e3b9b79c710ec

if [[ "$(uname -m)" != "x86_64" ]]; then
  echo "this Go install supports linux/amd64 only (got $(uname -m))" >&2
  exit 1
fi

if ! command -v gcc >/dev/null 2>&1 || ! command -v make >/dev/null 2>&1; then
  sudo apt-get update
  sudo apt-get install -y --no-install-recommends build-essential ca-certificates curl make
fi

if ! { [[ -x /usr/local/go/bin/go ]] && /usr/local/go/bin/go version | grep -q "go${GO_VERSION} "; }; then
  tmp="$(mktemp)"
  trap 'rm -f "$tmp"' EXIT
  curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" -o "$tmp"
  echo "${GO_SHA256_AMD64}  ${tmp}" | sha256sum -c -
  sudo rm -rf /usr/local/go
  sudo tar -C /usr/local -xzf "$tmp"
  rm -f "$tmp"
  trap - EXIT
fi

sudo ln -sfn /usr/local/go/bin/go /usr/local/bin/go
sudo ln -sfn /usr/local/go/bin/gofmt /usr/local/bin/gofmt

# actions/setup-go exports GOTOOLCHAIN=local so the go command cannot
# download a different toolchain. Login shells (install, start) source this.
sudo tee /etc/profile.d/go-toolchain.sh >/dev/null <<'EOF'
export GOTOOLCHAIN=local
EOF
export GOTOOLCHAIN=local

cd "$(dirname "$0")/.."
go version
go mod download
go mod download -modfile go.tools.mod
