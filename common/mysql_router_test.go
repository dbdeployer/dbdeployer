// DBDeployer - The MySQL Sandbox
// Copyright © 2025-2026 Roberto Garcia de Bem
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"os/exec"
	"testing"

	"github.com/dbdeployer/dbdeployer/globals"
)

func TestResolveMysqlRouterExecutableEmpty(t *testing.T) {
	exe, err := ResolveMysqlRouterExecutable("", "/opt/mysql", "8.0.27")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exe != "" {
		t.Fatalf("expected empty path, got %q", exe)
	}
}

func TestResolveMysqlRouterExecutableExplicitMissing(t *testing.T) {
	_, err := ResolveMysqlRouterExecutable("/nonexistent/router/install", "/opt/mysql", "8.0.27")
	if err == nil {
		t.Fatal("expected error for missing explicit path")
	}
}

func TestResolveMysqlRouterExecutableAutoFailsWithoutSiblingOrPath(t *testing.T) {
	if _, err := exec.LookPath(globals.FnMysqlrouter); err == nil {
		t.Skip("mysqlrouter found on PATH; skip negative test")
	}
	_, err := ResolveMysqlRouterExecutable(globals.MysqlRouterAutoResolveSentinel, "/tmp/dbdeployer-not-a-real-mysql-sb-root", "0.0.99")
	if err == nil {
		t.Fatal("expected error when sibling and PATH mysqlrouter unavailable")
	}
}

func TestSandboxRouterBinaryRoot(t *testing.T) {
	got := SandboxRouterBinaryRoot("/home/user/opt/mysql")
	want := "/home/user/opt/mysql-router"
	if got != want {
		t.Fatalf("SandboxRouterBinaryRoot: got %q want %q", got, want)
	}
}
