// DBDeployer - The MySQL Sandbox
// Copyright © 2025-2026 Roberto Garcia de Bem

package common

import "testing"

func TestResolveMysqlshExecutableInstallRoot(t *testing.T) {
	root := "/home/robertodebem/opt/mysql/mysqlsh/8.0.42"
	binary := root + "/bin/mysqlsh"
	if !ExecExists(binary) {
		t.Skipf("test requires %s", binary)
	}
	got := ResolveMysqlshExecutable(root, "")
	if got != binary {
		t.Fatalf("ResolveMysqlshExecutable(%q): got %q want %q", root, got, binary)
	}
}

func TestResolveMysqlshExecutableDefault(t *testing.T) {
	got := ResolveMysqlshExecutable("mysqlsh", "")
	if got == "" {
		t.Fatal("expected non-empty fallback")
	}
}
