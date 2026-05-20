// DBDeployer - The MySQL Sandbox
// Copyright © 2025-2026 Roberto Garcia de Bem
//
// Licensed under the Apache License, Version 2.0 (the "License");

package common

import "testing"

func TestParseMysqldVersionBanner(t *testing.T) {
	banner := "/home/user/opt/mysql/0.0.3/bin/mysqld  Ver 8.4.8-villagesql-0.0.3-dev-78e24815 for Linux on x86_64 (MySQL Community Server - GPL)"
	m := reMysqldVersion.FindStringSubmatch(banner)
	if len(m) < 2 {
		t.Fatalf("expected version match in %q", banner)
	}
	if m[1] != "8.4.8" {
		t.Fatalf("expected 8.4.8, got %q", m[1])
	}
}
