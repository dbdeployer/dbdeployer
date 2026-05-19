// DBDeployer - The MySQL Sandbox
// Copyright © 2025-2026 Roberto Garcia de Bem
//
// Licensed under the Apache License, Version 2.0 (the "License");

package common

import (
	"os/exec"
	"path"
)

// ResolveMysqlshExecutable returns a mysqlsh binary path.
// mysqlshPath may be empty, the literal "mysqlsh", an install root, or a path to the binary.
// Falls back to <serverBasedir>/bin/mysqlsh, then PATH, then the literal "mysqlsh".
func ResolveMysqlshExecutable(mysqlshPath, serverBasedir string) string {
	var candidates []string
	if mysqlshPath != "" && mysqlshPath != "mysqlsh" {
		candidates = append(candidates, mysqlshPath, path.Join(mysqlshPath, "bin", "mysqlsh"))
	}
	if serverBasedir != "" {
		candidates = append(candidates, path.Join(serverBasedir, "bin", "mysqlsh"))
	}
	for _, c := range candidates {
		if ExecExists(c) {
			return c
		}
	}
	if p, err := exec.LookPath("mysqlsh"); err == nil {
		return p
	}
	return "mysqlsh"
}
