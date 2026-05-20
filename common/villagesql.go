// DBDeployer - The MySQL Sandbox
// Copyright © 2025-2026 Roberto Garcia de Bem
//
// Licensed under the Apache License, Version 2.0 (the "License");

package common

import (
	"fmt"
	"os/exec"
	"path"
	"regexp"
)

var reMysqldVersion = regexp.MustCompile(`Ver\s+(\d+\.\d+\.\d+)`)

// ResolveVillageSQLUpstreamVersion runs <basedir>/bin/mysqld --version and
// returns the leading MySQL version (e.g. "8.4.8" from
// "Ver 8.4.8-villagesql-0.0.3-dev-78e24815").
func ResolveVillageSQLUpstreamVersion(basedir string) (string, error) {
	mysqld := path.Join(basedir, "bin", "mysqld")
	out, err := exec.Command(mysqld, "--version").CombinedOutput() // #nosec G204
	if err != nil {
		return "", fmt.Errorf("running %s --version: %w (%s)", mysqld, err, out)
	}
	m := reMysqldVersion.FindStringSubmatch(string(out))
	if len(m) < 2 {
		return "", fmt.Errorf("could not parse MySQL version from: %s", out)
	}
	return m[1], nil
}
