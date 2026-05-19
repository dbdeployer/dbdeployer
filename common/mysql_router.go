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
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"

	"github.com/dbdeployer/dbdeployer/globals"
)

// SandboxRouterBinaryRoot is the sibling tree for MySQL Router (e.g. $SANDBOX_BINARY-router).
func SandboxRouterBinaryRoot(sandboxBinaryRoot string) string {
	base := filepath.Base(sandboxBinaryRoot)
	if base == "" || base == "." {
		return sandboxBinaryRoot + "-router"
	}
	return filepath.Join(filepath.Dir(sandboxBinaryRoot), base+"-router")
}

// ResolveMysqlRouterExecutable returns the mysqlrouter binary path.
// mysqlRouterPath empty: no router. AUTO: sibling ...-router/<version>/bin/mysqlrouter, then PATH.
// Otherwise: explicit install dir or path to the binary.
func ResolveMysqlRouterExecutable(mysqlRouterPath string, sandboxBinaryRoot string, deployVersion string) (string, error) {
	if mysqlRouterPath == "" {
		return "", nil
	}
	if mysqlRouterPath == globals.MysqlRouterAutoResolveSentinel {
		routerRoot := SandboxRouterBinaryRoot(sandboxBinaryRoot)
		candidates := []string{
			path.Join(routerRoot, deployVersion, "bin", globals.FnMysqlrouter),
			path.Join(routerRoot, deployVersion, globals.FnMysqlrouter),
		}
		for _, candidate := range candidates {
			if ExecExists(candidate) {
				return candidate, nil
			}
		}
		if onPath, err := exec.LookPath(globals.FnMysqlrouter); err == nil {
			return onPath, nil
		}
		return "", fmt.Errorf(
			"no MySQL Router (mysqlrouter) found: checked %s and PATH for %q",
			routerRoot, globals.FnMysqlrouter)
	}
	if ExecExists(mysqlRouterPath) {
		info, err := os.Stat(mysqlRouterPath)
		if err != nil {
			return "", err
		}
		if !info.IsDir() {
			return mysqlRouterPath, nil
		}
	}
	if mysqlRouterPath != "" {
		for _, candidate := range []string{
			path.Join(mysqlRouterPath, "bin", globals.FnMysqlrouter),
			path.Join(mysqlRouterPath, globals.FnMysqlrouter),
		} {
			if ExecExists(candidate) {
				return candidate, nil
			}
		}
	}
	return "", fmt.Errorf(
		"no MySQL Router (mysqlrouter) found at %s (expected %s or a path to the %s binary)",
		mysqlRouterPath, path.Join(mysqlRouterPath, "bin", globals.FnMysqlrouter), globals.FnMysqlrouter)
}
