// DBDeployer - The MySQL Sandbox
// Copyright © 2025-2026 Roberto Garcia de Bem
//
// Licensed under the Apache License, Version 2.0 (the "License");

package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// flagValue returns the value of a string flag walking the command tree.
// Explicitly-set values (cmd or any ancestor) win over root defaults.
func flagValue(cmd *cobra.Command, name string) string {
	var fallback string
	for c := cmd; c != nil; c = c.Parent() {
		for _, set := range []*pflag.FlagSet{c.Flags(), c.PersistentFlags()} {
			f := set.Lookup(name)
			if f == nil {
				continue
			}
			if f.Changed {
				return f.Value.String()
			}
			fallback = f.Value.String()
		}
	}
	return fallback
}

// flagOrDefault returns the flag value if non-empty, else the default.
func flagOrDefault(cmd *cobra.Command, name, defaultValue string) string {
	if v := flagValue(cmd, name); v != "" {
		return v
	}
	return defaultValue
}
