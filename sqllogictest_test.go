package main

import (
	"fetadb/pkg/app"
	"github.com/stretchr/testify/require"
	"log"
	"os"
	"os/exec"
	"path"
	"testing"
)

func TestRunner(t *testing.T) {
	homeDir, err := os.UserHomeDir()
	require.Nil(t, err)

	feta := app.NewFetaDB()
	err = feta.Start("127.0.0.1:9999", "memory")
	require.Nil(t, err)

	runner := path.Join(homeDir, ".cargo", "bin", "sqllogictest")

	cmd := exec.Command(runner, "-h", "127.0.0.1", "-p", "9999", "test/**/*.slt")
	output, err := cmd.CombinedOutput()
	log.Println(string(output))
	require.Nil(t, err)
}
