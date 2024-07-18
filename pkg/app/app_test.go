package app

import (
	"github.com/stretchr/testify/require"
	"net"
	"testing"
	"time"
)

var listenAddr = "127.0.0.1:9999"

func TestAppStartStop(t *testing.T) {
	feta := NewFetaDB()
	err := feta.Start(listenAddr, "memory")
	require.Nil(t, err)

	timeout := 5 * time.Second
	conn, err := net.DialTimeout("tcp", listenAddr, timeout)
	require.Nil(t, err)
	require.NotNil(t, conn)
	defer conn.Close()

	err = feta.Stop()
	require.Nil(t, err)
}
