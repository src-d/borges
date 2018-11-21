package gfapi

import (
	"syscall"
)

// getLastModification returns the modification time
func getLastModification(st *syscall.Stat_t) syscall.Timespec {
	return st.Mtim
}
