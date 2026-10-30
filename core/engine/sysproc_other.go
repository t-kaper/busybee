//go:build !linux

package engine

import "syscall"

// getSysProcAttr возвращает настройки процесса для не-Linux систем
// Pdeathsig недоступен на macOS/Windows, поэтому только Setpgid
func getSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{
		Setpgid: true, // Новая process group (работает на macOS)
	}
}
