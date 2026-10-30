//go:build linux

package engine

import "syscall"

// getSysProcAttr возвращает настройки процесса для Linux
// Pdeathsig: ядро пошлёт SIGTERM дочернему при смерти родителя (краш, OOM, kill -9)
// Setpgid: создаём отдельную process group
func getSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{
		Setpgid:   true,            // Новая process group
		Pdeathsig: syscall.SIGTERM, // Убить при смерти родителя
	}
}
