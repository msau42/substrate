//go:build linux && amd64

// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

type pmemMapRange struct {
	start uint64
	end   uint64
}

func findDurablePmemRanges(pid int, durableDir string) ([]pmemMapRange, error) {
	if pid <= 0 || durableDir == "" {
		return nil, nil
	}
	prefix := filepath.Clean(durableDir) + string(filepath.Separator)
	f, err := os.Open(fmt.Sprintf("/proc/%d/maps", pid))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var ranges []pmemMapRange
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 6 {
			continue
		}
		if fields[1] != "rw-s" {
			continue
		}
		path := fields[len(fields)-1]
		if !strings.HasPrefix(path, prefix) || !strings.HasSuffix(path, ".ext4") {
			continue
		}
		addrParts := strings.SplitN(fields[0], "-", 2)
		if len(addrParts) != 2 {
			continue
		}
		start, err1 := strconv.ParseUint(addrParts[0], 16, 64)
		end, err2 := strconv.ParseUint(addrParts[1], 16, 64)
		if err1 != nil || err2 != nil || end <= start {
			continue
		}
		ranges = append(ranges, pmemMapRange{start: start, end: end})
	}
	return ranges, sc.Err()
}

func waitPtraceStop(pid int) error {
	var ws unix.WaitStatus
	for {
		_, err := unix.Wait4(pid, &ws, 0, nil)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		return err
	}
}

// enablePmemHugepages sets MADV_HUGEPAGE on all shared writable virtio-pmem
// (.ext4) mappings under durableDir inside the target cloud-hypervisor process.
//
// Cloud Hypervisor sets MADV_HUGEPAGE on MemoryZone mappings (thp=on by
// default) so guest RAM is backed by 2 MiB PMDs (FilePmdMapped) and 2 MiB KVM
// EPT entries (pages_2m), but it omits MADV_HUGEPAGE on PmemConfig mappings.
// Setting MADV_HUGEPAGE on the Pmem VMAs right after vm.create / vm.restore
// (while the VMM's API thread is idle before BootVM / Resume) allows Linux
// 6.12's filemap_fault / filemap_map_pages and KVM's EPT fault handler to use
// 2 MiB order-9 folios and 2 MiB EPT pages for virtio-pmem DAX volumes.
func enablePmemHugepages(pid int, durableDir string) error {
	ranges, err := findDurablePmemRanges(pid, durableDir)
	if err != nil || len(ranges) == 0 {
		return err
	}

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	if _, _, errno := unix.Syscall6(unix.SYS_PTRACE, unix.PTRACE_SEIZE, uintptr(pid), 0, 0, 0, 0); errno != 0 {
		return fmt.Errorf("ptrace seize %d: %w", pid, errno)
	}
	defer func() {
		_ = unix.PtraceDetach(pid)
	}()

	if _, _, errno := unix.Syscall6(unix.SYS_PTRACE, unix.PTRACE_INTERRUPT, uintptr(pid), 0, 0, 0, 0); errno != 0 {
		return fmt.Errorf("ptrace interrupt %d: %w", pid, errno)
	}
	if err := waitPtraceStop(pid); err != nil {
		return fmt.Errorf("waiting for ptrace interrupt on %d: %w", pid, err)
	}

	var savedRegs unix.PtraceRegs
	if err := unix.PtraceGetRegs(pid, &savedRegs); err != nil {
		return fmt.Errorf("ptrace getregs %d: %w", pid, err)
	}

	syscallRip := savedRegs.Rip
	needPoke := true
	if savedRegs.Rip >= 2 {
		var prevInsn [8]byte
		if _, err := unix.PtracePeekText(pid, uintptr(savedRegs.Rip-2), prevInsn[:]); err == nil && prevInsn[0] == 0x0f && prevInsn[1] == 0x05 {
			syscallRip = savedRegs.Rip - 2
			needPoke = false
		}
	}

	var origInsn [8]byte
	if needPoke {
		if _, err := unix.PtracePeekText(pid, uintptr(syscallRip), origInsn[:]); err != nil {
			return fmt.Errorf("ptrace peektext %d: %w", pid, err)
		}
		patched := origInsn
		patched[0] = 0x0f
		patched[1] = 0x05
		if _, err := unix.PtracePokeText(pid, uintptr(syscallRip), patched[:]); err != nil {
			return fmt.Errorf("ptrace poketext %d: %w", pid, err)
		}
		defer func() {
			_, _ = unix.PtracePokeText(pid, uintptr(syscallRip), origInsn[:])
		}()
	}

	for _, r := range ranges {
		regs := savedRegs
		regs.Rip = syscallRip
		regs.Rax = unix.SYS_MADVISE
		regs.Rdi = r.start
		regs.Rsi = r.end - r.start
		regs.Rdx = unix.MADV_HUGEPAGE
		if err := unix.PtraceSetRegs(pid, &regs); err != nil {
			return fmt.Errorf("ptrace setregs madvise %d: %w", pid, err)
		}
		if err := unix.PtraceSingleStep(pid); err != nil {
			return fmt.Errorf("ptrace singlestep madvise %d: %w", pid, err)
		}
		if err := waitPtraceStop(pid); err != nil {
			return fmt.Errorf("waiting for ptrace singlestep on %d: %w", pid, err)
		}
	}

	restoreRegs := savedRegs
	if int64(restoreRegs.Orig_rax) >= 0 {
		raxSigned := int64(restoreRegs.Rax)
		if raxSigned == -int64(unix.EINTR) || (raxSigned <= -512 && raxSigned >= -516) {
			restoreRegs.Rax = restoreRegs.Orig_rax
			restoreRegs.Rip -= 2
		}
	}
	if err := unix.PtraceSetRegs(pid, &restoreRegs); err != nil {
		return fmt.Errorf("ptrace restore regs %d: %w", pid, err)
	}
	return nil
}
