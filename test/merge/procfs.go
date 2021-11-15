package main

// Author Valentin Kuznetsov <vkuznet [AT] gmail {DOT] com >

import (
	"log"
	"os"

	"github.com/prometheus/procfs"
)

// ProcFS represents prometheus profcs metrics
type ProcFS struct {
	CpuTotal float64
	Vsize    float64
	Rss      float64
	OpenFDs  float64
	MaxFDs   float64
	MaxVsize float64
}

// ProcFSMetrics returns procfs (prometheus) metrics
func ProcFSMetrics() ProcFS {
	// get stats about given process
	var cpuTotal, vsize, rss, openFDs, maxFDs, maxVsize float64
	if proc, err := procfs.NewProc(os.Getpid()); err == nil {
		if stat, err := proc.Stat(); err == nil {
			// CPUTime returns the total CPU user and system time in seconds.
			cpuTotal = float64(stat.CPUTime())
			vsize = float64(stat.VirtualMemory())
			rss = float64(stat.ResidentMemory())
		}
		if fds, err := proc.FileDescriptorsLen(); err == nil {
			openFDs = float64(fds)
		}
		if limits, err := proc.NewLimits(); err == nil {
			maxFDs = float64(limits.OpenFiles)
			maxVsize = float64(limits.AddressSpace)
		}
	} else {
		log.Println("unable to get procfs info", err)
	}
	metrics := ProcFS{
		CpuTotal: cpuTotal,
		Vsize:    vsize,
		Rss:      rss,
		OpenFDs:  openFDs,
		MaxFDs:   maxFDs,
		MaxVsize: maxVsize,
	}
	return metrics
}
