package utils

import (
	"log"
	"os"

	"github.com/prometheus/procfs"
)

// ProcFS represents prometheus profcs metrics
type ProcFS struct {
	CpuTotal      float64
	Vsize         float64
	Rss           float64
	OpenFDs       float64
	MaxFDs        float64
	MaxVsize      float64
	UserCPUs      []float64
	SystemCPUs    []float64
	SumUserCPUs   float64
	SumSystemCPUs float64
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

	// collect info from /proc/stat
	fs, err := procfs.NewFS("/proc")
	if err != nil {
		log.Println("unable to get /proc info", err)
	} else {
		stats, err := fs.Stat()
		if err != nil {
			log.Println("unable to get /proc/stat info", err)
		} else {
			var userCpus, sysCpus []float64
			for _, v := range stats.CPU {
				userCpus = append(userCpus, v.User)
				sysCpus = append(sysCpus, v.User)
			}
			metrics.UserCPUs = userCpus
			metrics.SystemCPUs = sysCpus
			metrics.SumUserCPUs = stats.CPUTotal.User
			metrics.SumSystemCPUs = stats.CPUTotal.System
		}
	}
	return metrics
}
