package taloscluster

import (
	"fmt"
	"strconv"
	"strings"
)

// parseCPUList parses a Linux kernel cpulist range string — the format
// /sys/devices/system/cpu/possible uses, e.g. "0-3" or "0-3,8-11" — into
// the individual CPU indices it names. See Linux's own
// Documentation/admin-guide/cputopology.rst for the format.
func parseCPUList(cpuList string) ([]uint32, error) {
	cpuList = strings.TrimSpace(cpuList)
	if cpuList == "" {
		return nil, nil
	}

	var cpus []uint32

	for group := range strings.SplitSeq(cpuList, ",") {
		lowerBound, upperBound, isRange := strings.Cut(group, "-")

		loVal, err := parseSysfsUint(lowerBound)
		if err != nil {
			return nil, fmt.Errorf("failed to parse cpu list group %q: %w", group, err)
		}

		hiVal := loVal

		if isRange {
			hiVal, err = parseSysfsUint(upperBound)
			if err != nil {
				return nil, fmt.Errorf("failed to parse cpu list group %q: %w", group, err)
			}
		}

		for cpu := loVal; cpu <= hiVal; cpu++ {
			cpus = append(cpus, cpu)
		}
	}

	return cpus, nil
}

// parseSysfsUint parses a single unsigned integer from a sysfs file's
// contents (e.g. topology/core_id, cpufreq/cpuinfo_max_freq, or one bound
// of a cpulist group), tolerating surrounding whitespace/newlines.
func parseSysfsUint(value string) (uint32, error) {
	// bitSize 32 makes ParseUint itself reject anything that wouldn't fit
	// back into a uint32, so the conversion below is always lossless.
	val, err := strconv.ParseUint(strings.TrimSpace(value), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("failed to parse sysfs value %q: %w", value, err)
	}

	return uint32(val), nil
}
