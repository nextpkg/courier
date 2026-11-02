//go:build integration

package examples_test

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// loadResources reads cgroup v2 counters for the entire benchmark container.
// CPU usage is differenced per round; memory.peak is the container lifetime peak.
func loadResources() (map[string]uint64, float64, error) {
	data, err := os.ReadFile("/sys/fs/cgroup/cpu.stat")
	if err != nil {
		return nil, 0, err
	}
	values := map[string]uint64{}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		n, e := strconv.ParseUint(fields[1], 10, 64)
		if e != nil {
			return nil, 0, e
		}
		values[fields[0]] = n
	}
	for _, key := range []string{"usage_usec", "nr_throttled"} {
		if _, ok := values[key]; !ok {
			return nil, 0, fmt.Errorf("missing cgroup counter %s", key)
		}
	}
	limit, err := os.ReadFile("/sys/fs/cgroup/cpu.max")
	if err != nil {
		return nil, 0, err
	}
	fields := strings.Fields(string(limit))
	if len(fields) != 2 || fields[0] == "max" {
		return nil, 0, fmt.Errorf("CPU quota required for resource measurements")
	}
	quota, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return nil, 0, err
	}
	period, err := strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return nil, 0, err
	}
	if quota <= 0 || period <= 0 {
		return nil, 0, fmt.Errorf("invalid CPU quota")
	}
	peak, err := os.ReadFile("/sys/fs/cgroup/memory.peak")
	if err != nil {
		return nil, 0, err
	}
	values["memory_peak_bytes"], err = strconv.ParseUint(strings.TrimSpace(string(peak)), 10, 64)
	return values, quota / period, err
}
