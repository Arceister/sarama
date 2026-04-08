//go:build !functional

package sarama

import (
	"runtime"
	"testing"
)

func BenchmarkZstdCompressAllocs(b *testing.B) {
	params := ZstdEncoderParams{Level: CompressionLevelDefault}
	// Typical Kafka message payload: 2KB
	payload := make([]byte, 2048)
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = zstdCompress(params, nil, payload)
	}
}

func BenchmarkZstdMemoryConsumption(b *testing.B) {
	params := ZstdEncoderParams{Level: 9}
	buf := make([]byte, 1024*1024)
	for i := 0; i < len(buf); i++ {
		buf[i] = byte((i / 256) + (i * 257))
	}

	cpus := 96

	gomaxprocsBackup := runtime.GOMAXPROCS(cpus)
	b.ReportAllocs()
	for b.Loop() {
		for j := 0; j < 2*cpus; j++ {
			_, _ = zstdCompress(params, nil, buf)
		}
		// drain the buffered encoder
		getZstdEncoder(params)
		// previously this would be achieved with
		// zstdEncMap.Delete(params)
	}
	runtime.GOMAXPROCS(gomaxprocsBackup)
}
