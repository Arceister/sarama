package sarama

import (
	"sync"

	"github.com/klauspost/compress/zstd"
)

// zstdMaxBufferedEncoders maximum number of not-in-use zstd encoders
// If the pool of encoders is exhausted then new encoders will be created on the fly
const zstdMaxBufferedEncoders = 1

type ZstdEncoderParams struct {
	Level int
}
type ZstdDecoderParams struct {
}

var zstdDecMap sync.Map

var zstdAvailableEncoders sync.Map

var zstdDstPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 0, 4096)
		return &b
	},
}

func getZstdEncoderChannel(params ZstdEncoderParams) chan *zstd.Encoder {
	if c, ok := zstdAvailableEncoders.Load(params); ok {
		return c.(chan *zstd.Encoder)
	}
	c, _ := zstdAvailableEncoders.LoadOrStore(params, make(chan *zstd.Encoder, zstdMaxBufferedEncoders))
	return c.(chan *zstd.Encoder)
}

func getZstdEncoder(params ZstdEncoderParams) *zstd.Encoder {
	select {
	case enc := <-getZstdEncoderChannel(params):
		return enc
	default:
		encoderLevel := zstd.SpeedDefault
		if params.Level != CompressionLevelDefault {
			encoderLevel = zstd.EncoderLevelFromZstd(params.Level)
		}
		zstdEnc, _ := zstd.NewWriter(nil, zstd.WithZeroFrames(true),
			zstd.WithEncoderLevel(encoderLevel),
			zstd.WithEncoderConcurrency(1))
		return zstdEnc
	}
}

func releaseEncoder(params ZstdEncoderParams, enc *zstd.Encoder) {
	select {
	case getZstdEncoderChannel(params) <- enc:
	default:
	}
}

func getDecoder(params ZstdDecoderParams) *zstd.Decoder {
	if ret, ok := zstdDecMap.Load(params); ok {
		return ret.(*zstd.Decoder)
	}
	// It's possible to race and create multiple new readers.
	// Only one will survive GC after use.
	zstdDec, _ := zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
	zstdDecMap.Store(params, zstdDec)
	return zstdDec
}

func zstdDecompress(params ZstdDecoderParams, dst, src []byte) ([]byte, error) {
	return getDecoder(params).DecodeAll(src, dst)
}

func zstdCompress(params ZstdEncoderParams, dst, src []byte) ([]byte, error) {
	enc := getZstdEncoder(params)

	// Use a pooled buffer when no destination is provided to avoid
	// a heap allocation on every call (see github.com/IBM/sarama/issues/2964).
	if dst == nil {
		bufPtr := zstdDstPool.Get().(*[]byte)
		poolBuf := (*bufPtr)[:0]

		compressed := enc.EncodeAll(src, poolBuf)
		releaseEncoder(params, enc)

		// Copy result out so the pooled buffer can be reused.
		result := make([]byte, len(compressed))
		copy(result, compressed)

		*bufPtr = compressed[:0]
		zstdDstPool.Put(bufPtr)

		return result, nil
	}

	out := enc.EncodeAll(src, dst)
	releaseEncoder(params, enc)
	return out, nil
}
