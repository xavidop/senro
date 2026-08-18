package dockerd

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Stream identifiers from the daemon's multiplexed frame header.
const (
	streamStdin  = 0
	streamStdout = 1
	streamStderr = 2
)

// demuxBufSize is the chunk size demux copies a frame's body in. It bounds
// memory use for an oversized frame; it does not bound the frame itself, so
// a body larger than this is copied in several chunks rather than rejected.
const demuxBufSize = 32 * 1024

// demux copies a multiplexed daemon stream into two writers.
//
// The daemon frames a non-TTY container's output as an 8-byte header
// (stream, three zero bytes, big-endian uint32 length) plus payload.
// Reading it wrong does not fail loudly: it interleaves stdout and stderr,
// corrupting both the log and the byte offsets step.log.appended publishes.
//
// Returns nil at a clean EOF on a frame boundary; an error for a truncated
// frame (writing half a line and calling the log complete would be worse)
// or an unrecognised stream byte. A zero-length frame is legal wire
// traffic, not an error.
func demux(r io.Reader, stdout, stderr io.Writer) error {
	var header [8]byte
	buf := make([]byte, demuxBufSize)
	for {
		if _, err := io.ReadFull(r, header[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return fmt.Errorf("dockerd: truncated frame header: %w", err)
			}
			return err
		}
		var w io.Writer
		switch header[0] {
		case streamStdout, streamStdin:
			w = stdout
		case streamStderr:
			w = stderr
		default:
			return fmt.Errorf("dockerd: unknown stream byte %d in a log frame; "+
				"this container was created with a TTY, which senro never does", header[0])
		}
		n := int64(binary.BigEndian.Uint32(header[4:]))
		for n > 0 {
			chunk := int64(len(buf))
			if n < chunk {
				chunk = n
			}
			read, err := r.Read(buf[:chunk])
			if read > 0 {
				if _, werr := w.Write(buf[:read]); werr != nil {
					return werr
				}
				n -= int64(read)
			}
			if err != nil {
				if errors.Is(err, io.EOF) {
					if n > 0 {
						return fmt.Errorf("dockerd: truncated frame body, %d byte(s) missing", n)
					}
					break
				}
				return err
			}
		}
	}
}
