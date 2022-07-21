package heaptuple

// #include <stdbool.h>
// #include <stdint.h>
// #include <string.h>
//
// #define Min(x, y)		((x) < (y) ? (x) : (y))
// #define unlikely(x) ((x) != 0)
//
// typedef int32_t int32;
//
// int32
// pglz_decompress(const char *source, int32 slen, char *dest,
// 				int32 rawsize, bool check_complete)
// {
// 	const unsigned char *sp;
// 	const unsigned char *srcend;
// 	unsigned char *dp;
// 	unsigned char *destend;
//
// 	sp = (const unsigned char *) source;
// 	srcend = ((const unsigned char *) source) + slen;
// 	dp = (unsigned char *) dest;
// 	destend = dp + rawsize;
//
// 	while (sp < srcend && dp < destend)
// 	{
// 		/*
// 		 * Read one control byte and process the next 8 items (or as many as
// 		 * remain in the compressed input).
// 		 */
// 		unsigned char ctrl = *sp++;
// 		int			ctrlc;
//
// 		for (ctrlc = 0; ctrlc < 8 && sp < srcend && dp < destend; ctrlc++)
// 		{
// 			if (ctrl & 1)
// 			{
// 				/*
// 				 * Set control bit means we must read a match tag. The match
// 				 * is coded with two bytes. First byte uses lower nibble to
// 				 * code length - 3. Higher nibble contains upper 4 bits of the
// 				 * offset. The next following byte contains the lower 8 bits
// 				 * of the offset. If the length is coded as 18, another
// 				 * extension tag byte tells how much longer the match really
// 				 * was (0-255).
// 				 */
// 				int32		len;
// 				int32		off;
//
// 				len = (sp[0] & 0x0f) + 3;
// 				off = ((sp[0] & 0xf0) << 4) | sp[1];
// 				sp += 2;
// 				if (len == 18)
// 					len += *sp++;
//
// 				/*
// 				 * Check for corrupt data: if we fell off the end of the
// 				 * source, or if we obtained off = 0, we have problems.  (We
// 				 * must check this, else we risk an infinite loop below in the
// 				 * face of corrupt data.)
// 				 */
// 				if (unlikely(sp > srcend || off == 0))
// 					return -1;
//
// 				/*
// 				 * Don't emit more data than requested.
// 				 */
// 				len = Min(len, destend - dp);
//
// 				/*
// 				 * Now we copy the bytes specified by the tag from OUTPUT to
// 				 * OUTPUT (copy len bytes from dp - off to dp).  The copied
// 				 * areas could overlap, so to avoid undefined behavior in
// 				 * memcpy(), be careful to copy only non-overlapping regions.
// 				 *
// 				 * Note that we cannot use memmove() instead, since while its
// 				 * behavior is well-defined, it's also not what we want.
// 				 */
// 				while (off < len)
// 				{
// 					/*
// 					 * We can safely copy "off" bytes since that clearly
// 					 * results in non-overlapping source and destination.
// 					 */
// 					memcpy(dp, dp - off, off);
// 					len -= off;
// 					dp += off;
//
// 					/*----------
// 					 * This bit is less obvious: we can double "off" after
// 					 * each such step.  Consider this raw input:
// 					 *		112341234123412341234
// 					 * This will be encoded as 5 literal bytes "11234" and
// 					 * then a match tag with length 16 and offset 4.  After
// 					 * memcpy'ing the first 4 bytes, we will have emitted
// 					 *		112341234
// 					 * so we can double "off" to 8, then after the next step
// 					 * we have emitted
// 					 *		11234123412341234
// 					 * Then we can double "off" again, after which it is more
// 					 * than the remaining "len" so we fall out of this loop
// 					 * and finish with a non-overlapping copy of the
// 					 * remainder.  In general, a match tag with off < len
// 					 * implies that the decoded data has a repeat length of
// 					 * "off".  We can handle 1, 2, 4, etc repetitions of the
// 					 * repeated string per memcpy until we get to a situation
// 					 * where the final copy step is non-overlapping.
// 					 *
// 					 * (Another way to understand this is that we are keeping
// 					 * the copy source point dp - off the same throughout.)
// 					 *----------
// 					 */
// 					off += off;
// 				}
// 				memcpy(dp, dp - off, len);
// 				dp += len;
// 			}
// 			else
// 			{
// 				/*
// 				 * An unset control bit means LITERAL BYTE. So we just copy
// 				 * one from INPUT to OUTPUT.
// 				 */
// 				*dp++ = *sp++;
// 			}
//
// 			/*
// 			 * Advance the control bit
// 			 */
// 			ctrl >>= 1;
// 		}
// 	}
//
// 	/*
// 	 * If requested, check we decompressed the right amount.
// 	 */
// 	if (check_complete && (dp != destend || sp != srcend))
// 		return -1;
//
// 	/*
// 	 * That's it.
// 	 */
// 	return (char *) dp - dest;
// }
import "C"

import (
	"fmt"
	"unsafe"
)

func Decompress(src []byte, dest []byte) error {
	srcAddr := (*C.char)(unsafe.Pointer(&src[0]))
	srcLen := len(src)
	destAddr := (*C.char)(unsafe.Pointer(&dest[0]))
	destLen := len(dest)

	size := C.pglz_decompress(srcAddr, C.int32(srcLen), destAddr, C.int32(destLen), C.bool(true))
	if size == -1 || size != C.int32(len(dest)) {
		return fmt.Errorf("decompress failed")
	}
	return nil
}
