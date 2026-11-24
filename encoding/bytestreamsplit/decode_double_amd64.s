//go:build !purego && goexperiment.simd && go1.27

#include "textflag.h"

// func decodeDouble(dst, src []byte)
TEXT ·decodeDouble(SB), NOSPLIT, $0-48
	MOVQ dst_base+0(FP), AX
	MOVQ dst_len+8(FP), BX
	MOVQ src_base+24(FP), DX

	MOVQ AX, CX
	ADDQ BX, CX
	SHRQ $3, BX

	CMPQ BX, $0
	JE   done

	CMPB ·decodeDoubleHasAVX512(SB), $0
	JE   loop1x8

	CMPQ BX, $8
	JB   loop1x8

	MOVQ CX, DI
	SUBQ AX, DI
	SHRQ $6, DI
	SHLQ $6, DI
	ADDQ AX, DI

	VMOVDQU64  shuffle8x8<>(SB), Z0
	VPBROADCASTQ BX, Z2
	VPMULLQ    scale8x8<>(SB), Z2, Z2
loop8x8:
	KXORQ K1, K1, K1
	KNOTQ K1, K1

	VPGATHERQQ (DX)(Z2*1), K1, Z1
	VPERMB     Z1, Z0, Z1
	VMOVDQU64  Z1, (AX)

	ADDQ $64, AX
	ADDQ $8, DX
	CMPQ AX, DI
	JNE  loop8x8
	VZEROUPPER

	CMPQ AX, CX
	JE   done
loop1x8:
	MOVQ   DX, DI
	XORQ   R12, R12

	MOVBQZX (DI), R8
	ADDQ    BX, DI
	MOVBQZX (DI), R9
	ADDQ    BX, DI
	MOVBQZX (DI), R10
	ADDQ    BX, DI
	MOVBQZX (DI), R11
	ADDQ    BX, DI

	SHLQ $8, R9
	SHLQ $16, R10
	SHLQ $24, R11

	ORQ R8, R12
	ORQ R9, R12
	ORQ R10, R12
	ORQ R11, R12

	MOVBQZX (DI), R8
	ADDQ    BX, DI
	MOVBQZX (DI), R9
	ADDQ    BX, DI
	MOVBQZX (DI), R10
	ADDQ    BX, DI
	MOVBQZX (DI), R11

	SHLQ $32, R8
	SHLQ $40, R9
	SHLQ $48, R10
	SHLQ $56, R11

	ORQ R8, R12
	ORQ R9, R12
	ORQ R10, R12
	ORQ R11, R12

	MOVQ R12, (AX)

	ADDQ $8, AX
	INCQ DX
	CMPQ AX, CX
	JB   loop1x8
done:
	RET

GLOBL scale8x8<>(SB), RODATA|NOPTR, $64
DATA scale8x8<>+0(SB)/8, $0
DATA scale8x8<>+8(SB)/8, $1
DATA scale8x8<>+16(SB)/8, $2
DATA scale8x8<>+24(SB)/8, $3
DATA scale8x8<>+32(SB)/8, $4
DATA scale8x8<>+40(SB)/8, $5
DATA scale8x8<>+48(SB)/8, $6
DATA scale8x8<>+56(SB)/8, $7

GLOBL shuffle8x8<>(SB), RODATA|NOPTR, $64
DATA shuffle8x8<>+0(SB)/8, $0x3830282018100800
DATA shuffle8x8<>+8(SB)/8, $0x3931292119110901
DATA shuffle8x8<>+16(SB)/8, $0x3A322A221A120A02
DATA shuffle8x8<>+24(SB)/8, $0x3B332B231B130B03
DATA shuffle8x8<>+32(SB)/8, $0x3C342C241C140C04
DATA shuffle8x8<>+40(SB)/8, $0x3D352D251D150D05
DATA shuffle8x8<>+48(SB)/8, $0x3E362E261E160E06
DATA shuffle8x8<>+56(SB)/8, $0x3F372F271F170F07
