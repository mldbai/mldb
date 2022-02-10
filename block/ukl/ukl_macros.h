#pragma once

#define SYNC_CALL_0(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_1(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_2(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_3(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_4(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_5(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_6(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_7(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_8(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_9(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_10(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_11(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_12(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_13(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_14(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_15(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_16(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_17(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_18(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_19(...) SYNC_CALL(__VA_ARGS__)

// Returns the hundred and first argument in the list.  Used to implement COUNT_ARGS.
#define TWO_HUNDRED_AND_FIRST_ARGUMENT(a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, \
                             a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, \
                             a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, \
                             a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, \
                             a40, a41, a42, a43, a44, a45, a46, a47, a48, a49, \
                             a50, a51, a52, a53, a54, a55, a56, a57, a58, a59, \
                             a60, a61, a62, a63, a64, a65, a66, a67, a68, a69, \
                             a70, a71, a72, a73, a74, a75, a76, a77, a78, a79, \
                             a80, a81, a82, a83, a84, a85, a86, a87, a88, a89, \
                             a90, a91, a92, a93, a94, a95, a96, a97, a98, a99, \
                             a100, a101, a102, a103, a104, a105, a106, a107, a108, a109, \
                             a110, a111, a112, a113, a114, a115, a116, a117, a118, a119, \
                             a120, a121, a122, a123, a124, a125, a126, a127, a128, a129, \
                             a130, a131, a132, a133, a134, a135, a136, a137, a138, a139, \
                             a140, a141, a142, a143, a144, a145, a146, a147, a148, a149, \
                             a150, a151, a152, a153, a154, a155, a156, a157, a158, a159, \
                             a160, a161, a162, a163, a164, a165, a166, a167, a168, a169, \
                             a170, a171, a172, a173, a174, a175, a176, a177, a178, a179, \
                             a180, a181, a182, a183, a184, a185, a186, a187, a188, a189, \
                             a190, a191, a192, a193, a194, a195, a196, a197, a198, a199, \
                             a200, ...) a200

// Returns the number of arguments in its argument list
#define COUNT_ARGS(...) TWO_HUNDRED_AND_FIRST_ARGUMENT(dummy, ## __VA_ARGS__, 199, 198, 197, 196, 195, 194, 193, 192, 191, 190, 189, 188, 187, 186, 185, 184, 183, 182, 181, 180, 179, 178, 177, 176, 175, 174, 173, 172, 171, 170, 169, 168, 167, 166, 165, 164, 163, 162, 161, 160, 159, 158, 157, 156, 155, 154, 153, 152, 151, 150, 149, 148, 147, 146, 145, 144, 143, 142, 141, 140, 139, 138, 137, 136, 135, 134, 133, 132, 131, 130, 129, 128, 127, 126, 125, 124, 123, 122, 121, 120, 119, 118, 117, 116, 115, 114, 113, 112, 111, 110, 109, 108, 107, 106, 105, 104, 103, 102, 101, 100, 99, 98, 97, 96, 95, 94, 93, 92, 91, 90, 89, 88, 87, 86, 85, 84, 83, 82, 81, 80, 79, 78, 77, 76, 75, 74, 73, 72, 71, 70, 69, 68, 67, 66, 65, 64, 63, 62, 61, 60, 59, 58, 57, 56, 55, 54, 53, 52, 51, 50, 49, 48, 47, 46, 45, 44, 43, 42, 41, 40, 39, 38, 37, 36, 35, 34, 33, 32, 31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)

// Call the given macro with the given arguments
#define CALL(Fn, ...) Fn(__VA_ARGS__)

// Call the macro given by concatenating Name1 and Name2 with the given args
#define CALL2(Name1, Name2, ...) CALL(Name1##Name2, __VA_ARGS__)

// Another version so we can apply call when evaluating call
#define CALL_A(Fn, ...) Fn(__VA_ARGS__)
#define CALL2_A(Name1, Name2, ...) CALL_A(Name1##Name2, __VA_ARGS__)

#define CALL_B(Fn, ...) Fn(__VA_ARGS__)
#define CALL2_B(Name1, Name2, ...) CALL_B(Name1##Name2, __VA_ARGS__)

#define FOREACH_PAIR_0(Fn) \

#define FOREACH_PAIR_2(Fn, T0, N0) \
    Fn(T0, N0)
#define FOREACH_PAIR_4(Fn, T0, N0, T1, N1) \
    Fn(T0, N0), Fn(T1, N1)
#define FOREACH_PAIR_6(Fn, T0, N0, T1, N1, T2, N2) \
    Fn(T0, N0), Fn(T1, N1), Fn(T2, N2)
#define FOREACH_PAIR_8(Fn, T0, N0, T1, N1, T2, N2, T3, N3) \
    Fn(T0, N0), Fn(T1, N1), Fn(T2, N2), Fn(T3, N3)
#define FOREACH_PAIR_10(Fn, T0, N0, T1, N1, T2, N2, T3, N3, T4, N4) \
    Fn(T0, N0), Fn(T1, N1), Fn(T2, N2), Fn(T3, N3), Fn(T4, N4)
#define FOREACH_PAIR_12(Fn, T0, N0, T1, N1, T2, N2, T3, N3, T4, N4, T5, N5) \
    Fn(T0, N0), Fn(T1, N1), Fn(T2, N2), Fn(T3, N3), Fn(T4, N4), Fn(T5, N5)
#define FOREACH_PAIR_14(Fn, T0, N0, T1, N1, T2, N2, T3, N3, T4, N4, T5, N5, T6, N6) \
    Fn(T0, N0), Fn(T1, N1), Fn(T2, N2), Fn(T3, N3), Fn(T4, N4), Fn(T5, N5), Fn(T6, N6)
#define FOREACH_PAIR_16(Fn, T0, N0, T1, N1, T2, N2, T3, N3, T4, N4, T5, N5, T6, N6, T7, N7) \
    Fn(T0, N0), Fn(T1, N1), Fn(T2, N2), Fn(T3, N3), Fn(T4, N4), Fn(T5, N5), Fn(T6, N6), Fn(T7, N7)
#define FOREACH_PAIR_18(Fn, T0, N0, T1, N1, T2, N2, T3, N3, T4, N4, T5, N5, T6, N6, T7, N7, T8, N8) \
    Fn(T0, N0), Fn(T1, N1), Fn(T2, N2), Fn(T3, N3), Fn(T4, N4), Fn(T5, N5), Fn(T6, N6), Fn(T7, N7), Fn(T8, N8)
#define FOREACH_PAIR_20(Fn, T0, N0, T1, N1, T2, N2, T3, N3, T4, N4, T5, N5, T6, N6, T7, N7, T8, N8, T9, N9) \
    Fn(T0, N0), Fn(T1, N1), Fn(T2, N2), Fn(T3, N3), Fn(T4, N4), Fn(T5, N5), Fn(T6, N6), Fn(T7, N7), Fn(T8, N8), Fn(T9, N9)
#define FOREACH_PAIR_22(Fn, T0, N0, ...) \
    Fn(T0, N0), FOREACH_PAIR_20(Fn, __VA_ARGS__)
#define FOREACH_PAIR_24(Fn, T0, N0, T1, N1, ...) \
    Fn(T0, N0), Fn(T1, N1), FOREACH_PAIR_20(Fn, __VA_ARGS__)
#define FOREACH_PAIR_26(Fn, T0, N0, T1, N1, T2, N2, ...) \
    Fn(T0, N0), Fn(T1, N1), Fn(T2, N2), FOREACH_PAIR_20(Fn, __VA_ARGS__)
#define FOREACH_PAIR_28(Fn, T0, N0, T1, N1, T2, N2, T3, N3, ...) \
    Fn(T0, N0), Fn(T1, N1), Fn(T2, N2), Fn(T3, N3), FOREACH_PAIR_20(Fn, __VA_ARGS__)
#define FOREACH_PAIR_30(Fn, T0, N0, T1, N1, T2, N2, T3, N3, T4, N4, ...) \
    Fn(T0, N0), Fn(T1, N1), Fn(T2, N2), Fn(T3, N3), Fn(T4, N4), FOREACH_PAIR_20(Fn, __VA_ARGS__)
#define FOREACH_PAIR_32(Fn, T0, N0, T1, N1, T2, N2, T3, N3, T4, N4, T5, N5, ...) \
    Fn(T0, N0), Fn(T1, N1), Fn(T2, N2), Fn(T3, N3), Fn(T4, N4), Fn(T5, N5), FOREACH_PAIR_20(Fn, __VA_ARGS__)
#define FOREACH_PAIR_34(Fn, T0, N0, T1, N1, T2, N2, T3, N3, T4, N4, T5, N5, T6, N6, ...) \
    Fn(T0, N0), Fn(T1, N1), Fn(T2, N2), Fn(T3, N3), Fn(T4, N4), Fn(T5, N5), Fn(T6, N6), FOREACH_PAIR_20(Fn, __VA_ARGS__)
#define FOREACH_PAIR_36(Fn, T0, N0, T1, N1, T2, N2, T3, N3, T4, N4, T5, N5, T6, N6, T7, N7, ...) \
    Fn(T0, N0), Fn(T1, N1), Fn(T2, N2), Fn(T3, N3), Fn(T4, N4), Fn(T5, N5), Fn(T6, N6), Fn(T7, N7), FOREACH_PAIR_20(Fn, __VA_ARGS__)
#define FOREACH_PAIR_38(Fn, T0, N0, T1, N1, T2, N2, T3, N3, T4, N4, T5, N5, T6, N6, T7, N7, T8, N8, ...) \
    Fn(T0, N0), Fn(T1, N1), Fn(T2, N2), Fn(T3, N3), Fn(T4, N4), Fn(T5, N5), Fn(T6, N6), Fn(T7, N7), Fn(T8, N8), FOREACH_PAIR_20(Fn, __VA_ARGS__)
#define FOREACH_PAIR_40(Fn, T0, N0, T1, N1, T2, N2, T3, N3, T4, N4, T5, N5, T6, N6, T7, N7, T8, N8, T9, N9, ...) \
    Fn(T0, N0), Fn(T1, N1), Fn(T2, N2), Fn(T3, N3), Fn(T4, N4), Fn(T5, N5), Fn(T6, N6), Fn(T7, N7), Fn(T8, N8), Fn(T9, N9), FOREACH_PAIR_20(Fn, __VA_ARGS__)


#define FOREACH_PAIR_N(Name, Count, ...) \
    CALL2(FOREACH_PAIR_, Count, Name, __VA_ARGS__)

#define FOREACH_PAIR(Fn, ...) \
    FOREACH_PAIR_N(Fn, COUNT_ARGS(__VA_ARGS__), __VA_ARGS__)

#define COMMA_IF(...) __VA_OPT__(,)

#define COMMA_IF_CALL(Fn, ...) COMMA_IF(CALL_B(Fn, __VA_ARGS__))

#define MAYBE_COMMA(Fn, ...) COMMA_IF_CALL(Fn, __VA_ARGS__) CALL_B(Fn, __VA_ARGS__) 

#define FOREACH_QUAD_0(Fn)
#define FOREACH_QUAD_4(Fn, A0, A1, A2, A3)        MAYBE_COMMA(Fn, A0, A1, A2, A3)
#define FOREACH_QUAD_8(Fn, A0, A1, A2, A3, ...)   MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_4(Fn, __VA_ARGS__)
#define FOREACH_QUAD_12(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_8(Fn, __VA_ARGS__)
#define FOREACH_QUAD_16(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_12(Fn, __VA_ARGS__)
#define FOREACH_QUAD_20(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_16(Fn, __VA_ARGS__)
#define FOREACH_QUAD_24(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_20(Fn, __VA_ARGS__)
#define FOREACH_QUAD_28(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_24(Fn, __VA_ARGS__)
#define FOREACH_QUAD_32(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_28(Fn, __VA_ARGS__)
#define FOREACH_QUAD_36(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_32(Fn, __VA_ARGS__)
#define FOREACH_QUAD_40(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_36(Fn, __VA_ARGS__)
#define FOREACH_QUAD_44(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_40(Fn, __VA_ARGS__)
#define FOREACH_QUAD_48(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_44(Fn, __VA_ARGS__)
#define FOREACH_QUAD_52(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_48(Fn, __VA_ARGS__)
#define FOREACH_QUAD_56(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_52(Fn, __VA_ARGS__)
#define FOREACH_QUAD_60(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_56(Fn, __VA_ARGS__)
#define FOREACH_QUAD_64(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_60(Fn, __VA_ARGS__)
#define FOREACH_QUAD_68(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_64(Fn, __VA_ARGS__)
#define FOREACH_QUAD_72(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_68(Fn, __VA_ARGS__)
#define FOREACH_QUAD_76(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_72(Fn, __VA_ARGS__)
#define FOREACH_QUAD_80(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_76(Fn, __VA_ARGS__)
#define FOREACH_QUAD_84(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_80(Fn, __VA_ARGS__)
#define FOREACH_QUAD_88(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_84(Fn, __VA_ARGS__)
#define FOREACH_QUAD_92(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_88(Fn, __VA_ARGS__)
#define FOREACH_QUAD_96(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_92(Fn, __VA_ARGS__)
#define FOREACH_QUAD_100(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_96(Fn, __VA_ARGS__)
#define FOREACH_QUAD_104(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_100(Fn, __VA_ARGS__)
#define FOREACH_QUAD_108(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_104(Fn, __VA_ARGS__)
#define FOREACH_QUAD_112(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_108(Fn, __VA_ARGS__)
#define FOREACH_QUAD_116(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_112(Fn, __VA_ARGS__)
#define FOREACH_QUAD_120(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_116(Fn, __VA_ARGS__)
#define FOREACH_QUAD_124(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_120(Fn, __VA_ARGS__)
#define FOREACH_QUAD_128(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_124(Fn, __VA_ARGS__)
#define FOREACH_QUAD_132(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_128(Fn, __VA_ARGS__)
#define FOREACH_QUAD_136(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_132(Fn, __VA_ARGS__)
#define FOREACH_QUAD_140(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_136(Fn, __VA_ARGS__)
#define FOREACH_QUAD_144(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_140(Fn, __VA_ARGS__)
#define FOREACH_QUAD_148(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_144(Fn, __VA_ARGS__)
#define FOREACH_QUAD_152(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_148(Fn, __VA_ARGS__)
#define FOREACH_QUAD_156(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_152(Fn, __VA_ARGS__)
#define FOREACH_QUAD_160(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_156(Fn, __VA_ARGS__)
#define FOREACH_QUAD_164(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_160(Fn, __VA_ARGS__)
#define FOREACH_QUAD_168(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_164(Fn, __VA_ARGS__)
#define FOREACH_QUAD_172(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_168(Fn, __VA_ARGS__)
#define FOREACH_QUAD_176(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_172(Fn, __VA_ARGS__)
#define FOREACH_QUAD_180(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_176(Fn, __VA_ARGS__)
#define FOREACH_QUAD_184(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_180(Fn, __VA_ARGS__)
#define FOREACH_QUAD_188(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_184(Fn, __VA_ARGS__)
#define FOREACH_QUAD_192(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_188(Fn, __VA_ARGS__)
#define FOREACH_QUAD_196(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_192(Fn, __VA_ARGS__)
#define FOREACH_QUAD_200(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_196(Fn, __VA_ARGS__)

#define FOREACH_QUAD_N(Name, Count, ...) \
    CALL2(FOREACH_QUAD_, Count, Name, __VA_ARGS__)

#define FOREACH_QUAD(Fn, ...) \
    FOREACH_QUAD_N(Fn, COUNT_ARGS(__VA_ARGS__), __VA_ARGS__)
