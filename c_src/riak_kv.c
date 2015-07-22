#include <string.h>
#include "erl_nif.h"
#include "erl_driver.h"


static ERL_NIF_TERM ATOM_OK;
static ERL_NIF_TERM ATOM_ERROR;
static ERL_NIF_TERM ATOM_BAD_ALLOC;
static ERL_NIF_TERM ATOM_INVALID_INPUT;

/*

  Object format:
53 : byte
1 : byte -- Version 1 
VclockLen : int32
Vclock: bytes[VclockLen]
SibCount : int32

For each Sibling:

ValLen : int32
ValBin : bytes[ValLen]
MetaLen : int32
LastModMega : uint32
LastModSecs : uint32
LastModMicro : uint32
VTagLen : uint8
VTag    : bytes[VTagLen]
Deleted : uint8
MetaRest : bytes[MetaLen - 14 - VTagLen]

*/

/*
 -- Protocol Buffers layout for storing KV objects
VC = <<1,1,1>>, K = <<2,2,2>>, V = <<3,3,3>>
<<41, -- code for RpbCsBucketResp
-- One of the following per index object (K,Obj pair)
10, -- 1010, field 1 (RpbIndexObject[]) wire type 2 (sub-message)
 19, -- Sub message in next 19 bytes
    10, -- field 1 (key), wire type 2 (bytes)
      3, -- 3 bytes
      2,2,2,
    18, -- field 2 (RpbGetResp object), wire type 2 (sub-message)
      12, -- object in the next 12 bytes
        -- One of the following per sibling
        10, -- field 1 (content), wt 2 (rpbcontent)
          5, -- using the next 5 bytes
            10, -- field 1 (value) wt 2 (bytes)
              3, -- using next 3 bytes
              3,3,3,
        18, -- field 2 (vclock) wt 2 (bytes)
          3, -- using next 3 bytes
            1,1,1>>
*/

    /*
       Input is list of {o, K, V}.

NOTE: 1st pass we'll copy all data to binaries. Later change to include
      sub-binaries for KV values, maybe long keys.
      Also, vclock output as is (binary to term'ed vclock).

       Allocate one buffer.
       Prepare list of binaries structure.

       Output RpbCsBucketResp message code to buffer (41).
       For each element in list:
         Output objects field code (10)
         Output 4 byte placeholder for varint size of index pair

         Output index pair key field code (10)
         Output key size as varint.
         Copy key to buffer

         Output index pair object field code (18)
         Output 4 byte placeholder for varint size of object.

         Output vclock field code (18)
         Output vclock size as varint
         Output vclock binary to buffer.

         For each sibling in object:
           Output content field code (10)
           Output 4 byte placeholder for varint size of content
           Output value field code (10)
           Output value size as varint
           Copy value to buffer (or break binary and add to list)

           Update size of content placeholder

         Update index pair object size placeholder

         Update index pair size placeholder
           
       At any point, if buffer fills up, realloc to size, place at tail of 
       list of binaries. Mark it as an open binary (placeholder sizes may be
       updated later)
       Remember placeholder locations and sizes as position in the binary list
       + offset.
           
     */

typedef struct {
    uint8_t * data;
    size_t size;
} slice_t;

typedef enum {OPEN, OWNED} bin_state_t;
typedef union {
    ErlNifBinary bin;
    ERL_NIF_TERM bin_term;
} bin_or_term_t;

typedef struct {
    bin_or_term_t bin_or_term;
    bin_state_t bin_state;
} response_binary_t;

typedef struct {
    int bin_index;
    int offset;
} bin_location_t;

static const size_t init_bin_size = 1024 * 1024;
static const int max_response_binaries = 500;

typedef struct {
    response_binary_t bins[max_response_binaries];
    ErlNifBinary * current_bin;
    int current_idx;
    size_t offset;
} bin_out_t;

int rkve_bin_out_init(ErlNifEnv * env, bin_out_t * bin_out)
{
    bin_out->offset = 0;
    bin_out->current_idx = 0;
    bin_out->current_bin = &bin_out->bins[0].bin_or_term.bin;
    bin_out->bins[0].bin_state = OPEN;

    return enif_alloc_binary(init_bin_size, bin_out->current_bin);
}

static void rkve_bin_out_get_location(bin_out_t * bin_out,
                                      bin_location_t * location)
{
    location->bin_index = bin_out->current_idx;
    location->offset = bin_out->offset;
}

static uint8_t * rkve_bin_out_get_location_pointer(const bin_out_t * bin_out,
                                                   const bin_location_t location)
{
    return (uint8_t*) bin_out->bins[location.bin_index].bin_or_term.bin.data
        + location.offset;
}
 
void rkve_bin_out_free(bin_out_t * bin_out)
{
    int i;

    for (i = 0; i <= bin_out->current_idx; ++i) {
        if (bin_out->bins[i].bin_state == OPEN) {
            enif_release_binary(&bin_out->bins[i].bin_or_term.bin);
        }
    }
}

/* 
 * After returning successfully, it's safe to write num_bytes bytes to the
 * output binary data.
 * Returns 1 on success. */
static int rkve_bin_out_make_room(size_t num_bytes, bin_out_t * bin_out)
{
    size_t new_size;

    // Is current space enough?
    if (bin_out->current_bin->size - bin_out->offset >= num_bytes)
        return 1;

    //Allocate another binary, realloc this one to size.
    if (!enif_realloc_binary(bin_out->current_bin, bin_out->offset)) {
        // Failed. Free the whole thing, bail.
        rkve_bin_out_free(bin_out);
        return 0;
    }

    new_size = num_bytes > init_bin_size ? num_bytes : init_bin_size;

    bin_out->offset = 0;
    bin_out->current_idx++;
    bin_out->current_bin = &bin_out->bins[bin_out->current_idx].bin_or_term.bin;

    return enif_alloc_binary(new_size, bin_out->current_bin);
}

// Do not call unless you have verified there is space for the value.
static void rkve_bin_out_write_byte(bin_out_t * bin_out, uint8_t val)
{
    bin_out->current_bin->data[bin_out->offset++] = val;
}

static void rkve_bin_out_skip(bin_out_t * out, size_t size)
{
    out->offset += size;
}

static uint8_t rkve_write_varint(uint64_t v, uint8_t * out)
{
    uint8_t l = 1, byte = v & 0xf;
    while (v >>= 7) {
        *out++ = byte | 0x80;
        byte = v & 0xf;
        ++l;
    }
    *out = byte;
    return l;
}

static void rkve_write_varint4(uint64_t v, uint8_t * out)
{
    uint8_t l = 1, byte = v & 0xf;
    out[0] = (v & 0xf) | 0x80;
    out[1] = ((v >> 7) & 0xf) | 0x80;
    out[2] = ((v >> 14) & 0xf) | 0x80;
    out[3] = ((v >> 21) & 0xf);
}

static void rkve_bin_out_write_varint(bin_out_t * bin_out,
                                      uint64_t n)
{
    uint8_t varint_size =
        rkve_write_varint(n,
                          (uint8_t*)bin_out->current_bin->data
                          + bin_out->offset);
    rkve_bin_out_skip(bin_out, varint_size);

}
static void rkve_bin_out_write_bytes(bin_out_t * bin_out,
                                     const slice_t slice)
{
    memcpy(bin_out->current_bin->data + bin_out->offset, slice.data,
           slice.size);
    bin_out->offset += slice.size;
}

static void rkve_bin_out_write_prefixed_bytes(bin_out_t * bin_out,
                                              const slice_t slice)
{
    rkve_bin_out_write_varint(bin_out, slice.size);
    rkve_bin_out_write_bytes(bin_out, slice);
}

static void rkve_slice_from_bin(ErlNifBinary * bin, slice_t * slice)
{
    slice->data = (uint8_t*)bin->data;
    slice->size = bin->size;
}

/* Read big endian unsigned 32 bit value */
static int rkve_read_uint32(slice_t * in, uint32_t * out)
{
    if (in->size < 4)
        return 0;

    (*out) = ((uint32_t)in->data[0]) << 24
        | ((uint32_t)in->data[1]) << 16
        | ((uint32_t)in->data[2]) << 8
        | ((uint32_t)in->data[3]);

    in->size -= 4;
    in->data += 4;
    return 1;
}

/* Read 32 bit length prefixed slice and advance the pointer. */
static int rkve_read_bytes32(slice_t * in, slice_t * slice)
{
    uint32_t size;
    if (rkve_read_uint32(in, &size) && in->size > size) {
        slice->size = size;
        slice->data = in->data;
        in->data += size;
        return 1;
    }

    return 0;
}

static int rkve_slice_skip(slice_t * slice, size_t n) {
    if (slice->size > n) {
        slice->size -= n;
        slice->data += n;
        return 1;
    }
    return 0;
}

static uint8_t rkve_varint_size(uint64_t s)
{
    uint8_t l = 1;
    while (s >>= 7)
        ++l;
    return l;
}

/* Make Erlang binaries out of all the binaries created here.
 * Sub-binaries are already Erlang binaries.
 */
void rkve_bin_out_make_binaries(bin_out_t * out, ErlNifEnv * env)
{
    int i;
    for (i = 0; i <= out->current_idx; ++i) {
        if (out->bins[i].bin_state == OPEN) {
            out->bins[i].bin_or_term.bin_term =
                enif_make_binary(env, &out->bins[i].bin_or_term.bin);
            out->bins[i].bin_state = OWNED;
        }
    }
}

/* Convert data structure to final Erlang iolist or binary form. */
ERL_NIF_TERM rkve_bin_out_to_term(bin_out_t * out, ErlNifEnv * env)
{
    // Special case a single binary, don't put it in a list.
    if (out->current_idx == 0) {
        return out->bins[0].bin_or_term.bin_term;
    }

    // Create list in reverse
    int i;
    ERL_NIF_TERM list = enif_make_list(env, 0);
    for (i = out->current_idx; i >= 0; --i) {
        ERL_NIF_TERM next_bin = out->bins[i].bin_or_term.bin_term;
        list = enif_make_list_cell(env, next_bin, list);
    }

    return list;
}

int rkve_encode_obj(ErlNifEnv * env,
                    bin_out_t * bin_out,
                    ERL_NIF_TERM key_term,
                    ERL_NIF_TERM value_term)
{
    ErlNifBinary key_bin, value_bin;
    slice_t input, vc_slice, key_slice;
    uint32_t sib_count;

    if (!enif_inspect_binary(env, key_term, &key_bin)
        || !enif_inspect_binary(env, value_term, &value_bin)) {
        return 0;
    }
    
    rkve_slice_from_bin(&value_bin, &input);
    rkve_slice_from_bin(&key_bin, &key_slice);

    // Skip 53, 1 header, read vclock and sibling count.
    if (!rkve_slice_skip(&input, 2)
        || !rkve_read_bytes32(&input, &vc_slice)
        || !rkve_read_uint32(&input, &sib_count))
        return 0;

    uint8_t vclen_varint_size = rkve_varint_size((uint64_t)vc_slice.size);
    uint8_t klen_varint_size = rkve_varint_size((uint64_t)key_bin.size);

    size_t space_needed =
        1 
        + 4
        + 1
        + klen_varint_size
        + key_bin.size
        + 1
        + 4
        + 1
        + vclen_varint_size
        + vc_slice.size;

    /* Ensure enough room for 
     * objects field code : 1
     * objects size placeholder : 4
     * index pair key field code : 1
     * key size as varint : varint_size(key_size)
     * key size
     * index pair object field code : 1
     * object size placeholder : 4
     * vclock field code : 1
     * length of vclock size as varint : varint_size(vc_size)
     * size of vclock binary.
     */
    
    if (!rkve_bin_out_make_room(space_needed, bin_out))
        return 0;

    bin_location_t pair_size_loc, obj_size_loc;

    rkve_bin_out_write_byte(bin_out, 10);
    rkve_bin_out_get_location(bin_out, &pair_size_loc);
    rkve_bin_out_skip(bin_out, 4);
    rkve_bin_out_write_byte(bin_out, 10);
    rkve_bin_out_write_prefixed_bytes(bin_out, key_slice);
    rkve_bin_out_write_byte(bin_out, 18);
    rkve_bin_out_get_location(bin_out, &obj_size_loc);
    rkve_bin_out_skip(bin_out, 4);
    rkve_bin_out_write_byte(bin_out, 18);
    rkve_bin_out_write_prefixed_bytes(bin_out, vc_slice);

    size_t tot_sib_size = 0;
    while (sib_count-- > 0) {
        slice_t val_slice;
        uint32_t meta_len, sib_size;

        // Read value, skip meta-data.
        if (!rkve_read_bytes32(&input, &val_slice)
            || !rkve_read_uint32(&input, &meta_len)
            || !rkve_slice_skip(&input, meta_len))
            return 0;

        sib_size = 1 + 4 + 1
            + rkve_varint_size(val_slice.size)
            + val_slice.size;

        if (!rkve_bin_out_make_room(sib_size, bin_out))
            return 0;

        rkve_bin_out_write_byte(bin_out, 10);

        tot_sib_size += sib_size;
    }

    size_t idx_pair_obj_size = 1 + vclen_varint_size + vc_slice.size
        + tot_sib_size;
    uint8_t * idx_pair_obj_size_placeholder =
        rkve_bin_out_get_location_pointer(bin_out, obj_size_loc);

    rkve_write_varint(idx_pair_obj_size, idx_pair_obj_size_placeholder);

    size_t idx_pair_size = idx_pair_obj_size + 1 + 4
        + 1 + klen_varint_size + key_slice.size; 
    uint8_t * idx_pair_size_placeholder =
        rkve_bin_out_get_location_pointer(bin_out, pair_size_loc);
    rkve_write_varint(idx_pair_size, idx_pair_size_placeholder);

    rkve_bin_out_make_binaries(bin_out, env);

    return 1;
}

ERL_NIF_TERM rkv_encode_get_resp(ErlNifEnv* env, int argc,
                                 const ERL_NIF_TERM argv[])
{
    bin_out_t bin_out;
    ERL_NIF_TERM head, tail;

    if (argc < 1 || !enif_is_list(env, argv[0]))
        return enif_make_badarg(env);

    if (!rkve_bin_out_init(env, &bin_out))
        return enif_make_tuple2(env, ATOM_ERROR, ATOM_BAD_ALLOC);

    bin_out.current_bin->data[bin_out.offset++] = 41;

    // Convert list of KVs to a big PB iolist or binary.
    for (tail = argv[0];
         !enif_is_empty_list(env, tail);
         enif_get_list_cell(env, tail, &head, &tail)) {
        int tlen;
        const ERL_NIF_TERM * telements;
        // Read {o, Key, Value} tuple.
        if (!enif_get_tuple(env, head, &tlen, &telements) || tlen != 3) {
            rkve_bin_out_free(&bin_out);
            return enif_make_tuple2(env, ATOM_ERROR, ATOM_INVALID_INPUT);
        }

        if (!rkve_encode_obj(env, &bin_out, telements[1], telements[2])) {
            rkve_bin_out_free(&bin_out);
            return enif_make_tuple2(env, ATOM_ERROR, ATOM_INVALID_INPUT);
        }
    }

    ERL_NIF_TERM result = rkve_bin_out_to_term(&bin_out, env);

    return enif_make_tuple2(env, ATOM_OK, result);
}

static ErlNifFunc nif_funcs[] =
{
    {"encode_get_resp", 1, rkv_encode_get_resp}
};

static int on_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    ATOM_OK = enif_make_atom(env, "ok");
    ATOM_ERROR = enif_make_atom(env, "error");
    ATOM_BAD_ALLOC = enif_make_atom(env, "bad_alloc"); 
    ATOM_INVALID_INPUT = enif_make_atom(env, "invalid_input");
    return 0;
}

ERL_NIF_INIT(rkv_encoding_nifs, nif_funcs, &on_load, NULL, NULL, NULL);
