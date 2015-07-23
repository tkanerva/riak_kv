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
    ValBin : bytes[ValLen] // Contains 1 prefix to indicate binary, not t2b
    MetaLen : int32
    LastModMega : uint32
    LastModSecs : uint32
    LastModMicro : uint32
    VTagLen : uint8
    VTag    : bytes[VTagLen]
    Deleted : uint8
    MetaRest : bytes[MetaLen - 14 - VTagLen]
    
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

typedef struct {
    ErlNifBinary bin;
    ERL_NIF_TERM term;
    uint8_t owned;
} response_binary_t;

typedef struct {
    int bin_index;
    int offset;
} bin_location_t;

static const size_t init_bin_size = 1024 * 1024;
static const int max_response_binaries = 100;

/* Holds a list of binaries containing the output processed so far. */
typedef struct {
    response_binary_t bins[max_response_binaries];
    int current_idx;
    size_t offset;
} bin_out_t;

static uint8_t rke_varsize(uint64_t s)
{
    uint8_t l = 1;
    while (s >>= 7)
        ++l;
    return l;
}

static uint8_t * rke_write_varint(uint64_t v, uint8_t * out)
{
    uint8_t byte = v & 0x7f;
    while (v >>= 7) {
        *out++ = byte | 0x80;
        byte = v & 0x7f;
    }
    // Last one written without setting last bit to signal end.
    *out++ = byte;
    return out;
}

/*
 * Write a naughty kind of varint that always takes up 4 bytes.
 * Values requiring more bytes are silently truncated.
 * It pads with redundant zero values that are technically following
 * the varint format, so are hopefully parsed ok.
 */
static void rke_write_varint4(uint32_t v, uint8_t * out)
{
    out[0] = (v & 0x7f) | 0x80;
    out[1] = ((v >> 7) & 0x7f) | 0x80;
    out[2] = ((v >> 14) & 0x7f) | 0x80;
    out[3] = ((v >> 21) & 0x7f);
}

int rke_bout_init(ErlNifEnv * env, bin_out_t * out)
{
    out->offset = 0;
    out->current_idx = 0;
    out->bins[0].owned = 0; 

    return enif_alloc_binary(init_bin_size, &out->bins[out->current_idx].bin);
}

uint8_t * rke_bout_ptr(bin_out_t * out)
{
    return (uint8_t*) out->bins[out->current_idx].bin.data + out->offset;
}

static void rke_bout_get_location(bin_out_t * out,
                                      bin_location_t * location)
{
    location->bin_index = out->current_idx;
    location->offset = out->offset;
}

static uint8_t * rke_bout_loc_ptr(const bin_out_t * out,
                                                   const bin_location_t loc)
{
    return (uint8_t*) out->bins[loc.bin_index].bin.data + loc.offset;
}
 
void rke_bout_free(bin_out_t * out)
{
    int i;
    for (i = 0; i <= out->current_idx; ++i) {
        if (!out->bins[i].owned)
            enif_release_binary(&out->bins[i].bin);
    }
}

/* 
 * After returning successfully, it's safe to write num_bytes bytes to the
 * output binary data.
 * Returns 1 on success. */
static int rke_bout_make_room(bin_out_t * out, size_t num_bytes)
{
    size_t new_size;
    int next_idx;

    // Is current space enough?
    if (out->bins[out->current_idx].bin.size - out->offset >= num_bytes)
        return 1;

    // Trim current one to size.
    if (!enif_realloc_binary(&out->bins[out->current_idx].bin, out->offset)) {
        // Failed. Free the whole thing, bail.
        rke_bout_free(out);
        return 0;
    }

    // Allocation a new binary.
    next_idx = out->current_idx + 1;

    if (next_idx >= max_response_binaries)
        return 0;

    new_size = num_bytes > init_bin_size ? num_bytes : init_bin_size;

    if (enif_alloc_binary(new_size, &out->bins[next_idx].bin)) {
        out->bins[next_idx].owned = 0;
        out->offset = 0;
        out->current_idx = next_idx;
        return 1;
    }

    return 0;
}

// Do not call unless you have verified there is space for the extra byte.
static void rke_bout_write_byte(bin_out_t * out, uint8_t val)
{
    out->bins[out->current_idx].bin.data[out->offset++] = val;
}

/*
 * Move output pointer forward without writing anything.
 * Useful to create placeholder slots to be filled out later.
 * Verify that the extra space exists before calling.
 */
static void rke_bout_skip(bin_out_t * out, size_t size)
{
    out->offset += size;
}

static void rke_bout_write_varint(bin_out_t * out, uint64_t n)
{
    uint8_t * cur = rke_bout_ptr(out);
    uint8_t * next = rke_write_varint(n, cur);
    rke_bout_skip(out, next - cur);
}

static void rke_bout_write_bytes(bin_out_t * out, const slice_t slice)
{
    memcpy(rke_bout_ptr(out), slice.data, slice.size);
    out->offset += slice.size;
}

static void rke_slice_from_bin(ErlNifBinary * bin, slice_t * slice)
{
    slice->data = (uint8_t*)bin->data;
    slice->size = bin->size;
}

/* Read big endian unsigned 32 bit value */
static int rke_read_big_endian_uint32(slice_t * in, uint32_t * out)
{
    if (in->size < 4)
        return 0;

    (*out) = ((uint32_t)in->data[0]) << 24
        | ((uint32_t)in->data[1]) << 16
        | ((uint32_t)in->data[2]) << 8
        | ((uint32_t)in->data[3]);

    // Point slice after 32 bit value.
    in->size -= 4;
    in->data += 4;
    return 1;
}

/* Read 32 bit length prefixed slice and advance the pointer. */
static int rke_read_bytes32(slice_t * in, slice_t * slice)
{
    uint32_t size;
    if (rke_read_big_endian_uint32(in, &size) && in->size > size) {
        slice->size = size;
        slice->data = in->data;
        in->data += size;
        in->size -= size;
        return 1;
    }

    return 0;
}

static int rke_slice_skip(slice_t * slice, size_t n)
{
    if (slice->size >= n) {
        slice->size -= n;
        slice->data += n;
        return 1;
    }
    return 0;
}

/* Make Erlang binaries out of all the binaries created here.
 * Sub-binaries are already Erlang binaries.
 */
void rke_bout_make_binaries(bin_out_t * out, ErlNifEnv * env)
{
    int i;
    response_binary_t * cur_bin = &out->bins[out->current_idx];
    // Trim last binary to size.
    if (!cur_bin->owned && out->offset < cur_bin->bin.size)
        enif_realloc_binary(&cur_bin->bin, out->offset);

    for (i = 0; i <= out->current_idx; ++i) {
        if (!out->bins[i].owned) {
            out->bins[i].term = enif_make_binary(env, &out->bins[i].bin);
            out->bins[i].owned = 1;
        }
    }
}

/* Convert data structure to final Erlang iolist or binary form. */
ERL_NIF_TERM rke_bout_to_term(bin_out_t * out, ErlNifEnv * env)
{
    // Special case a single binary, don't put it in a list.
    if (out->current_idx == 0)
        return out->bins[0].term;

    // Create list in reverse
    int i;
    ERL_NIF_TERM list = enif_make_list(env, 0);
    for (i = out->current_idx; i >= 0; --i) {
        ERL_NIF_TERM next_bin = out->bins[i].term;
        list = enif_make_list_cell(env, next_bin, list);
    }

    return list;
}

int rke_encode_obj(ErlNifEnv * env,
                    bin_out_t * out,
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
    
    rke_slice_from_bin(&value_bin, &input);
    rke_slice_from_bin(&key_bin, &key_slice);

    // Skip 53, 1 header, read vclock and sibling count.
    if (!rke_slice_skip(&input, 2)
        || !rke_read_bytes32(&input, &vc_slice)
        || !rke_read_big_endian_uint32(&input, &sib_count))
        return 0;

    uint8_t vclen_varsize = rke_varsize((uint64_t)vc_slice.size);
    uint8_t klen_varsize = rke_varsize((uint64_t)key_bin.size);

    size_t space_needed = 1 + 4 + 1 + klen_varsize + key_bin.size
        + 1 + 4 + 1 + vclen_varsize + vc_slice.size;

    /* Ensure enough room for 
     * objects field code : 1
     * objects size placeholder : 4
     * index pair key field code : 1
     * key size as varint : varsize(key_size)
     * key size
     * index pair object field code : 1
     * object size placeholder : 4
     * vclock field code : 1
     * length of vclock size as varint : varsize(vc_size)
     * size of vclock binary.
     */
    
    if (!rke_bout_make_room(out, space_needed))
        return 0;

    bin_location_t pair_size_loc, obj_size_loc;

    rke_bout_write_byte(out, 10);
    rke_bout_get_location(out, &pair_size_loc);
    rke_bout_skip(out, 4);
    rke_bout_write_byte(out, 10);
    rke_bout_write_varint(out, key_slice.size);
    rke_bout_write_bytes(out, key_slice);
    rke_bout_write_byte(out, 18);
    rke_bout_get_location(out, &obj_size_loc);
    rke_bout_skip(out, 4);
    rke_bout_write_byte(out, 18);
    rke_bout_write_varint(out, vc_slice.size);
    rke_bout_write_bytes(out, vc_slice);

    size_t tot_sib_size = 0;
    while (sib_count-- > 0) {
        slice_t val_slice;
        uint32_t meta_len, sib_size, content_size;

        // Read value, skip meta-data.
        if (!rke_read_bytes32(&input, &val_slice)
            || !rke_read_big_endian_uint32(&input, &meta_len)
            || !rke_slice_skip(&input, meta_len)
            // Skip first byte marker in value binary
            || !rke_slice_skip(&val_slice, 1))
            return 0;

        content_size = 1 + rke_varsize(val_slice.size) + val_slice.size;
        sib_size = 1 + rke_varsize(content_size) + content_size;

        if (!rke_bout_make_room(out, sib_size))
            return 0;

        rke_bout_write_byte(out, 10);
        rke_bout_write_varint(out, content_size);
        rke_bout_write_byte(out, 10);
        rke_bout_write_varint(out, val_slice.size);
        rke_bout_write_bytes(out, val_slice);

        tot_sib_size += sib_size;
    }

    size_t obj_sz = 1 + vclen_varsize + vc_slice.size + tot_sib_size;
    uint8_t * obj_sz_ptr = rke_bout_loc_ptr(out, obj_size_loc);
    size_t pair_sz = 1 + klen_varsize + key_slice.size + 1 + 4 + obj_sz;
    uint8_t * pair_sz_ptr = rke_bout_loc_ptr(out, pair_size_loc);

    rke_write_varint4(obj_sz, obj_sz_ptr);
    rke_write_varint4(pair_sz, pair_sz_ptr);

    return 1;
}

ERL_NIF_TERM rke_encode_csbucket_resp(ErlNifEnv* env, int argc,
                                 const ERL_NIF_TERM argv[])
{
    bin_out_t out;
    ERL_NIF_TERM list;

    if (argc < 1 || !enif_is_list(env, argv[0]))
        return enif_make_badarg(env);

    if (!rke_bout_init(env, &out))
        return enif_make_tuple2(env, ATOM_ERROR, ATOM_BAD_ALLOC);

    // Write message code for CS Bucket Response
    rke_bout_write_byte(&out, 41);

    // Convert list of KVs to a big PB iolist or binary.
    list = argv[0];
    while (!enif_is_empty_list(env, list)) {
        int tlen;
        const ERL_NIF_TERM * telements;
        ERL_NIF_TERM next;
        enif_get_list_cell(env, list, &next, &list);
        // Read {_, Key, Value} tuple and encode it.
        if (!enif_get_tuple(env, next, &tlen, &telements)
            || tlen != 3
            || !rke_encode_obj(env, &out, telements[1], telements[2])) {
            rke_bout_free(&out);
            return enif_make_tuple2(env, ATOM_ERROR, ATOM_INVALID_INPUT);
        }
    }

    rke_bout_make_binaries(&out, env);

    return enif_make_tuple2(env, ATOM_OK, rke_bout_to_term(&out, env));
}

static ErlNifFunc rke_nif_funcs[] =
{
    {"encode_csbucket_resp", 1, rke_encode_csbucket_resp}
};

static int rke_on_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    ATOM_OK = enif_make_atom(env, "ok");
    ATOM_ERROR = enif_make_atom(env, "error");
    ATOM_BAD_ALLOC = enif_make_atom(env, "bad_alloc"); 
    ATOM_INVALID_INPUT = enif_make_atom(env, "invalid_input");

    return 0;
}

ERL_NIF_INIT(riak_kv_encoding_nifs, rke_nif_funcs, &rke_on_load, NULL, NULL, NULL);
