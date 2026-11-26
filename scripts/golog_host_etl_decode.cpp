// Build: g++ -std=c++17 -O2 golog_host_etl_decode.cpp -municode -ltdh -ladvapi32 -lole32 -luuid -o golog-host-etl-decode.exe
// Usage: golog-host-etl-decode.exe disk.etl > events.jsonl
//        golog-host-etl-decode.exe --native-thread <decimal-tid> disk.etl > native.jsonl
// Native mode preserves all disk flushes for cross-thread analysis and emits FileIo path
// candidates from earlier FileKey name records. Later matching name records are kept.
// FileIo and DiskIo IRPs are independent.
#define _WIN32_WINNT 0x0601
#include <windows.h>
#include <evntrace.h>
#include <tdh.h>
#include <objbase.h>
#include <cstdio>
#include <string>
#include <vector>
#include <map>
#include <tuple>
#include <cstring>
#include <cstdint>
#include <cerrno>
#include <cstdlib>
#include <cwchar>
#include <limits>
#include <unordered_map>
#include <unordered_set>
#include <optional>

static const GUID disk = {0x3d6fa8d4,0xfe05,0x11d0,{0x9d,0xda,0x00,0xc0,0x4f,0xd7,0xba,0x7c}};
static const GUID kdisk = {0xc7bde69a,0xe1e0,0x4177,{0xb6,0xef,0x28,0x3a,0xd1,0x52,0x52,0x71}};
static const GUID vhd = {0xe2816346,0x87f4,0x4f85,{0x95,0xc3,0x0c,0x79,0x40,0x9a,0xa8,0x9d}};
static const GUID vsp = {0x10b3d268,0x9782,0x49a4,{0xaa,0xcc,0xa9,0x3c,0x54,0x82,0xcb,0x6f}};
static const GUID stor = {0xc4636a1e,0x7986,0x4646,{0xbf,0x10,0x7b,0xc3,0xb4,0xa7,0x6e,0x8e}};
static const GUID fileio = {0x90cbdc39,0x4a3e,0x11d1,{0x84,0xf4,0x00,0x00,0xf8,0x04,0x64,0xe3}};
static const GUID trace = {0x68fdd900,0x4a3e,0x11d1,{0x84,0xf4,0x00,0x00,0xf8,0x04,0x64,0xe3}};
static const GUID thread = {0x3d6fa8d1,0xfe05,0x11d0,{0x9d,0xda,0x00,0xc0,0x4f,0xd7,0xba,0x7c}};
enum Provider { ClassicDisk, KernelDisk, Vhd, Vsp, Storport, ClassicFile, Trace, ClassicThread, ProviderCount };
static const char* names[]={"classic_disk","kernel_disk","vhdmp","storage_vsp","storport","classic_file","trace_control","classic_thread"};
static const GUID* guids[]={&disk,&kdisk,&vhd,&vsp,&stor,&fileio,&trace,&thread};

static void string_json(const wchar_t *value) {
    int n = WideCharToMultiByte(CP_UTF8, 0, value, -1, nullptr, 0, nullptr, nullptr);
    std::string utf(n > 0 ? n : 1, 0);
    if (n > 0) WideCharToMultiByte(CP_UTF8, 0, value, -1, utf.data(), n, nullptr, nullptr);
    putchar('"');
    for (unsigned char c : utf) {
        if (!c) break;
        if (c == '"' || c == '\\') { putchar('\\'); putchar(c); }
        else if (c < 32) printf("\\u%04x", c);
        else putchar(c);
    }
    putchar('"');
}

using Key = std::tuple<unsigned,unsigned,unsigned,unsigned>;
struct Schema {
    std::vector<BYTE> data;
    bool cswitch_v5_prefix = false;
    unsigned decoded = 0, raw = 0, filtered = 0, unresolved = 0;
};
static std::map<Key, Schema> schemas;
static std::map<std::wstring, unsigned long long> provider_counts;
static unsigned long long records = 0, buffers = 0;
static unsigned long native_thread = 0;
static long long first_qpc = 0, last_qpc = 0;
static unsigned long long unmatched_file_flushes = 0;
static std::unordered_map<unsigned long long, long long> pending_file_flushes;
struct FileName { std::wstring name; long long qpc; unsigned opcode; };
static std::unordered_map<unsigned long long, FileName> file_names;
static std::unordered_set<unsigned long long> flush_file_keys;

using FormatProperty = ULONG (WINAPI *)(PTRACE_EVENT_INFO, PEVENT_MAP_INFO, ULONG, USHORT, USHORT, USHORT, USHORT, PBYTE, PULONG, PWCHAR, PUSHORT);
static const auto format_property = reinterpret_cast<FormatProperty>(GetProcAddress(GetModuleHandleW(L"tdh.dll"), "TdhFormatProperty"));

static bool file_name_opcode(unsigned opcode) {
    return opcode == 0 || opcode == 32 || opcode == 35 || opcode == 36;
}

static bool native_candidate(unsigned kind, unsigned opcode) {
    switch (kind) {
    case ClassicDisk: return opcode == 14 || opcode == 15;
    case ClassicFile: return file_name_opcode(opcode) || opcode == 73 || opcode == 76;
    case ClassicThread: return opcode == 36 || opcode == 50;
    default: return false;
    }
}

using Fields = std::vector<std::pair<const wchar_t *, std::wstring>>;

// Windows can emit classic Thread/CSwitch v5 without local TDH metadata.
// PerfView's KernelTraceEventParser documents the stable 24-byte prefix:
// https://github.com/microsoft/perfview/blob/main/src/TraceEvent/Parsers/KernelTraceEventParser.cs
static uint32_t read_u32(const BYTE *data) {
    uint32_t value;
    std::memcpy(&value, data, sizeof(value));
    return value;
}

static const wchar_t *cswitch_v5_fields[] = {
    L"NewThreadId", L"OldThreadId", L"NewThreadPriority", L"OldThreadPriority",
    L"OldThreadWaitReason", L"OldThreadWaitMode", L"OldThreadState",
    L"OldThreadWaitIdealProcessor", L"NewThreadWaitTime",
};

static bool decode_cswitch_v5(PEVENT_RECORD row, Fields &fields, Schema &schema) {
    const auto *payload = static_cast<const BYTE *>(row->UserData);
    if (!payload || row->UserDataLength < 24) {
        ++schema.unresolved;
        return false;
    }
    const auto incoming = read_u32(payload);
    const auto outgoing = read_u32(payload + 4);
    if (incoming != native_thread && outgoing != native_thread) {
        ++schema.filtered;
        return false;
    }
    unsigned values[] = {incoming, outgoing, payload[8], payload[9], payload[12], payload[13],
                         payload[14], payload[15], read_u32(payload + 16)};
    for (unsigned n = 0; n < sizeof(values) / sizeof(values[0]); ++n)
        fields.emplace_back(cswitch_v5_fields[n], std::to_wstring(values[n]));
    return true;
}

static bool decode_fields(PEVENT_RECORD row, Schema &schema, Fields &fields,
                          unsigned &offset, unsigned limit = UINT_MAX) {
    if (schema.data.empty() || !format_property) return false;
    auto *info = reinterpret_cast<PTRACE_EVENT_INFO>(schema.data.data());
    const auto *payload = static_cast<const BYTE *>(row->UserData);
    ULONG ptr_size = (row->EventHeader.Flags & EVENT_HEADER_FLAG_32_BIT_HEADER) ? 4 : 8;
    for (ULONG n = static_cast<ULONG>(fields.size()); n < info->TopLevelPropertyCount && n < limit; ++n) {
        const auto &p = info->EventPropertyInfoArray[n];
        if (p.Flags & (PropertyStruct | PropertyParamLength | PropertyParamCount) || p.count > 1) return false;
        if (!payload || offset > row->UserDataLength) return false;
        wchar_t output[4096];
        ULONG output_size = sizeof(output);
        USHORT consumed = 0;
        if (format_property(info, nullptr, ptr_size, p.nonStructType.InType, p.nonStructType.OutType,
                            p.length, static_cast<USHORT>(row->UserDataLength - offset),
                            const_cast<BYTE *>(payload + offset), &output_size, output, &consumed) != ERROR_SUCCESS)
            return false;
        fields.emplace_back(reinterpret_cast<const wchar_t *>(schema.data.data() + p.NameOffset), output);
        offset += consumed;
    }
    return fields.size() == info->TopLevelPropertyCount && offset == row->UserDataLength;
}

static const wchar_t *field(const Fields &fields, const wchar_t *name) {
    for (const auto &[key, value] : fields)
        if (_wcsicmp(key, name) == 0) return value.c_str();
    return nullptr;
}

static unsigned long long number(const wchar_t *value) {
    if (!value || !*value) return 0;
    wchar_t *end = nullptr;
    int base = value[0] == L'0' && (value[1] == L'x' || value[1] == L'X') ? 16 : 10;
    auto result = wcstoull(value, &end, base);
    return *end ? 0 : result;
}

static unsigned long long irp(const Fields &fields) {
    const wchar_t *value = field(fields, L"IrpPtr");
    return number(value ? value : field(fields, L"Irp"));
}

enum class Selection { Include, Filter, Unresolved };

static Selection select_native(unsigned kind, unsigned opcode, long long qpc, const Fields &fields,
                               const FileName *&path_candidate, std::optional<long long> &flush_qpc) {
    if (kind == ClassicDisk) return Selection::Include;
    if (kind == ClassicThread) {
        if (opcode == 50) {
            const auto *target = field(fields, L"TThreadId");
            if (!target) return Selection::Unresolved;
            return number(target) == native_thread ? Selection::Include : Selection::Filter;
        }
        auto *incoming = field(fields, L"NewThreadId");
        auto *outgoing = field(fields, L"OldThreadId");
        if (number(incoming) == native_thread || number(outgoing) == native_thread) return Selection::Include;
        return incoming && outgoing ? Selection::Filter : Selection::Unresolved;
    }
    if (file_name_opcode(opcode)) {
        auto key = number(field(fields, L"FileObject"));
        const wchar_t *name = field(fields, L"FileName");
        if (!key || !name) return Selection::Unresolved;
        if (opcode == 35) file_names.erase(key);
        else if (*name) file_names.insert_or_assign(key, FileName{name, qpc, opcode});
        // Preserve later name/rundown records for keys already seen in a kept Flush.
        return flush_file_keys.count(key) ? Selection::Include : Selection::Filter;
    }
    auto identity = irp(fields);
    if (!identity) return Selection::Unresolved;
    if (opcode == 76) {
        auto it = pending_file_flushes.find(identity);
        if (it == pending_file_flushes.end() || it->second > qpc) return Selection::Filter;
        flush_qpc = it->second;
        pending_file_flushes.erase(it);
        --unmatched_file_flushes;
        return Selection::Include;
    }
    const auto *target = field(fields, L"TTID");
    pending_file_flushes.erase(identity);
    if (!target) return Selection::Unresolved;
    if (number(target) != native_thread) return Selection::Filter;
    ++unmatched_file_flushes;
    pending_file_flushes.insert_or_assign(identity, qpc);
    const auto file_key = number(field(fields, L"FileKey"));
    if (file_key) flush_file_keys.insert(file_key);
    auto name = file_names.find(file_key);
    if (name != file_names.end() && name->second.qpc <= qpc) path_candidate = &name->second;
    return Selection::Include;
}

static void CALLBACK event(PEVENT_RECORD row) {
    ++records;
    const auto &h = row->EventHeader;
    const auto &d = h.EventDescriptor;
    if (native_thread && h.TimeStamp.QuadPart > 0) {
        const auto qpc = h.TimeStamp.QuadPart;
        if (!first_qpc || qpc < first_qpc) first_qpc = qpc;
        if (qpc > last_qpc) last_qpc = qpc;
    }
    wchar_t provider[40]{};
    if (!native_thread) {
        StringFromGUID2(h.ProviderId, provider, 40);
        ++provider_counts[provider];
    }
    unsigned kind = 0;
    for (; kind < ProviderCount && !IsEqualGUID(h.ProviderId, *guids[kind]); ++kind) {}
    if (native_thread) {
        if (!native_candidate(kind, d.Opcode)) return;
    } else {
        if (kind >= ClassicThread) return;
        if (kind == ClassicDisk && d.Opcode != 14 && d.Opcode != 15 && d.Opcode < 32) return;
        if (kind == ClassicFile && !file_name_opcode(d.Opcode)) return;
    }
    Key key{kind,d.Id,d.Opcode,d.Version};
    auto [it, fresh] = schemas.try_emplace(key);
    Schema &schema = it->second;
    if (fresh) {
        ULONG size=0;
        if (TdhGetEventInformation(row, 0, nullptr, nullptr, &size)==ERROR_INSUFFICIENT_BUFFER) {
            schema.data.resize(size);
            if (TdhGetEventInformation(row,0,nullptr,reinterpret_cast<PTRACE_EVENT_INFO>(schema.data.data()),&size)!=ERROR_SUCCESS)
                schema.data.clear();
        }
        schema.cswitch_v5_prefix = native_thread && kind == ClassicThread &&
                                   d.Opcode == 36 && d.Version == 5 && schema.data.empty();
        if (native_thread) StringFromGUID2(h.ProviderId, provider, 40);
        printf("{\"type\":\"schema\",\"provider\":\"%s\",\"guid\":", names[kind]);
        string_json(provider);
        printf(",\"id\":%u,\"opcode\":%u,\"version\":%u",d.Id,d.Opcode,d.Version);
        if (!schema.data.empty()) {
            auto *info = reinterpret_cast<PTRACE_EVENT_INFO>(schema.data.data());
            const auto *base=schema.data.data();
            printf(",\"fields\":[");
            for(ULONG n=0;n<info->TopLevelPropertyCount;++n) {
                const auto &p=info->EventPropertyInfoArray[n];
                if(n) putchar(',');
                printf("{\"name\":"); string_json(reinterpret_cast<const wchar_t *>(base+p.NameOffset));
                printf(",\"in\":%u,\"out\":%u,\"flags\":%u}",p.nonStructType.InType,p.nonStructType.OutType,p.Flags);
            }
            putchar(']');
            for (auto item : {std::pair<const char*,ULONG>{"task",info->TaskNameOffset}, {"opcode_name",info->OpcodeNameOffset}}) {
                if (item.second) { printf(",\"%s\":",item.first); string_json(reinterpret_cast<const wchar_t*>(base+item.second)); }
            }
        }
        if (schema.cswitch_v5_prefix) {
            printf(",\"schema_source\":\"TraceEvent\",\"payload_prefix_bytes\":24,\"fields\":[");
            bool comma = false;
            for (const auto *name : cswitch_v5_fields) {
                if (comma) putchar(',');
                comma = true;
                printf("{\"name\":"); string_json(name); putchar('}');
            }
            putchar(']');
        }
        puts("}");
    }
    // ProcessTrace dispatches these callbacks serially. Reuse field storage.
    static Fields fields;
    fields.clear();
    unsigned offset = 0;
    const FileName *path_candidate = nullptr;
    std::optional<long long> flush_qpc;
    bool complete = false;
    // The classic scheduler schemas put target TIDs first. Decode the rest only
    // for matching threads. If the names differ, try the full schema.
    if (schema.cswitch_v5_prefix) {
        complete = decode_cswitch_v5(row, fields, schema);
        if (!complete) return;
    } else if (native_thread && kind == ClassicThread) {
        complete = decode_fields(row, schema, fields, offset, d.Opcode == 50 ? 1 : 2);
        auto selection = select_native(kind, d.Opcode, h.TimeStamp.QuadPart, fields, path_candidate, flush_qpc);
        if (selection == Selection::Unresolved && !complete) {
            complete = decode_fields(row, schema, fields, offset);
            selection = select_native(kind, d.Opcode, h.TimeStamp.QuadPart, fields, path_candidate, flush_qpc);
        }
        if (selection != Selection::Include) {
            ++(selection == Selection::Unresolved ? schema.unresolved : schema.filtered);
            return;
        }
        if (!complete) complete = decode_fields(row, schema, fields, offset);
    } else {
        complete = decode_fields(row, schema, fields, offset);
        if (native_thread) {
            auto selection = select_native(kind, d.Opcode, h.TimeStamp.QuadPart, fields, path_candidate, flush_qpc);
            if (selection != Selection::Include) {
                ++(selection == Selection::Unresolved ? schema.unresolved : schema.filtered);
                return;
            }
        }
    }
    printf("{\"type\":\"event\",\"provider\":\"%s\",\"id\":%u,\"opcode\":%u,\"version\":%u,\"qpc\":%lld,\"pid\":%lu,\"tid\":%lu,\"cpu\":%u",
           names[kind],d.Id,d.Opcode,d.Version,h.TimeStamp.QuadPart,h.ProcessId,h.ThreadId,row->BufferContext.ProcessorIndex);
    if (!IsEqualGUID(h.ActivityId,GUID_NULL)) {
        wchar_t activity[40]{}; StringFromGUID2(h.ActivityId,activity,40);
        printf(",\"activity\":"); string_json(activity);
    }
    if (!schema.data.empty() || schema.cswitch_v5_prefix) {
        printf(",\"fields\":{");
        bool comma = false;
        for (const auto &[name, value] : fields) {
            if (comma) putchar(',');
            comma = true;
            string_json(name); putchar(':'); string_json(value.c_str());
        }
        putchar('}');
    }
    if (path_candidate) {
        printf(",\"file_path_candidate\":");
        string_json(path_candidate->name.c_str());
        printf(",\"file_path_source_qpc\":%lld,\"file_path_source_opcode\":%u",
               path_candidate->qpc, path_candidate->opcode);
    }
    if (flush_qpc) printf(",\"file_flush_start_qpc\":%lld", *flush_qpc);
    if (complete) ++schema.decoded;
    else {
        ++schema.raw;
        printf(",\"raw_hex\":\"");
        const auto *payload = static_cast<const BYTE *>(row->UserData);
        for (unsigned i=0;i<row->UserDataLength;++i) printf("%02x",payload[i]);
        putchar('"');
    }
    puts("}");
}

static ULONG WINAPI buffer(PEVENT_TRACE_LOGFILEW) { ++buffers; return TRUE; }

int wmain(int argc,wchar_t **argv) {
    if (argc == 4 && wcscmp(argv[1], L"--native-thread") == 0 &&
        argv[2][0] && argv[2][wcsspn(argv[2], L"0123456789")] == L'\0') {
        errno = 0;
        native_thread = wcstoul(argv[2], nullptr, 10);
        if (errno == ERANGE || native_thread == std::numeric_limits<unsigned long>::max()) native_thread = 0;
    }
    if ((argc != 2 || wcscmp(argv[1], L"--native-thread") == 0) && !native_thread) {
        fprintf(stderr, "Usage: golog-host-etl-decode.exe [--native-thread <decimal-tid>] disk.etl\n");
        return 2;
    }
    static char output_buffer[1<<20]; setvbuf(stdout,output_buffer,_IOFBF,sizeof(output_buffer));
    EVENT_TRACE_LOGFILEW log{};
    log.LogFileName=argv[argc - 1];
    log.ProcessTraceMode=PROCESS_TRACE_MODE_EVENT_RECORD|PROCESS_TRACE_MODE_RAW_TIMESTAMP;
    log.EventRecordCallback=event;
    log.BufferCallback=buffer;
    TRACEHANDLE handle=OpenTraceW(&log);
    if(handle==INVALID_PROCESSTRACE_HANDLE) { fprintf(stderr,"OpenTraceW: %lu\n",GetLastError()); return 1; }
    const auto &h=log.LogfileHeader;
    printf("{\"type\":\"header\",\"raw_timestamp\":true,\"clock_type\":%lu,\"perf_frequency\":%lld,\"start_filetime\":%lld,\"end_filetime\":%lld,\"boot_filetime\":%lld,\"pointer_size\":%lu,\"processors\":%lu,\"buffers_written\":%lu,\"buffers_lost\":%lu,\"events_lost\":%lu}\n",
           h.ReservedFlags,h.PerfFreq.QuadPart,h.StartTime.QuadPart,h.EndTime.QuadPart,h.BootTime.QuadPart,h.PointerSize,h.NumberOfProcessors,h.BuffersWritten,h.BuffersLost,h.EventsLost);
    ULONG status=ProcessTrace(&handle,1,nullptr,nullptr);
    CloseTrace(handle);
    printf("{\"type\":\"summary\",\"process_status\":%lu,\"records\":%llu,\"buffers\":%llu,\"events_lost\":%lu,\"provider_counts\":{",status,records,buffers,log.EventsLost);
    bool comma=false;
    for(const auto &[guid,count]:provider_counts) {
        if(comma) putchar(','); comma=true;
        string_json(guid.c_str()); printf(":%llu",count);
    }
    putchar('}');
    if (native_thread) {
        printf(",\"native_thread\":%lu,\"file_flushes_without_op_end\":%llu,\"first_qpc\":%lld,\"last_qpc\":%lld",
               native_thread, unmatched_file_flushes, first_qpc, last_qpc);
    }
    printf(",\"decoder_counts\":[");
    comma=false;
    for(const auto &[key,schema]:schemas) {
        auto [kind,id,opcode,version]=key;
        if(comma) putchar(','); comma=true;
        printf("{\"provider\":\"%s\",\"id\":%u,\"opcode\":%u,\"version\":%u,\"decoded\":%u,\"raw_included\":%u",names[kind],id,opcode,version,schema.decoded,schema.raw);
        if (native_thread) printf(",\"filtered\":%u,\"filter_unresolved\":%u", schema.filtered, schema.unresolved);
        putchar('}');
    }
    puts("]}");
    return status==ERROR_SUCCESS && fflush(stdout)==0 && !ferror(stdout)?0:1;
}
