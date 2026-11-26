// Build: g++ -std=c++17 -O2 golog_host_etl_decode.cpp -municode -ltdh -ladvapi32 -lole32 -luuid -o golog-host-etl-decode.exe
// Usage: golog-host-etl-decode.exe disk.etl > events.jsonl
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

static const GUID disk = {0x3d6fa8d4,0xfe05,0x11d0,{0x9d,0xda,0x00,0xc0,0x4f,0xd7,0xba,0x7c}};
static const GUID kdisk = {0xc7bde69a,0xe1e0,0x4177,{0xb6,0xef,0x28,0x3a,0xd1,0x52,0x52,0x71}};
static const GUID vhd = {0xe2816346,0x87f4,0x4f85,{0x95,0xc3,0x0c,0x79,0x40,0x9a,0xa8,0x9d}};
static const GUID vsp = {0x10b3d268,0x9782,0x49a4,{0xaa,0xcc,0xa9,0x3c,0x54,0x82,0xcb,0x6f}};
static const GUID stor = {0xc4636a1e,0x7986,0x4646,{0xbf,0x10,0x7b,0xc3,0xb4,0xa7,0x6e,0x8e}};
static const GUID fileio = {0x90cbdc39,0x4a3e,0x11d1,{0x84,0xf4,0x00,0x00,0xf8,0x04,0x64,0xe3}};
static const GUID trace = {0x68fdd900,0x4a3e,0x11d1,{0x84,0xf4,0x00,0x00,0xf8,0x04,0x64,0xe3}};
static const char* names[]={"classic_disk","kernel_disk","vhdmp","storage_vsp","storport","classic_file","trace_control"};
static const GUID* guids[]={&disk,&kdisk,&vhd,&vsp,&stor,&fileio,&trace};

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
struct Schema { std::vector<BYTE> data; unsigned decoded = 0, raw = 0; };
static std::map<Key, Schema> schemas;
static std::map<std::wstring, unsigned long long> provider_counts;
static unsigned long long records = 0, buffers = 0;

using FormatProperty = ULONG (WINAPI *)(PTRACE_EVENT_INFO, PEVENT_MAP_INFO, ULONG, USHORT, USHORT, USHORT, USHORT, PBYTE, PULONG, PWCHAR, PUSHORT);
static const auto format_property = reinterpret_cast<FormatProperty>(GetProcAddress(GetModuleHandleW(L"tdh.dll"), "TdhFormatProperty"));

static void CALLBACK event(PEVENT_RECORD row) {
    ++records;
    const auto &h = row->EventHeader;
    const auto &d = h.EventDescriptor;
    wchar_t provider[40]{};
    StringFromGUID2(h.ProviderId, provider, 40);
    ++provider_counts[provider];
    unsigned kind = 0;
    for (; kind < 7 && !IsEqualGUID(h.ProviderId, *guids[kind]); ++kind) {}
    if (kind == 7) return;
    if (kind == 0 && d.Opcode != 14 && d.Opcode != 15 && d.Opcode < 32) return;
    if (kind == 5 && d.Opcode != 0 && d.Opcode != 32 && d.Opcode != 35 && d.Opcode != 36) return;
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
        puts("}");
    }
    printf("{\"type\":\"event\",\"provider\":\"%s\",\"id\":%u,\"opcode\":%u,\"version\":%u,\"qpc\":%lld,\"pid\":%lu,\"tid\":%lu,\"cpu\":%u",
           names[kind],d.Id,d.Opcode,d.Version,h.TimeStamp.QuadPart,h.ProcessId,h.ThreadId,row->BufferContext.ProcessorIndex);
    if (!IsEqualGUID(h.ActivityId,GUID_NULL)) {
        wchar_t activity[40]{}; StringFromGUID2(h.ActivityId,activity,40);
        printf(",\"activity\":"); string_json(activity);
    }
    const auto *payload=static_cast<const BYTE*>(row->UserData);
    unsigned offset=0;
    bool complete=false;
    if (!schema.data.empty()) {
        auto *info=reinterpret_cast<PTRACE_EVENT_INFO>(schema.data.data());
        ULONG ptrSize=(h.Flags & EVENT_HEADER_FLAG_32_BIT_HEADER)?4:8;
        printf(",\"fields\":{");
        ULONG n=0;
        for (;n<info->TopLevelPropertyCount;++n) {
            const auto &p=info->EventPropertyInfoArray[n];
            if (p.Flags & (PropertyStruct | PropertyParamLength | PropertyParamCount) || p.count > 1) break;
            wchar_t output[4096]{};
            ULONG output_size=sizeof(output);
            USHORT consumed=0;
            if (!format_property || format_property(info,nullptr,ptrSize,p.nonStructType.InType,p.nonStructType.OutType,p.length,
                                  static_cast<USHORT>(row->UserDataLength-offset),const_cast<BYTE*>(payload+offset),
                                  &output_size,output,&consumed)!=ERROR_SUCCESS) break;
            if(n) putchar(',');
            string_json(reinterpret_cast<const wchar_t*>(schema.data.data()+p.NameOffset)); putchar(':'); string_json(output);
            offset+=consumed;
        }
        complete=n==info->TopLevelPropertyCount && offset==row->UserDataLength;
        putchar('}');
    }
    if (complete) ++schema.decoded;
    else {
        ++schema.raw;
        printf(",\"raw_hex\":\"");
        for (unsigned i=0;i<row->UserDataLength;++i) printf("%02x",payload[i]);
        putchar('"');
    }
    puts("}");
}

static ULONG WINAPI buffer(PEVENT_TRACE_LOGFILEW) { ++buffers; return TRUE; }

int wmain(int argc,wchar_t **argv) {
    if(argc!=2) return 2;
    static char output_buffer[1<<20]; setvbuf(stdout,output_buffer,_IOFBF,sizeof(output_buffer));
    EVENT_TRACE_LOGFILEW log{};
    log.LogFileName=argv[1];
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
    printf("},\"decoder_counts\":[");
    comma=false;
    for(const auto &[key,schema]:schemas) {
        auto [kind,id,opcode,version]=key;
        if(comma) putchar(','); comma=true;
        printf("{\"provider\":\"%s\",\"id\":%u,\"opcode\":%u,\"version\":%u,\"decoded\":%u,\"raw_included\":%u}",names[kind],id,opcode,version,schema.decoded,schema.raw);
    }
    puts("]}");
    return status==ERROR_SUCCESS && fflush(stdout)==0 && !ferror(stdout)?0:1;
}
