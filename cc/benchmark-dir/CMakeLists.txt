set(BENCHMARK_HEADERS
  file.h
)

set(BENCHMARK_SOURCES
)

ADD_FASTER_BENCHMARK(benchmark)
ADD_FASTER_BENCHMARK(benchmark-single-length)

add_executable(process_ycsb process_ycsb.cc)
