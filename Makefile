CXX      := g++
CXXFLAGS := -std=c++14 -O2 -Wall -Wextra -Wshadow -Iinclude

# Detect OS for path separator / executable suffix
ifeq ($(OS),Windows_NT)
  EXE  := .exe
  RM   := del /Q
  CXXFLAGS += -static
else
  EXE  :=
  RM   := rm -f
endif

# ── Source files ───────────────────────────────────────────
SRC_DIR  := src
SRCS     := $(SRC_DIR)/plan.cpp \
            $(SRC_DIR)/catalog.cpp \
            $(SRC_DIR)/parser.cpp \
            $(SRC_DIR)/executor.cpp \
            $(SRC_DIR)/rewriter.cpp \
            $(SRC_DIR)/cost_model.cpp \
            $(SRC_DIR)/join_order.cpp

MAIN_SRC := $(SRC_DIR)/main.cpp
OBJS     := $(SRCS:.cpp=.o)
MAIN_OBJ := $(MAIN_SRC:.cpp=.o)

# ── Targets ────────────────────────────────────────────────
TARGET   := qopt$(EXE)

.PHONY: all clean tests bench

all: $(TARGET)

$(TARGET): $(OBJS) $(MAIN_OBJ)
	$(CXX) $(CXXFLAGS) -o $@ $^

# Object file rule
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -I$(SRC_DIR) -c -o $@ $<

# ── Tests ──────────────────────────────────────────────────
TEST_DIR := tests
TEST_BINS := $(TEST_DIR)/test_parser$(EXE) \
             $(TEST_DIR)/test_rewriter$(EXE) \
             $(TEST_DIR)/test_cost$(EXE) \
             $(TEST_DIR)/test_join_order$(EXE) \
             $(TEST_DIR)/test_e2e$(EXE)

tests: $(TEST_BINS)
	@echo "Running all tests..."
ifeq ($(OS),Windows_NT)
	@.\tests\test_parser$(EXE) && echo "  PASS test_parser" || echo "  FAIL test_parser"
	@.\tests\test_rewriter$(EXE) && echo "  PASS test_rewriter" || echo "  FAIL test_rewriter"
	@.\tests\test_cost$(EXE) && echo "  PASS test_cost" || echo "  FAIL test_cost"
	@.\tests\test_join_order$(EXE) && echo "  PASS test_join_order" || echo "  FAIL test_join_order"
	@.\tests\test_e2e$(EXE) && echo "  PASS test_e2e" || echo "  FAIL test_e2e"
else
	@for bin in $(TEST_BINS); do \
		echo "  $$bin"; \
		./$$bin && echo "  PASS" || echo "  FAIL"; \
	done
endif

$(TEST_DIR)/test_parser$(EXE): $(OBJS) $(TEST_DIR)/test_parser.cpp
	$(CXX) $(CXXFLAGS) -I$(SRC_DIR) -o $@ $(OBJS) $(TEST_DIR)/test_parser.cpp

$(TEST_DIR)/test_rewriter$(EXE): $(OBJS) $(TEST_DIR)/test_rewriter.cpp
	$(CXX) $(CXXFLAGS) -I$(SRC_DIR) -o $@ $(OBJS) $(TEST_DIR)/test_rewriter.cpp

$(TEST_DIR)/test_cost$(EXE): $(OBJS) $(TEST_DIR)/test_cost.cpp
	$(CXX) $(CXXFLAGS) -I$(SRC_DIR) -o $@ $(OBJS) $(TEST_DIR)/test_cost.cpp

$(TEST_DIR)/test_join_order$(EXE): $(OBJS) $(TEST_DIR)/test_join_order.cpp
	$(CXX) $(CXXFLAGS) -I$(SRC_DIR) -o $@ $(OBJS) $(TEST_DIR)/test_join_order.cpp

$(TEST_DIR)/test_e2e$(EXE): $(OBJS) $(TEST_DIR)/test_e2e.cpp
	$(CXX) $(CXXFLAGS) -I$(SRC_DIR) -o $@ $(OBJS) $(TEST_DIR)/test_e2e.cpp

# ── Data generator ─────────────────────────────────────────
GEN := benchmark/gen_data$(EXE)

$(GEN): benchmark/gen_data.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

BENCH_BATCH := benchmark/run_bench_batch$(EXE)

$(BENCH_BATCH): benchmark/run_bench_batch.cpp $(OBJS)
	$(CXX) $(CXXFLAGS) -Isrc -o $@ $(OBJS) benchmark/run_bench_batch.cpp

bench: all $(GEN) $(BENCH_BATCH)
ifeq ($(OS),Windows_NT)
	@if not exist benchmark\benchdata mkdir benchmark\benchdata
	$(GEN) benchmark\benchdata
	$(BENCH_BATCH) --data benchmark/benchdata --out benchmark/benchmark_results.txt
else
	@mkdir -p benchmark/benchdata
	$(GEN) benchmark/benchdata
	./$(BENCH_BATCH) --data benchmark/benchdata --out benchmark/benchmark_results.txt
endif

# ── Clean ──────────────────────────────────────────────────
clean:
ifeq ($(OS),Windows_NT)
	-@if exist $(TARGET) del /Q $(TARGET) 2>NUL
	-@if exist src\*.o del /Q src\*.o 2>NUL
	-@if exist tests\test_*.exe del /Q tests\test_*.exe 2>NUL
	-@if exist benchmark\gen_data.exe del /Q benchmark\gen_data.exe 2>NUL
else
	$(RM) $(TARGET) $(OBJS) $(MAIN_OBJ) $(TEST_BINS) $(GEN) 2>/dev/null; true
endif
