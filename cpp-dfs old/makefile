module_files = $(wildcard modules/**/*.cppm)
module_impl_files = $(wildcard modules/**/*.cpp)
module_binaries = $(patsubst modules/%.cppm,build/modules/%.pcm,$(module_files))
module_impl_objects = $(patsubst modules/%.cpp,build/objects/%.o,$(module_impl_files))
CXX          = /opt/homebrew/opt/llvm/bin/clang++
MODULE_PATHS = $(shell find build/modules -type d)
CXX_FLAGS    = -std=c++20 -O2 -fmodules $(addprefix -fprebuilt-module-path=,$(MODULE_PATHS))
source       = apps
modules      = build/modules
objects      = build/objects
output       = build/output

# Get all directories in 'apps' and convert them into names for binaries and object folders
app_dirs = $(wildcard $(source)/*)
app_binaries = $(patsubst $(source)/%, $(output)/%, $(notdir $(app_dirs)))

# Make rules
all: modules $(app_binaries)

modules: $(module_binaries) $(module_impl_objects)

clean: 
	@rm -fr build

# Create object directories for each app
$(objects)/%/:
	@mkdir -p $@

# Create output directories for each binary
$(output)/%:
	@mkdir -p $@

# Compile module interface files to .pcm (nested structure)
build/modules/%.pcm: modules/%.cppm
	@mkdir -p $(dir $@)
	$(CXX) $(CXX_FLAGS) --precompile $< -o $@

# Compile module implementation files to .o
build/objects/%.o: modules/%.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXX_FLAGS) -c $< -o $@

# Compile source files for each app (each folder under apps)
$(objects)/%/main.o: $(source)/%/main.cpp | $(objects)/%/
	$(CXX) $(CXX_FLAGS) -c $< -o $@

# Link the application binaries for each app
$(output)/%: $(objects)/%/main.o $(module_binaries) $(module_impl_objects) | $(output)/%/
	$(CXX) $(CXX_FLAGS) $^ -o $@

# Default run command (runs the client binary by default)
run: all
	$(output)/hdfsc

# Run specific binaries
run_hdfsc:
	$(output)/hdfsc

run_hdfsnn:
	$(output)/hdfsnn

run_hdfsdn:
	$(output)/hdfsdn