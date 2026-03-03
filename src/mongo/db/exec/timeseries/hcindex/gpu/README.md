# GPU Acceleration for HCIndex

This module provides GPU-accelerated operations for HCIndex (High Cardinality Index)
timeseries queries. The primary focus is on accelerating AttributeTable scans and
filter operations. The implementation mimics the Render Pipeline of Game
Engines to a small degree. Pipelining GPUs tightly allows the best utilization
of PCIE bandwidth. Directly using libraries written for AI is a waste. Almost
all of theose libraries are very sloppy and wastes many CPU and GPU cycles
doing unnecessary things. Fundamentally the pipelines look like this;

```
CPU: --- Query -> AttributeTable -> Push-op-to-CommandBuffer -> Async-Wait() --->
GPU: -------- Compute Commands--> Copy-back-Results --- Compute Commands -------

```
The key here is to ensure that GPU is not stalled/waiting when there is work
on the CPU. And simililary CPU is not stalled/waiting when we can still push
work onto the GPU command buffer. Game engines does this pipelining most
efficiently. To get to that level, there is a good chunk of changes requied
in our query engine. More on this later.

`Execution Frame` manages a set of contexts. A frame is tied to a device and a
device may only ever have 1-frame. In system with multiple gpu devices, there
will be multiple execution frames. Within a device, parallel execution is
achieved by having different `Execution Contexts`. A given collection's GPU
memory structures are owned by a specific context. This mapping is determined by
the hash-id of the collection-uuid. Context owns GPUBuffers, Fences and other
Async operational constructs.  Complex (nested or aggregation queries) can
leverage two parallel knobs:
1. multiple command buffers where each command
buffer operates independently wherever possible (e.g. window aggregations).
2. multiple commands pushed onto the same command buffer and let gpu handle task
level parallelism wherever possible (e.g nested queries parallel sub-queries).

When a device is reset (by OS or other kernel events), all resources in the
frame is re-created. Data is re-uploaded to the GPU memory during such reset.
Parameters like `uploadStrategyOnReset` controls how aggresively the data
re-upload happens. When `uploadStrategyOnReset=="Balanced"` implies that the
system will auto balance the upload depending on the usage pattern and when this
is set of `"Lazy"`, no upload takes place until the first usage of the memory
happens. Setting this to `"All"` will force a reload of all data.

`Segmented GPU Memory` is another important concept used to partition the
GPU memory.
* L0 Segment: For tables that are actively being written to or queried frequently.
  * Higher priority, not evicted unless memory is critically low.
* L1 Segment: For tables that are read-only and less frequently accessed.
  * Subject to LAL eviction when memory pressure is high.

First goal of PoC is to prove the performance advantage compared to the CPU.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    HCIndex Query Engine                          │
├─────────────────────────────────────────────────────────────────┤
│  AttributeTable (CPU)  ──────►  GpuAttributeTable               │
│       │                              │                           │
│       │ uploadFromCpu()              │ filter()                  │
│       ▼                              ▼                           │
│  ┌─────────────────┐          ┌─────────────────┐               │
│  │ _columns        │          │ _gpuColumns     │               │
│  │ vector<vector>  │   ──►    │ GpuBuffer[]     │               │
│  │ uint32_t        │          │ Device Memory   │               │
│  └─────────────────┘          └─────────────────┘               │
├─────────────────────────────────────────────────────────────────┤
│                    GPU Kernels (HIP/CUDA)                        │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │ filterColumn    │  │ andBitmaps      │  │ compactRowIds   │  │
│  │ Kernel          │  │ Kernel          │  │ Kernel          │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## Platform Support

| Platform | Backend | Status |
|----------|---------|--------|
| AMD GPU (ROCm) | HIP | Primary |
| NVIDIA GPU | CUDA (via HIP) | Supported |
| CPU-only | Stub | Fallback |

## Files

- `gpu_common.h` - Platform abstraction macros (HIP/CUDA)
- `gpu_error.h` - Error handling utilities
- `gpu_buffer.h` - RAII GPU memory wrapper
- `gpu_device.h` - Device management utilities
- `gpu_attribute_table.h/cpp` - GPU-accelerated AttributeTable
- `kernels/filter_kernels.hip` - HIP kernels for filtering

## Building with GPU Support

### Prerequisites

1. Install ROCm (tested with 5.x+):
   ```bash
   # Ubuntu
   wget https://repo.radeon.com/amdgpu-install/latest/ubuntu/focal/amdgpu-install_5.4.50400-1_all.deb
   sudo apt install ./amdgpu-install_*.deb
   sudo amdgpu-install --usecase=rocm
   ```

2. Verify installation:
   ```bash
   rocminfo
   hipcc --version
   ```

### Manual Kernel Compilation

Until Bazel HIP rules are integrated:

```bash
cd src/mongo/db/exec/timeseries/hcindex/gpu

# Compile HIP kernel
hipcc -c kernels/filter_kernels.hip -o filter_kernels.o \
    -I../../../../.. \
    -DMONGO_CONFIG_GPU_HIP \
    -O3

# Link with MongoDB (integration TBD)
```

### Building MongoDB with GPU (Future)

```bash
bazel build install-mongod --define=gpu=hip
```

## Ideas To Explore

- [ ] GPU-accelerated bitmap index operations
- [ ] Parallel stream processing for multiple time windows
- [ ] GPU aggregation kernels (count, sum, avg)
- [ ] Integration with MongoDB query planner

