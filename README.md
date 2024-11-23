# Large Buffer Sort Program

This program is a high-performance external sorting implementation, designed to handle sorting operations on datasets too large to fit in memory. It uses multi-way merge sort with buffering to minimize disk I/O operations and is optimized for scenarios involving very large input sizes.

## Features

- **Efficient Disk I/O:** Minimizes disk reads and writes using buffering techniques.
- **Multi-way Merge Sort:** Implements a scalable sorting algorithm suitable for external sorting.
- **Performance Metrics:** Tracks operations (sorting phases, reads, writes, merges) and compares actual disk operations to expected values.
- **Modular Design:** Can be adjusted for different buffer sizes (`b`) and number of buffers (`n`).

---

## How It Works

1. **Phase 1: Initial Sorting**
   - Reads blocks of data (`b * n` records) from the input file.
   - Sorts each block in memory and writes it to temporary files.

2. **Phase 2: Multi-way Merge**
   - Uses a priority queue to efficiently merge sorted runs from temporary files into a single sorted output.
   - Writes the final sorted data to the output file.

3. **Metrics Calculation:**
   - Counts the number of sorting phases, read/write operations, and merge steps.
   - Compares actual disk operations against theoretical calculations.
