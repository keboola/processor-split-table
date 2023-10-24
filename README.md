# Split Table CLI / Processor

## Motivation

- Keboola components usually generate one uncompressed CSV file.
- Database backends support parallel import of multiple CSV slices.
  - Importing one large CSV is unnecessarily slow.
- Staging storage may not support large files.
  - For example, the maximum file size on Google Cloud Storage is 4GB.
- The standard `gzip` tool only works in one thread and is slow.

--------------------

- This utility addresses these issues and provides fast slicing and compression for CSV files.
- It can be run as a Keboola component/processor or as a separate CLI binary.


## CLI

The `slicer` CLI slices and optionally compresses the input table.
- Input table can be a single or a sliced CSV table.
- The input table may or may not be compressed.

### Download

You can download the CLI from the [GitHub Releases](https://github.com/keboola/processor-split-table/releases).

### Modes

- There are the following modes for slicing the input table.
- You can configure the mode using the `--mode` flag.
- The default mode is `bytes`:

#### `bytes`

- New slice is created when the `--bytes-per-slice` limit is reached.
- Bytes size is measured before output compression, if any.

#### `rows`

- New slice is created when the `--rows-per-slice` limit is reached.

#### `slices`

- The table is split into a fixed `--number-of-slices`.
- Each slice except the last must have at least `--min-bytes-per-slice`, it takes precedence.

###  Input and output table

- `--table-name` *required*
  - Table name for logging purposes.
- `--table-input-path` *required*
  -  Path to the input table, either a file or a directory with slices.
- `--table-input-manifest-path`
  - Path to the manifest of the input table.
  - It is used to get `delimiter` and `enclosure` fields, if any.
  - It can be omitted only if the table does not have a manifest.
- `--table-output-path` *required*
  - Directory where the slices of the output table will be written.
  - If it does not exist, it will be created, but the parent directory must exist.
- `--table-output-manifest-path` *required*
  - Path where the output manifest will be written.
  - The parent directory must exist.
  - The output manifest is a copy of the input manifest.
  - The `columns` field is set from the CSV header, if it is missing.

###  Environment Variables

- Each flag can be specified via an env variable with the `SLICER_` prefix.
- For example `--bytes-per-slice` flag can be specified via `SLICER_BYTES_PER_SLICE` env.

###  CPU and Memory Usage

- CPU usage and speed can be influenced by the `--gzip-concurrency` flag.
- Memory usage can be influenced by following flags:
  - `--buffer-size`
  - `--gzip-concurrency`
  - `--gzip-block-size`
  - `--memory-limit`


- Examples:
  - [Example 1](./docs/example1.png): Unsliced and uncompressed CSV table on the input:
    - Speed: ~ `200MB/s`
    - Memory usage: < `100MB`
  - [Example 2](./docs/example2.png): Sliced and compressed CSV table on the input:
    - Speed: ~ `100MB/s`
    - Memory usage: < `100MB`

### All Flags

<details>
  <summary>Expand</summary>


- `--buffer-size` *string*
  - Or `SLICER_BUFFER_SIZE` env.
  - Output buffer size when gzip compression is disabled. (default "20MB")
- `--bytes-per-slice` *string*
  - Or `SLICER_BYTES_PER_SLICE` env. 
  - Maximum size of a slice, for "bytes"" mode. (default "500MB")
- `--cpuprofile` *string*                
  - Or `SLICER_CPUPROFILE` env.
  - Write the CPU profile to the specified file.
- `--dump-config`
  - Or `SLICER_DUMP_CONFIG` env.
  - Print all parameters to the STDOUT.
- `--gzip`    
  - Or `SLICER_GZIP` env.
  - Enable gzip compression for slices. (default true)
- `--gzip-block-size` *string*
  - Or `SLICER_GZIP_BLOCK_SIZE` env.
  - Size of the one gzip block; allocated memory = concurrency * block size. (default "2MB")
- `--gzip-concurrency` *int*
  - Or `SLICER_GZIP_CONCURRENCY` env.
  - Number of parallel processed gzip blocks, 0 means the number of CPU threads.
- ` --gzip-level` *int*
  - Or `SLICER_GZIP_LEVEL` env.
  - GZIP compression level, range: 1 best speed - 9 best compression. (default 2)
- `--help`    
  - Or `SLICER_HELP` env.
  - Print help.
- `--memory-limit` *string*
  - Or `SLICER_MEMORY_LIMIT` env.
  - Soft memory limit, GOMEMLIMIT. (default "256MB")
- `--min-bytes-per-slice` *string*
  - Or `SLICER_MIN_BYTES_PER_SLICE` env.
  - Minimum size of a slice, for "slices" mode. (default "4MB")
- `--mode` *string*
  - Or `SLICER_MODE` env.
  - bytes, rows, or slices (default "bytes")
- `--number-of-slices` *int*
  - Or `SLICER_NUMBER_OF_SLICES` env.
  - Number of slices, for "slices" mode. (default 60)
- `--rows-per-slice` *int*
  - Or `SLICER_ROWS_PER_SLICE` env.
  - Maximum number of rows per slice, for "rows" mode. (default 1000000)
- `--table-input-manifest-path` *string*
  - Or `SLICER_TABLE_INPUT_MANIFEST_PATH` env.
  - Path to the manifest describing the input table, if any.
- `--table-input-path` *string*           
  - Or `SLICER_TABLE_INPUT_PATH` env.
  - Path to the input table, either a file or a directory with slices.
- `--table-name` *string*                 
  - Or `SLICER_TABLE_NAME` env.
  - Table name for logging purposes.
- `--table-output-manifest-path` *string*   
  - Or `SLICER_TABLE_OUTPUT_MANIFEST_PATH` env.
  - Path where the output manifest will be written.
- `--table-output-path` *string`           
  - Or `SLICER_TABLE_OUTPUT_PATH` env.
  - Directory where the slices of the output table will be written.

</details>


## Split Table Processor

<details>
  <summary>Expand</summary>

- Takes all CSV files in `/data/in/tables` and converts them to [sliced tables](https://developers.keboola.com/extend/common-interface/folders/#sliced-tables).
- The default slice size is `500MB`, the number of rows per slice can also be configured.
- The approximate speed in Keboola Connection is `200 MB / s` (if `gzip` disabled).
- Manifest is created if needed. Original manifest's keys are preserved.
- Header from CSV table is moved to manifest's `columns` key if input table is not headless.
- CSV delimiter and enclosure are loaded from manifest if set.
- Files and already sliced tables are copied without change.

## Usage

It supports optional parameters:

- `mode` - enum (`bytes`, `rows`, `slices`), default `bytes`
- `bytesPerSlice` (`string`/`int`) - for `mode = bytes`, maximum size of the one slice in bytes before compression, default `500MB`
- `rowsPerSlice` (`int`) - for `mode = rows`, maximum rows in the one slice, default `1 000 000`
- `numberOfSlices` (`int`) - for `mode = slices`, fixed number of slices, default `60`
- `minBytesPerSlice` (`string`/`int`) - for `mode = slices`, minimum size of the one slice in bytes before compression, default `4MB`.
- `gzip` (`bool`) - enable gzip compression, default `true`
- `gzipLevel` (`int`) - compression level, min `1` - the best speed), max `9` - the best compression, default `2`

## Sample configurations

Default parameters (`500 MB` per slice, gzip enabled):

```json
{
  "definition": {
    "component": "keboola.processor-split-table"
  }
}
```

Bytes mode:

```json
{
  "definition": {
    "component": "keboola.processor-split-table"
  },
  "parameters": {
    "mode": "bytes",
    "bytesPerSlice": "100MB"
  }
}
```

Rows mode:
```json
{
  "definition": {
    "component": "keboola.processor-split-table"
  },
  "parameters": {
    "mode": "rows",
    "rowsPerSlice": 5000000
  }
}
```

Slices mode:
```json
{
  "definition": {
    "component": "keboola.processor-split-table"
  },
  "parameters": {
    "mode": "slices",
    "numberOfSlices": 30,
    "minBytesPerSlice": "10MB"
  }
}
```

## Development

Clone this repository and init the workspace with following command:

```
git clone https://github.com/keboola/processor-split-table
cd processor-split-table
docker-compose build
```

Run the test suite and download the dependencies using this command:

```
docker-compose run --rm -u "$UID:$GID" dev make ci
```


Run bash in the container:

```
docker-compose run --rm -u "$UID:$GID" dev bash
```

</details>

## Integration

For information about deployment and integration with KBC, please refer to
the [deployment section of developers documentation](https://developers.keboola.com/extend/component/deployment/)
