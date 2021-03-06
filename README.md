# Split Table Processor
[![Build Status](https://github.com/keboola/processor-split-table/actions/workflows/push.yml/badge.svg)](https://github.com/keboola/processor-split-table/actions)

- Takes all CSV files in `/data/in/tables` and converts them to [sliced tables](https://developers.keboola.com/extend/common-interface/folders/#sliced-tables).
- The default slice size is `500MiB`, the number of rows per slice can also be configured.
- The approximate speed in Keboola Connection is `200 MiB / s` (if `gzip` disabled).
- Manifest is created if needed. Original manifest's keys are preserved.
- Header from CSV table is moved to manifest's `columns` key if input table is not headless.
- CSV delimiter and enclosure are loaded from manifest if set.
- Files and already sliced tables are copied without change.

# Usage

It supports optional parameters:

- `mode` - enum (`bytes`, `rows`, `slices`), default `bytes`
- `bytesPerSlice` (`int`) - for `mode = bytes`, maximum size of the one slice in bytes before compression, default `524 288 000` (`500 MiB`)
- `rowsPerSlice` (`int`) - for `mode = rows`, maximum rows in the one slice, default `1 000 000`
- `numberOfSlices` (`int`) - for `mode = slices`, fixed number of slices, default `60`
- `minBytesPerSlice` (`int`) - for `mode = slices`, minimum size of the one slice in bytes before compression, default `4 194 304` (`4 MiB`)
- `gzip` (`bool`) - enable gzip compression, default `true`
- `gzipLevel` (`int`) - compression level, min `1` - the best speed), max `9` - the best compression, default `2`

## Sample configurations

Default parameters (`500 MiB` per slice, gzip enabled):

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
    "bytesPerSlice": 104857600
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
    "minBytesPerSlice": 10485760
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
docker-compose run --rm dev ./test.sh
```

In IntelliJ IDEA is needed to set project GOPATH to `/go` directory.

# Integration

For information about deployment and integration with KBC, please refer to
the [deployment section of developers documentation](https://developers.keboola.com/extend/component/deployment/)
