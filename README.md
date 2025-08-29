# Simple tool to export and import kafka messages

# Usage

```bash
# export kafka messages
go run main.go export --config <export.yaml>

# check exported messages
go run main.go check --data=<path_to_data_dir>

# import kafka messages
go run main.go import --config <import.yaml>
```