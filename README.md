# Ktdreader

Library for reading FIAT ePER KTD VIN files

## Usage

```elixir
reader = Ktdreader.Reader.from_file("filename")

# find by primary key, 1 is line length
reader = Ktdreader.Reader.from_file(filename)
Ktdreader.Query.find_by_primary_key(reader, 1, "11600233210")

# find by secondary key VIN, 2 is line length
# vin is padded
vin = String.pad_trailing("ZFA31200000745586", 25)
Ktdreader.Query.find_by_secondary_key(reader, 2, "VIN", vin)
```

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `ktdreader` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:ktdreader, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/ktdreader>.

