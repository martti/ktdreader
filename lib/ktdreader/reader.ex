defmodule Ktdreader.Reader do
  defstruct filename: "",
            version: "",
            records: 0,
            block_size: 0,
            reference_positions: [],
            primary_key_position: 0,
            primary_key_length: 0,
            secondary_key_positions: [],
            table_name: "",
            primary_key_item_count: 0,
            primary_key_items: [],
            secondary_key_items: [],
            secondary_key_lengths: [],
            column_items: [],
            column_reference_index: [],
            column_packed_positions: [],
            reference_padding: 0,
            reference_tables: []

  def from_file(filename) do
    {:ok, data_file} = :file.open(filename, [:read, :binary, :raw])

    {:ok, header} = :file.read(data_file, 768)

    <<
      version::size(2)-bytes,
      records::size(32)-unsigned-integer-little,
      block_size::size(16)-unsigned-integer-little,
      # 5 unsigned ints
      reference_pos::size(20)-bytes,
      primary_key_pos::size(32)-unsigned-integer-little,
      # 3 unsigned ints
      secondary_pos::size(12)-bytes,
      table_name::size(20)-bytes,
      primary_key_item_count::size(8)-unsigned-integer-little,
      rest::binary
    >> = header

    reference_pos = for <<pos::size(32)-unsigned-integer-little <- reference_pos>>, do: pos
    secondary_pos = for <<pos::size(32)-unsigned-integer-little <- secondary_pos>>, do: pos
    table_name = String.trim(table_name, "\0")
    primary_key_items_bytes = 23 * primary_key_item_count

    <<
      primary_key_items::size(primary_key_items_bytes)-bytes,
      secondary_key_item_count::size(8)-unsigned-integer-little,
      rest::binary
    >> = rest

    primary_key_items = key_items(primary_key_items)
    secondary_key_items = secondary_key_items(secondary_key_item_count, rest)

    secondary_bytes =
      Enum.reduce(secondary_key_items, 0, fn x, acc ->
        Enum.count(x) * 23 + 1 + acc
      end)

    <<
      _::size(secondary_bytes)-bytes,
      column_items_count::size(8)-unsigned-integer-little,
      rest::binary
    >> = rest

    column_items_bytes = 23 * column_items_count

    <<
      column_items::size(column_items_bytes)-bytes,
      _rest::binary
    >> = rest

    column_items = key_items(column_items)
    # not used?
    # reference_table_count = Enum.reduce(reference_pos, 0, &if(&1 > 0, do: &2 + 1, else: &2))
    [column_reference_index, column_packed_positions] = column_indexes(column_items)

    reader = %Ktdreader.Reader{
      filename: filename,
      version: version,
      records: records,
      block_size: block_size,
      reference_positions: reference_pos,
      primary_key_position: primary_key_pos,
      secondary_key_positions: secondary_pos,
      table_name: table_name,
      primary_key_item_count: 0,
      primary_key_items: primary_key_items,
      secondary_key_items: secondary_key_items,
      column_items: column_items,
      primary_key_length: Enum.reduce(primary_key_items, 0, &(Enum.at(&1, 3) + &2)),
      secondary_key_lengths:
        Enum.map(secondary_key_items, fn x ->
          Enum.reduce(x, 0, &(Enum.at(&1, 3) + &2))
        end),
      column_reference_index: column_reference_index,
      column_packed_positions: column_packed_positions,
      reference_padding: reference_padding(column_items),
      reference_tables: reference_tables(reference_pos, data_file)
    }

    :file.close(data_file)
    reader
  end

  defp reference_tables(reference_pos, data_file) do
    Enum.map(Enum.filter(reference_pos, &(&1 > 0)), fn pos ->
      :file.position(data_file, pos)
      {:ok, ref_tables} = :file.read(data_file, 256)

      <<
        ref_items::size(32)-unsigned-integer-little,
        item_width::size(32)-unsigned-integer-little,
        _rest::binary
      >> = ref_tables

      :file.position(data_file, pos + 2 * 4)
      {:ok, ref_tables} = :file.read(data_file, ref_items * item_width)

      parse_ref_tables(ref_items, item_width, ref_tables)
    end)
  end

  defp parse_ref_tables(0, _width, _rest) do
    []
  end

  defp parse_ref_tables(count, width, rest) when count > 0 do
    <<
      items::size(width)-bytes,
      rest::binary
    >> = rest

    items = for <<value::size(8)-integer <- items>>, do: value

    [items | parse_ref_tables(count - 1, width, rest)]
  end

  defp column_indexes(column_items) do
    Enum.reduce(column_items, [0, 0, [], []], fn x, acc ->
      pos = Enum.at(acc, 0)
      cur_pos = [pos | Enum.at(acc, 3)]
      ref_index = Enum.at(acc, 1)
      type = Enum.at(x, 1)
      length = Enum.at(x, 3)
      pos = if type == 0, do: pos + 2, else: pos + length
      ref = if type == 0, do: ref_index, else: -1
      ref_index = if type == 0, do: ref_index + 1, else: ref_index
      [pos, ref_index, [ref | Enum.at(acc, 2)], cur_pos]
    end)
    |> Enum.slice(2, 2)
    |> Enum.map(&Enum.reverse(&1))
  end

  defp reference_padding(column_items) do
    Enum.reduce(column_items, 0, fn [_, type, _, length], acc ->
      padding = if type == 0, do: length - 2, else: 0
      acc + padding
    end)
  end

  defp key_items(items) do
    for <<
          field_name::size(20)-bytes,
          data_type::size(8)-unsigned-integer-little,
          start_pos::size(8)-unsigned-integer-little,
          length::size(8)-unsigned-integer-little <- items
        >>,
        do: [String.trim(field_name, "\0"), data_type, start_pos, length]
  end

  defp secondary_key_items(0, _rest) do
    []
  end

  defp secondary_key_items(count, rest) when count > 0 do
    <<secondary_count::size(8)-unsigned-integer-little, rest::binary>> = rest
    secondary_bytes = 23 * secondary_count
    <<secondary_key_items::size(secondary_bytes)-bytes, rest::binary>> = rest
    [key_items(secondary_key_items) | secondary_key_items(count - 1, rest)]
  end
end
