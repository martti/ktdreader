defmodule Ktdreader.Rows do
  # open block pointed by primary key index
  def find(reader = %Ktdreader.Reader{}, block = %Ktdreader.Block{}, keyword) do
    block_size = block.block_end - block.block_start
    :file.position(reader.data_file, block.block_start)
    {:ok, data_block} = :file.read(reader.data_file, block_size)
    data_block = :erlbz2.decompress(data_block)
    parse_rows(reader, keyword, data_block)
  end

  defp parse_rows(_, _, <<>>) do
    false
  end

  defp parse_rows(reader = %Ktdreader.Reader{}, keyword, data_block) do
    <<
      data_length::size(8)-unsigned-integer-little,
      rest::binary
    >> = data_block

    # 1 is line length
    read_length = data_length - 1

    <<
      column_data::size(read_length)-bytes,
      rest::binary
    >> = rest

    columns =
      Enum.reduce(reader.column_items, [column_data, 0, []], fn x, acc ->
        # name = Enum.at(x, 0)
        type = Enum.at(x, 1)
        start_pos = Enum.at(x, 2)
        length = Enum.at(x, 3)
        # column index
        column_index = Enum.at(acc, 1)

        [col_data, rest] =
          if type == 0 do
            <<
              reference_index::size(16)-unsigned-integer-little,
              rest::binary
            >> = Enum.at(acc, 0)

            # fetch data from reference_tables
            column_data =
              Enum.at(
                Enum.at(reader.reference_tables, Enum.at(reader.column_reference_index, column_index)),
                reference_index
              )

            [List.to_string(column_data), rest]
          else
            length = if length == 0, do: read_length + reader.reference_padding - start_pos, else: length

            <<
              column_data::size(length)-bytes,
              rest::binary
            >> = Enum.at(acc, 0)

            [column_data, rest]
          end

        [rest, column_index + 1, [col_data | Enum.at(acc, 2)]]
      end)
      |> Enum.at(2)
      |> Enum.reverse()

    row_data = Enum.join(columns)

    # construct primary key field
    primary_key =
      Enum.reduce(reader.primary_key_items, "", fn x, acc ->
        block_start = Enum.at(x, 2)
        block_end = Enum.at(x, 3)
        acc <> String.slice(row_data, block_start, block_end)
      end)

    if primary_key == keyword do
      columns
    else
      parse_rows(reader, keyword, rest)
    end
  end
end
