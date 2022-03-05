defmodule Ktdreader.Rows do
  def parse_row({reader, file, line_length, block_size, data_block, count}) do
    #  fn {file, block_size, data_block, count} ->
    # IO.puts("ROW PARSER #{count} < #{block_size}")

    if count < block_size do
      data_size = 8 * line_length

      <<
        data_length::size(data_size)-unsigned-integer-little,
        rest::binary
      >> = data_block

      read_size = data_length - line_length

      <<
        column_data::size(read_size)-bytes,
        rest::binary
      >> = rest

      # IO.puts(Hexdump.to_string(column_data))

      {_, _, columns} =
        Enum.reduce(reader.column_items, {column_data, 0, []}, fn [_name, type, start_pos, length],
                                                                  {rest, column_index, columns} ->
          # IO.puts("len: #{length}")

          {cd, rest} =
            case type do
              0 ->
                <<
                  reference_index::size(16)-unsigned-integer-little,
                  rest::binary
                >> = rest

                # fetch data from reference_tables
                reference_value =
                  Enum.at(
                    Enum.at(
                      reader.reference_tables,
                      Enum.at(reader.column_reference_index, column_index)
                    ),
                    reference_index
                  )

                {List.to_string(reference_value), rest}

              1 ->
                # last column length is 0, so read to end
                length =
                  if length == 0 do
                    read_size + reader.reference_padding - start_pos
                  else
                    length
                  end

                <<
                  column_data::size(length)-bytes,
                  rest::binary
                >> = rest

                {column_data, rest}
            end

          {rest, column_index + 1, [cd | columns]}
        end)

      columns = Enum.reverse(columns)

      # construct primary key
      primary_key =
        Enum.reduce(reader.primary_key_items, "", fn [_name, _, start, length], acc ->
          acc <> String.slice(Enum.join(columns), start, length)
        end)

      # map columns
      columns =
        Enum.zip(reader.column_items, columns)
        |> Enum.reduce(%{}, fn {[name, _, _, _], value}, acc ->
          Map.put(acc, name, value)
        end)

      columns = Map.put(columns, "_PK", primary_key)

      {[columns], {reader, file, line_length, block_size, rest, count + data_length}}
    else
      {:halt, {reader, file, line_length, block_size, data_block, 0}}
    end
  end
end
