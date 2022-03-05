defmodule Ktdreader.Query do
  import Ktdreader.Block
  import Ktdreader.Rows
  import Ktdreader.Index

  def find_by_primary_key(reader, line_length, find_key) do
    key_pos = reader.primary_key_position
    key_length = reader.primary_key_length

    key_stream(reader, key_pos, key_length)
    |> Stream.transform(false, fn {key, block_start, block_end}, acc ->
      if acc do
        {:halt, acc}
      else
        if check_primary_key(find_key, key) == 1 do
          {block_stream(reader, block_start, block_end, line_length, &parse_row/1), true}
        else
          {[], false}
        end
      end
    end)
    |> Enum.find(fn %{"_PK" => value} ->
      value == find_key
    end)
  end

  def find_by_secondary_key(reader, line_length, secondary_key_name, find_key) do
    # does not support compound secondary keys, if those even exist?
    {key_name, key_pos, key_length} =
      Enum.with_index(reader.secondary_key_items)
      |> Enum.find_value(fn {[[key_name, _, _, _]], index} ->
        if key_name == secondary_key_name do
          pos = Enum.at(reader.secondary_key_positions, index)
          len = Enum.at(reader.secondary_key_lengths, index)
          {key_name, pos, len}
        end
      end)

    key_stream(reader, key_pos, key_length)
    |> Stream.transform(false, fn {key, block_start, block_end}, acc ->
      if acc do
        {:halt, acc}
      else
        if check_primary_key(find_key, key) == 1 do
          {block_stream(
             reader,
             block_start,
             block_end,
             line_length,
             &parse_index(key_length, &1)
           ), true}
        else
          {[], false}
        end
      end
    end)
    |> Stream.transform(false, fn {key, block_start, block_end}, acc ->
      if acc do
        {:halt, acc}
      else
        if check_primary_key(find_key, key) == 1 do
          {block_stream(reader, block_start, block_end, line_length, &parse_row/1), true}
        else
          {[], false}
        end
      end
    end)
    |> Enum.find(fn value = %{} ->
      value[key_name] == find_key
    end)
  end

  defp check_primary_key(keyword, primary_key) do
    Enum.reduce_while(
      Enum.zip(String.to_charlist(keyword), String.to_charlist(primary_key)),
      0,
      fn {a, b}, acc ->
        cond do
          a > b ->
            {:halt, -1}

          a < b ->
            {:halt, 1}

          a == b ->
            {:cont, acc}
        end
      end
    )
  end
end
