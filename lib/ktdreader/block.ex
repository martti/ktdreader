defmodule Ktdreader.Block do
  defstruct primary_key: "", block_start: 0, block_end: 0

    # search variable search_vin from primary keys
    # return bzip2 blocks block_start block_end
    def find(reader = %Ktdreader.Reader{}, keyword) do
    :file.position(reader.data_file, reader.primary_key_position)
    {:ok, primary_pos} = :file.read(reader.data_file, 4)

    <<
      primary_items_count::size(32)-unsigned-integer-little
    >> = primary_pos

    primary_key_length = reader.primary_key_length
    primary_key_pos_c = reader.primary_key_position + 4
    :file.position(reader.data_file, primary_key_pos_c)

    primary_key_block_size = primary_key_length + 8
    primary_keys_block_size = primary_items_count * primary_key_block_size
    {:ok, primary_keys_block} = :file.read(reader.data_file, primary_keys_block_size)

    Enum.find_value(
      for(<<chunk::size(primary_key_block_size)-bytes <- primary_keys_block>>, do: chunk),
      fn x ->
        <<
          primary_key::size(primary_key_length)-bytes,
          block_start::size(32)-unsigned-integer-little,
          block_end::size(32)-unsigned-integer-little
        >> = x

        if check_primary_key(keyword, primary_key) == 1 do
          %Ktdreader.Block{ primary_key: primary_key, block_start: block_start, block_end: block_end }
        else
          false
        end
      end)
  end

  defp check_primary_key(keyword, primary_key) do
    Enum.reduce_while(
      Enum.zip(String.to_charlist(keyword), String.to_charlist(primary_key)), 0,
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
