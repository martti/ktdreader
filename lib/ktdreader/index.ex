defmodule Ktdreader.Index do
  def parse_index(key_length, {reader, file, line_length, block_size, data_block, count}) do
    if count < block_size do
      read_size = key_length + 2

      <<
        key::size(key_length)-bytes,
        index::size(16)-unsigned-integer-little,
        rest::binary
      >> = data_block

      primary_key_length = reader.primary_key_length
      position = reader.primary_key_position + 4 + (primary_key_length + 8) * index
      :file.position(file, position)
      {:ok, block} = :file.read(file, primary_key_length + 8)

      <<
        _key::size(primary_key_length)-bytes,
        block_start::size(32)-unsigned-integer-little,
        block_end::size(32)-unsigned-integer-little
      >> = block

      {[{key, block_start, block_end}],
       {reader, file, line_length, block_size, rest, count + read_size}}
    else
      {:halt, {reader, file, line_length, block_size, data_block, 0}}
    end
  end
end
