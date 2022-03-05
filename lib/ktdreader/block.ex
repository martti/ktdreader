defmodule Ktdreader.Block do
  def key_stream(reader, key_pos, key_length) do
    Stream.resource(
      fn ->
        {:ok, file} = File.open(reader.filename, [:read, :binary, :raw, read_ahead: 1_000_000])
        :file.position(file, key_pos)
        {:ok, pos} = :file.read(file, 4)
        <<items_count::size(32)-unsigned-integer-little>> = pos
        key_block_size = key_length + 8
        {file, items_count, key_block_size, 0}
      end,
      fn {file, items_count, key_block_size, count} ->
        if count < items_count do
          {:ok, key_block} = :file.read(file, key_block_size)

          <<
            key::size(key_length)-bytes,
            block_start::size(32)-unsigned-integer-little,
            block_end::size(32)-unsigned-integer-little
          >> = key_block

          {[{key, block_start, block_end}], {file, items_count, key_block_size, count + 1}}
        else
          {:halt, {file, items_count, key_block_size, 0}}
        end
      end,
      fn {file, _, _, _} ->
        File.close(file)
      end
    )
  end

  def block_stream(reader, block_start, block_end, line_length, row_parser) do
    Stream.resource(
      fn ->
        {:ok, file} = File.open(reader.filename, [:read, :binary, :raw, read_ahead: 1_000_000])

        block_size = block_end - block_start
        :file.position(file, block_start)
        {:ok, data_block} = :file.read(file, block_size)
        data_block = :erlbz2.decompress(data_block)

        {reader, file, line_length, byte_size(data_block), data_block, 0}
      end,
      &row_parser.(&1),
      fn {_, file, _, _, _, _} ->
        File.close(file)
      end
    )
  end
end
