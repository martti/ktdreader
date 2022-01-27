defmodule Ktdreader do
  def open do
    code = "11600233210"
    reader = Ktdreader.Reader.from_file("SP.CH.03818.FCTLR")
    block = Ktdreader.Block.find(reader, code)
    Ktdreader.Rows.find(reader, block, code)
  end
end
