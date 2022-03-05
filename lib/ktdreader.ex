defmodule Ktdreader do
  def open do
    filename = "SP.RT.03818.FCTLR"
    reader = Ktdreader.Reader.from_file(filename)
    Ktdreader.Query.find_by_secondary_key(reader, 2, "VIN", String.pad_trailing("ZFA31200000745586", 25))
  end
end
