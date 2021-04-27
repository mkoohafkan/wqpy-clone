import pandas

# todo: force time zone
def read(data):
  return(pandas.read_csv(data, sep = "|"))
