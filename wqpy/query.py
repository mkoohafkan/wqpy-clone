import wqpy.read
import requests
import io

def basic_query(service_url, service_params = None, parse = True):
  """
  Basic WQP query interface.

  service_url specifies the complete URL of the query.
  If parse is True, the query response text will be parsed 
  as if it were a delimited file. Otherwise, the complete text
  of the response will be returned.
  """
  try:
    r = requests.get(service_url, service_params, verify = True)
    r.raise_for_status()
  except requests.exceptions.HTTPError as err:
    raise err
  if parse:
    return(wqpy.read.read(io.StringIO(r.text)))
  else:
    return(io.StringIO(r.text).read())

