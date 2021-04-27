import wqpy.read
import aiohttp
import asyncio
import io

async def _basic_aquery(service_url, service_params):
  async with aiohttp.ClientSession() as session:
    async with session.get(service_url, params = service_params) as r:
      return(await r.text())

def multi_query(service_url, service_param_list, parse = True):
  query_args = [(service_url, params) for params in service_param_list]
  loop = asyncio.get_event_loop()
  outputs = loop.run_until_complete(
    asyncio.gather(*(_basic_aquery(*args) for args in query_args))
  )
  if parse:
    return([wqpy.read.read(io.StringIO(o)) for o in outputs])
  else:
    return([io.StringIO(o).read() for o in outputs])
