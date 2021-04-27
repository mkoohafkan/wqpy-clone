import wqpy

wqp = wqpy.connection("production", "marsh")
wqp.is_connected()

print(wqp.result_details())

# specific conductance at HSL
wqp.result_dates(36289)
wqp.result_data(36289, "2019-10-01 00:00:00", "2020-08-30 23:59:59")
