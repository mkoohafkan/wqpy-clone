==================
Quickstart to wqpy
==================



First, import the ``wqpy`` module::

    import wqpy

Connect to WQP by creating a new ``WQPcon`` object. You will be 
prompted for credentials::
    
    wqp = wqpy.WQPcon()

Connect to WQP using the  ``connect()`` method::

    wqp.connect()

List the available result datasets using the ``list_all()`` method::

    wqp.list_all()

Identify all time series records for salinity data at the Hunter Cut station::

    station_ids = (
        wqp.list_stations()
        .query('ABBREVIATION.str.contains("Hunter")')
        .STATION_ID
        .tolist()
    )
    constituent_ids = (
        wqp.list_constituents()
        .query('ANALYTE.str.contains("SC")')
        .CONSTITUENT_ID
        .tolist()
    )
    reading_ids = (
        wqp.list_readings()
        .query('READING_TYPE_NAME.str.contains("Time Series")')
        .READING_TYPE_ID
        .tolist()
    )
    result_ids = (
        wqp.list_all()
        .query("STATION_ID == @station_ids")
        .query("CONSTITUENT_ID == @constituent_ids")
        .query("READING_TYPE_ID == @reading_ids")
        .RESULT_ID
        .tolist()  
    )

Query the data for each result id and combine into one dataframe::

    import pandas
    results = pandas.concat([wqp.get_results(id) for id in result_ids])

