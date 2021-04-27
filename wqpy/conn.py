import warnings
import datetime
import wqpy.query as query
import wqpy.aquery as aquery

class connection:
  """
  Water Quality Portal connection object. Provides behavior 
  for specifying login information, connecting to the database,
  and querying data.

  Attributes:
    database    The WQP database to connect to, e.g., "production" or "test".
    program     The WQP program to connect to, e.g., "marsh" or "emp".
  """

  database = None
  program = None
  _base_url = None
  _program_code = None

  _available_databases = {
    "test" : None,
    "production" : None
  }

  _available_programs = {
    "marsh" : None,
    "emp" : None
  }

  # meta services
  _meta_contacts_service = "TelemetryDirect/api/Meta/Contacts"
  _meta_stations_service = "TelemetryDirect/api/Meta/Stations"
  _meta_locations_service = "TelemetryDirect/api/Meta/Locations"

  # asset services
  _asset_sondes_service = "TelemetryDirect/api/Assets/Sondes"
  _meta_action_types_service = "TelemetryDirect/api/Assets/ActionTypes"
  _asset_verificationinstrument_types_service = "TelemetryDirect/api/Assets/VerificationInstrumentTypes"
  _asset_standardsolution_types_service = "TelemetryDirect/api/Assets/StandardSolutionTypes"

  # result services
  _result_data_service = "TelemetryDirect/api/Results/ResultData"
  _result_detail_service = "TelemetryDirect/api/Results/ResultDetails"
  _result_dates_service = "TelemetryDirect/api/Results/ReadingDates"

  # event services
  _event_types_service = "TelemetryDirect/api/Events/Types"
  _event_reasons_service = "TelemetryDirect/api/Events/Reasons"
  _event_summaries_service = "TelemetryDirect/api/Events/Summaries"
  _event_detail_service = "TelemetryDirect/api/Events/EventDetails"
  _event_action_detail_service = "TelemetryDirect/api/Events/ActionDetails"
  _event_verificationinstrument_detail_service = "TelemetryDirect/api/Events/VerificationInstrumentDetails"
  _event_standardsolution_detail_service = "TelemetryDirect/api/Events/StandardSolutionDetails"

  # report services
  _report_pdm_service = "Reports/api/pdm"

  #### connection methods ####

  def _connect(self, database, program, check):
    """
    Connect to the Water Quality Portal.
    """
    database = database.lower()
    program = program.lower()
    if(database in self._available_databases.keys()):
      self.database = database
      self._base_url = self._available_databases[database]
    else:
      raise Exception("Database not found.")
    if(program in self._available_programs.keys()):
      self.program = program
      self._program_code = self._available_programs[program]
    else:
      raise Exception("Program not found.")
    if check:
      connected = self.is_connected()
      if not connected:  
        raise Exception("Could not connect to WQP.")
      else:
        print("Established connection to WQP.")
    return(self)


  def is_connected(self):
    """
    Check if the Water Quality Portal can be accessed.
    """
    try:
      r = query.basic_query(self._base_url, parse = False)
    except:
      return(False)
    return(True)

  #### results methods ####

  def result_details(self): 
    """
    List all WQP result sets.

    result_ids is a single value or list result IDs.
    """
    service_url = f"{self._base_url}/{self._result_detail_service}"
    service_params = {"program" : self._program_code}
    return(query.basic_query(service_url, service_params, True))


  def result_dates(self, result_ids):
    """
    List all WQP result sets.
    """
    if type(result_ids) is not list:
      result_ids = [result_ids]
    service_url = f"{self._base_url}/{self._result_dates_service}"
    service_param_list = [{"program" : self._program_code, "resultid" : rid} for rid in result_ids]
    return(aquery.multi_query(service_url, service_param_list, True))


  def result_data(self, result_ids, start_times, end_times, version = 1):
    """
    Get data for the specified result id.

    result_ids is a single value or list result IDs.
    start_times is a single value or list of datetime strings, e.g., "2019-01-01 00:00:00".
    end_times is a single value or list of datetime strings, e.g., "2019-01-01 00:00:00".
    version is a single value or list of integer versions (value of 1 in almost all cases).
    """
    if type(result_ids) is not list:
      result_ids = [result_ids]
    if type(version) is not list:
      version = [int(version)] * len(result_ids)
    if type(start_times) is not list:
      start_times = [start_times] * len(result_ids)
    if type(end_times) is not list:
      end_times = [end_times] * len(result_ids)
    if len(start_times) != len(end_times):
      raise ValueError("'start_times' and 'end_times' must be the same length.")
    if len(start_times) != len(result_ids):
      raise ValueError("'start_times' and 'end_times' must be the same length as 'result_ids'.")
    start_times = [datetime.datetime.strptime(st, "%Y-%m-%d %H:%M:%S") for st in start_times]
    end_times = [datetime.datetime.strptime(et, "%Y-%m-%d %H:%M:%S") for et in end_times]
    start_format = [st.strftime("%Y-%m-%d:%H:%M:%S") for st in start_times]
    end_format = [et.strftime("%Y-%m-%d:%H:%M:%S") for et in end_times]
    service_url = f"{self._base_url}/{self._result_data_service}"
    rp = zip(result_ids, start_format, end_format, version)
    service_param_list = [{"program" : self._program_code, "resultid" : r, "start" : s, "end" : e, "version" : v}
      for r, s, e, v in rp]
    results = aquery.multi_query(service_url, service_param_list, True)
    empty_df = [df.empty for df in results]
    empty_rid = [r for (r, e) in zip(result_ids, empty_df) if e]
    if len(empty_rid) > 0:
      warnings.warn("Some result ids returned no data for the specified time window: " +
        ", ".join([str(rid) for rid in empty_rid]))
    return([df for (df, e) in zip(results, empty_df) if not e])

  #### event methods ####


  #### meta methods ####


  #### asset methods ####

  def __init__(self, database = None, program = None, check = False):
    """
    Initialize the WQP connection object.
    """
    if database is None:
      database = input("Enter your database ('production' or 'test'): ")
    if program is None:
      program = input("Enter your database ('MARSH' or 'EMP'): ")
    self._connect(database, program, check)
    None

  def __del__(self):
    """
    Destructor for the WQP connection object.
    """
    pass
