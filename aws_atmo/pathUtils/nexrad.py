import logging
import os
from datetime import datetime

from .. import NEXRAD_STATION_IDS, CONFIG

################################################################################
def nexrad_level2_directory(date, station = NEXRAD_STATION_IDS, root = None):
  """
  Name:
      nexrad_level2_directory
  Purpose:
      Return the full paths to the local directories containing NEXRAD
      Level 2 data for the specified date and station IDs
  Inputs:
      date    : Datetime for which the directory paths are to be created
  Keywords:
      station : Scalar string or string list containing the station IDs
                  for which directories are to be created.
                  DEFAULT: directories are created for all of the
                  stations in the nexrad.NEXRAD_STATION_IDS
      root    : Top-level root directory. Default value depends on the
                  date of the data selected
  Returns:
      Returns three values:
          - Scalar string or list of strings containing full path(s) to 
              directory(s) of Level 2 files for the date and station(s)
              specified
          - The parent directory of the directories returned in first
              return value. This is the full path to those directories
              without the station ID component
          - The full root directory of Level 2 files for the 
              specified date
  """
  log = logging.getLogger(__name__);

  if (root is None):
    for dir, dates in CONFIG['level2_archive'].items():
      date0 = datetime.strptime( str(dates[0]), '%Y%m%d' )
      date1 = datetime.strptime( str(dates[1]), '%Y%m%d' )
      if (date >= date0) and (date <= date1):
        root = os.path.join( dir, 'NEXRAD', 'level2' )
        break
  if (root is None):
    raise Exception('The given date is NOT in the TAMU archive!')

  yyyy     = date.strftime('%Y')
  yyyymm   = date.strftime('%Y%m') 
  yyyymmdd = date.strftime('%Y%m%d') 

  parent   = os.path.join(root, yyyy, yyyymm, yyyymmdd, '')

  if not isinstance(station, (list, tuple,)): station = [station]                       # If station variable is NOT a list or tuple, make it a tuple

  return [os.path.join( parent, s, '' ) for s in station], parent, root 

################################################################################
def nexrad_level3_filepath(date, 
        root    = None,
        version = None,
        product = None,
        suffix  = '.nc'):
    
    if (root    is None): root    = CONFIG['defaults']['nexrad_level3_directory']
    if (version is None): version = CONFIG['version']['nexrad_level3_version']
    if (product is None): product = '3d'
    yyyy           = date.strftime('%Y');                        #YYYY string
    yyyymm         = date.strftime('%Y%m')                        #YYYYMM string
    yyyymmddhhmmss = date.strftime('%Y%m%dT%H%M%SZ');                                    #YYYYMMDDhhmmss string 
    
    directory      = os.path.join(root, version, 'level3', product, yyyy, yyyymm, '')
    filename       = 'nexrad_{}_{}_{}{}'.format(product, version, yyyymmddhhmmss, suffix)
    return os.path.join( directory, filename), directory

################################################################################
def nexrad_level2_tmp_dirs( **kwargs ):
  statid  = kwargs.get('station')
  root    = kwargs.get('root',    None)
  subdirs = kwargs.get('subdirs', 'netcdf')
  if root is None:
    root = CONFIG['defaults']['nexrad_level2_tmp_root']
  if not isinstance(statid,  (list,tuple,)):
    statid = [statid]
  if not isinstance(subdirs, (list,tuple,)):
    subdirs = [subdirs]
  return [os.path.join(root, *subdirs, sid) for sid in statid] 
