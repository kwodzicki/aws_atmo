import logging
import os, re, yaml
from multiprocessing import cpu_count;
from .version import __version__

                                                                               
def _parseIDLStartup( startupFile, config ):
  """
  Name:
      _parseIDLStartup
  Purpose:
      Function to parse simple system variable definitions from an
      IDL startup.pro file
  Inputs:
      startupFile : Path to file to parse
  Keywords:
      None.
  Outputs:
      None. Sets/updates global variables
  """
  IDLcomment = re.compile(r'^\s*\;')                                          # Pattern for comment line   
  IDLDefSysV = re.compile(r'(?:\,\s*[\'\"](\!?[^\'\"]+)[\'\"])')              # Pattern for extracting system variable and associated string; we ignore all system variables that are NOT strings 
  IDLDefLclV = re.compile(r'^\s*([\s\w\d\_]+=.+)')                            # Pattern for variable definition
  IDLAddVar  = r'([\'\"]?\s*\+\s*{}\s*\+\s*[\'\"]?)'                          # Pattern for addition of variable

  lclvars     = {};                                                           # Dictionary to hold variables that are local to the startup.pro file                                                                
  for line in open(startupFile).readlines():                                  # Open file, read all lines, iterate over lines
    if not IDLcomment.search(line):                                         # If the line is NOT a commented out line
                                                                          
      lclvar = IDLDefLclV.findall( line );                                # Check if line is a variable definition; i.e., it has an = sign in it
      if len(lclvar) == 1:                                                # If it is a variable definition
        lclvar = lclvar[0].split();                                     # Split line on space; Note that comments MUST be a least one space away from actual information and equals sign MUST have spaces around int
        try:                                                            # Try to
          index  = lclvar.index('=')                                  # Locate the equals sign in the list
          tmp = [lclvar[index-1].strip(), lclvar[index+1].strip()]    # Create new list taking value before equals sign and after equals sign
          lclvars[tmp[0]] = tmp[1]                                    # Add variable to lclvars dictionary using first element of tmp as key and second element as val
        except:                                                         # On exception
          pass                                                        # Fail silently
                                                                        
      for key, val in lclvars.items():                                    # Iterate over all key/value pairs in lclvars dictionary
        if (key in line):                                               # If the key is in the line we need to replace it
          addVar = IDLAddVar.format(key)                              # See if the variable is added to anything in the string
          if re.findall(addVar, line):                                # If the variable is added to something in the string
            val  = re.sub(r'[\'\"]', '', val)                       # Replace any quotations in the variable with nothing
            line = re.sub(addVar, val, line)                        # Replace varaiable reference in line with value
          else:                                                       # Else,
            line = line.replace(key, val)                           # Just replace the variable reference with the value
      sysvar = IDLDefSysV.findall(line)                                   # Check if DEFSYSV
      if len(sysvar) == 2:                                                # IF two strings found in line
        if sysvar[0][0] == '!':                                         # If first string starts with !, then system var
           config[ sysvar[0][1:].upper() ] = sysvar[1]             # Define global variable using uppercase system variable name
  return config 


os.environ['PYART_QUIET'] = '1';                                                # Supress PyART banner on load

HOME        = os.path.expanduser('~');                                    # Get home directory of user running package

NCPU         = cpu_count() // 2                                            # Set package wide CPU count to half the total number of CPUs
if NCPU > 6:                                                                    # If NCPU is greater than 6, force to 6 
  NCPU = 6
elif NCPU < 1:                                                                  # Else, if less than 1, force to 1
  NCPU = 1

TauDSolar         = 86400                                                       # Seconds per day
                                                                               
logFormatter = logging.Formatter(
  '%(asctime)s [%(process)d] %(levelname).4s  - %(message)s'
);                                                                              # Set base formatter for logs

LOG = logging.getLogger(__name__)                                               # Get logger with same name as package
LOG.setLevel( logging.DEBUG )                                                   # Set root log level to debug

def consoleLogger():
  console = logging.StreamHandler()
  console.setFormatter( logFormatter )
  console.setLevel( logging.WARNING )
  LOG.addHandler( console )

  return console

# Define data directory for package
DATADIR = os.path.join( os.path.dirname(__file__), 'data' )

# Read in list of stations
NEXRAD_STATION_ID_FILE = os.path.join(DATADIR, 'nexrad_station_id_list.yml' )
with open(NEXRAD_STATION_ID_FILE, 'r') as fid:
  NEXRAD_STATION_ID_LIST = yaml.load( fid, Loader=yaml.SafeLoader )
NEXRAD_STATION_IDS = NEXRAD_STATION_ID_LIST['143']

# Read in data path configure
CONFIG   = os.path.join(HOME, '.{}rc'.format(__name__))
if not os.path.isfile( CONFIG ):
  CONFIG = os.path.join(DATADIR, 'configrc.yml')
with open(CONFIG, 'r') as fid:
  CONFIG = yaml.safe_load(fid)

if CONFIG['defaults']['noaa_wct_batch_config'] == '':
  CONFIG['defaults']['noaa_wct_batch_config'] = os.path.join( DATADIR, 'wctBatchConfig_GridRad.xml' )

if 'version' not in CONFIG or not isinstance(CONFIG['version'], dict):
  CONFIG['version'] = {'nexrad_level3_version'    : 'v' + '_'.join(__version__.split('.')),
                       'nexrad_level3_3d_version' : 'v' + '_'.join(__version__.split('.')) }
else:
  if CONFIG['version'].get('nexrad_level3_version', '') == '' :
    CONFIG['version']['nexrad_level3_version'] = 'v' + '_'.join(__version__.split('.'))
  if CONFIG['version'].get('nexrad_level3_3d_version', '') == '':
    CONFIG['version']['nexrad_level3_3d_version'] = 'v' + '_'.join(__version__.split('.'))

# Test vars
#root                   = '/Volumes/Free_Space'

#GFS_root               = os.path.join( root,     'GFS')
#GFS_ANALYSIS_GRIB      = os.path.join( GFS_root, 'analysis',      'grib',  '' )
#GFS_ANALYSIS_NCDF      = os.path.join( GFS_root, 'analysis',      'netcdf','' )
#GFS_ANALYSIS_TROP_NCDF = os.path.join( GFS_root, 'analysis_trop', 'netcdf', '')

#GFS_FORECAST_GRIB      = os.path.join( GFS_root, 'forecast',      'grib',   '' )
#GFS_FORECAST_NCDF      = os.path.join( GFS_root, 'forecast',      'netcdf', '' )
#GFS_FORECAST_TROP_NCDF = os.path.join( GFS_root, 'forecast_trop', 'netcdf', '')

#TRAJ3D_GFS_ROOT        = os.path.join(root, 'TRAJ3D', '')
#TRAJ3D_GFS_WINDS       = os.path.join(TRAJ3D_GFS_ROOT, 'winds',         '')
#TRAJ3D_GFS_GLOBAL      = os.path.join(TRAJ3D_GFS_ROOT, 'dcotss_global', '')
#TRAJ3D_GFS_OT          = os.path.join(TRAJ3D_GFS_ROOT, 'dcotss_ot',     '') 

#NEXRAD_ROOT            = os.path.join(root, 'NEXRAD')
#NEXRAD_LEVEL2_ROOT     = os.path.join(NEXRAD_ROOT, 'level2', '')
#NEXRAD_LEVEL2_TMP_ROOT = os.path.join(NEXRAD_ROOT, 'tmp',    '')
#NEXRAD_LEVEL3_ROOT     = os.path.join(NEXRAD_ROOT, '')
#NEXARD_LEVEL3_DIRECTORY= os.path.join(NEXRAD_LEVEL3_ROOT, 'level3', '')

#NEXRAD_GFS_ECHO_TOP_ROOT = os.path.join(root,  'NEXRAD_DCOTSS', '')

#NOAA_WCT_EXPORT          = os.path.join('/', 'Users', 'kwodzicki', 'wct-4.3.1', 'wct-export')

# If IDL startup file defined, then overwrite any variables using those values
if ('IDL_STARTUP' in os.environ):
    CONFIG = _parseIDLStartup( os.environ.get('IDL_STARTUP'), CONFIG )

def setDefaults( kwargs = {} ):
  for key, val in CONFIG['defaults'].items():
    if key not in kwargs:
      kwargs[key] = val
  return kwargs
