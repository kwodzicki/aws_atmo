import logging

from . import NWP_AWS_Scheduler, TIMEOUT
from .pathUtils import nwpPath
from . import HRRR_DEFAULTS


def hrrr( outroot,
        subset      = None,
        outPathFMT  = HRRR_DEFAULTS['outPathFMT'],
        outFileFMT  = HRRR_DEFAULTS['outFileFMT'],
        domain      = HRRR_DEFAULTS['domain'],
        fcstlen     = HRRR_DEFAULTS['fcstlen'],
        fcststep    = HRRR_DEFAULTS['fcststep'],
        initstep    = HRRR_DEFAULTS['initstep'],
        vert_coord  = 'pressure',
        subhourly   = False,
        date1       = None,
        date2       = None,
        retries     = 3,
        resource    = 's3',
        bucketName  = 'noaa-hrrr-bdp-pds',
        clobber     = False,
        jobs        = 4):

    """
    Function for downloading NEXRAD Level 2 data from AWS.

    Arguments:
        stations   : Scalar string or list of strings containing 
                        radar station IDs in the for KXXX
        outroot    : Top level output directory for downloaded files.
                        This function will create the following directory
                        structure in the directory:
                            <outroot>/YYYY/YYYYMM/YYYYMMDD/KXXX/
 
    Keyword arguments:
        subhourly (bool) : Set if you want to download the sub-houlry data files
        fcstlen (int)    : Length of the forecast download in units of hours; there are probably issues with sub-hourly data as has not been tested yet
        fcststep (int)   : Set for forecast hours to download. E.g., if set to 3 will download forecast hours 0, 3, 6, 9, etc.
        initstep (int)   : Set forecast initialization step. By default will download every hourly initialized forecast between date1 and date2.
          If set to 6, will download runs initalized at 00, 06, 12, and 18.
        date1 (datetime) : Starting date/time for data to download.
                            DEFAULT: Current UTC day at 00Z
        date2 (datetime) : Ending date/time for data to download. This date is NOT inclusive.
                            DEFAULT: End of current UTC day
        no_MDM (bool) : Set to True to exclude *_MDM files from
                        download. THIS IS THE DEFAULT BEHAVIOR
        no_tar (bool) : Set to True to exclude *tar files from
                        download. THIS IS THE DEFAULT BEHAVIOR
        retries (int) : Maximum number of times to try to download
                        file. DEFAULT: 3
        bucketName (str) : Name of the AWS s3 bucket to download data
                        from. DEFAULT: 'noaa-nexrad-level2'
        jobs (int) : Number of concurrent downloads to allow

    Author and History:
        Kyle R. Wodzicki     Created 2019-07-06

    """

    log = logging.getLogger(__name__)

    scheduler = NWP_AWS_Scheduler( resource, bucketName, clobber, retries, jobs )

    if subhourly:
      pattern = 'wrfsubh'
    elif vert_coord == 'pressure':
      pattern = 'wrfprs'
    elif vert_coord == 'native':
      pattern = 'wrfnat'
    elif vert_coord == 'surface':
      pattern = 'wrfsfc'
    
    outdir, nSuccess, nFail, size = scheduler.download('hrrr', domain, pattern,
        outPathFMT  = outPathFMT,
        outFileFMT  = outFileFMT,
        subset      = subset,
        fcstlen     = fcstlen,
        fcststep    = fcststep,
        initstep    = initstep,
        date1       = date1,
        date2       = date2,
        outroot     = outroot)

    scheduler.close()
