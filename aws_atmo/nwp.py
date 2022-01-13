import logging
import boto3
import boto3.session
import os, time, re
from datetime import datetime, timedelta

from threading import Event
from queue import Queue

from .downloader import AWS_Scheduler, TIMEOUT
from .pathUtils.nwp import nwpPath

import os, shutil, glob, time


###############################################################################
class NWP_AWS_Scheduler( AWS_Scheduler ):
  """
  This class was developed to cleanly close downloading processes when
  an interupt signal is received. On interupt, an event is set that
  tells the download processes to stop. While this will not happen
  instantly, trust that the processes are finishing what they are
  working on and closing.
  """

  ############################################################################
  def download(self, model, domain, pattern,
        fcstlen     = 18,
        fcststep    =  1,
        initstep    =  1,
        date1       = None,
        date2       = None, 
        outroot     = '/',
        outPathFMT  = None,
        outFileFMT  = None,
        **kwargs):

    """
    Name:
        download
    Purpose:
        Method to download NEXRAD Level 2 data from AWS. Is a wrapper
        for the _enqueueFiles and _wait methods.
    Inputs:
        None.
    Keywords:
        date0      : datetime object with starting date/time for download.
                        DEFAULT: Current UTC day at 00Z
        date1      : datetime object with ending date/time for download.
                        DEFAULT: End of date0 day.
        station    : Scalar string or list of strings containing 
                        radar station IDs in the for KXXX
        resource   : AWS resource to download from. Default is s3
        bucketName : Name of the bucket to download data from.
                        DEFAULT: noaa-nexrad_level2
        outroot    : Top level output directory for downloaded files.
                        This function will create the following directory
                        structure in the directory:
                            <outroot>/YYYY/YYYYMM/YYYYMMDD/KXXX/
        no_MDM     : Set to True to exclude *_MDM files from
                        download. THIS IS THE DEFAULT BEHAVIOR
        no_tar     : Set to True to exclude *tar files from
                        download. THIS IS THE DEFAULT BEHAVIOR
        clobber    : Set to True to re download files that exist.
        maxAttempt : Maximum number of times to try to download
                        file. DEFAULT: 3
        concurrency: Number of concurrent downloads to allow
    Outputs:
        Returns output directory for data files, # successful downloads,
        # failed downloads, and total size of all downloaded files.
    """
    
    self.t0  = time.time()

    utcnow = datetime.utcnow()                                                      # Get current UTC time
    if (date1 is None):
        date1 = datetime(utcnow.year, utcnow.month, utcnow.day, 0)                  # Set default start date to current UTC day at 00Z

    if (date2 is None):
        date2 = date1
        #date2 = datetime(date1.year, date1.month, date1.day+1, 0)                   # Set default end date to tomorrow's UTC day at 00z

    fcstTimes = list( range(0, fcstlen+fcststep, fcststep ) )
    initDate = date1

    while date2 >= initDate:                                                        # While the end date is greater than date
      dataDir  = nwpPath( model, domain, initDate )
      if outPathFMT:
        localDir = [ fmt.format( model=model, initDate=initDate, **kwargs ) for fmt in outPathFMT ]
      else:
         localDir = dataDir

      outdir   = os.path.join( outroot, *localDir )
      prefix   = '/'.join( [*dataDir, f'{model}.t{initDate:%H}z.{pattern}'] )

      statKeys = self.bucket.objects.filter( Prefix = prefix )
      for statKey in statKeys:
        fBase = statKey.key.split('/')[-1]
        fHour = re.findall( 'f(\d+)', fBase )
        if len(fHour) == 1 and int(fHour[0]) in fcstTimes:
          fHour = int(fHour[0])
          if outFileFMT:
            localFile = outFileFMT.format( model=model, initDate=initDate, fHour=fHour, **kwargs )
            _, ext = os.path.splitext(fBase)
            if not localFile.endswith( ext ):
              localFile += ext
          else:
            localFile = fBase
          localFile = os.path.join( outdir, localFile )
          info = (prefix, statKey.key, statKey.size, localFile, )
          if not os.path.isdir( outdir ):
            os.makedirs( outdir )

          while not self.killEvent.is_set():                                                   # If the killEvent is set, then return from method; we don't want to put anything el
            try:
              self.fileQueue.put( info, True, TIMEOUT )
            except Exception as err:
              pass
            else:
              break

      if self.killEvent.is_set():                                                          # If the killEvent is set, then return from method; we don't want to put anything el
        date = date1
        break

      initDate += timedelta( hours = initstep )

    return self.wait()


def hrrr( outroot,
        domain      = 'conus',
        vert_coord  = 'pressure',
        subhourly   = False,
        fcstlen     = 18,
        fcststep    =  1,
        initstep    =  1,
        date1       = None,
        date2       = None,
        maxAttempt  = 3,
        resource    = 's3',
        bucketName  = 'noaa-hrrr-bdp-pds',
        clobber     = False,
        concurrency = 4):

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
        maxAttempt (int) : Maximum number of times to try to download
                        file. DEFAULT: 3
        bucketName (str) : Name of the AWS s3 bucket to download data
                        from. DEFAULT: 'noaa-nexrad-level2'
        concurrency (int ): Number of concurrent downloads to allow

    Author and History:
        Kyle R. Wodzicki     Created 2019-07-06

    """

    log = logging.getLogger(__name__)
    t0  = time.time()

    scheduler = NWP_AWS_Scheduler( resource, bucketName, clobber, maxAttempt, concurrency )

    if subhourly:
      pattern = 'wrfsubh'
    elif vert_coord == 'pressure':
      pattern = 'wrfprs'
    elif vert_coord == 'native':
      pattern = 'wrfnat'
    elif vert_coord == 'surface':
      pattern = 'wrfsfc'
    
    outdir, nSuccess, nFail, size = scheduler.download('hrrr', domain, pattern,
        fcstlen     = fcstlen,
        fcststep    = fcststep,
        initstep    = initstep,
        date1       = date1,
        date2       = date2,
        outroot     = outroot)

    scheduler.close()

GFS_DEFAULTS = {
  'outPathFMT' : ['{resolution:0.2f}', '{initDate:%Y}', '{initDate:%Y%m}', '{initDate:%Y%m%dT%H}'],
  'outFileFMT' : '{model}.t{initDate:%H}z.{type}.{res}.f{fHour:03d}',
  'domain'     : 'atmos',
  'fcstlen'    : 120,
  'fcststep'   :   6,
  'initstep'   :   6,
  'resolution' : 0.5
}

def gfs( outroot,
        outPathFMT  = GFS_DEFAULTS['outPathFMT'],
        outFileFMT  = GFS_DEFAULTS['outFileFMT'],
        domain      = GFS_DEFAULTS['domain'],
        fcstlen     = GFS_DEFAULTS['fcstlen'],
        fcststep    = GFS_DEFAULTS['fcststep'],
        initstep    = GFS_DEFAULTS['initstep'],
        resolution  = GFS_DEFAULTS['resolution'],
        date1       = None,
        date2       = None,
        maxAttempt  = 3,
        resource    = 's3',
        bucketName  = 'noaa-gfs-bdp-pds',
        clobber     = False,
        concurrency = 4):

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
        outfmt (iter) : Iterable with '{}' format codes to specify the path to the downloaded file
        subhourly (bool) : Set if you want to download the sub-houlry data files
        fcstlen (int)    : Length of the forecast download in units of hours; there are probably issues with sub-hourly data as has not been tested yet
        fcststep (int)   : Set for forecast hours to download. E.g., if set to 3 will download forecast hours 0, 3, 6, 9, etc.
        initstep (int)   : Set forecast initialization step. By default will download every hourly initialized forecast between date1 and date2.
          If set to 6, will download runs initalized at 00, 06, 12, and 18.
        date1 (datetime) : Starting date/time for data to download.
                            DEFAULT: Current UTC day at 00Z
        date2 (datetime) : Ending date/time for data to download. This date IS inclusive.
                            DEFAULT: End of current UTC day
        no_MDM (bool) : Set to True to exclude *_MDM files from
                        download. THIS IS THE DEFAULT BEHAVIOR
        no_tar (bool) : Set to True to exclude *tar files from
                        download. THIS IS THE DEFAULT BEHAVIOR
        maxAttempt (int) : Maximum number of times to try to download
                        file. DEFAULT: 3
        bucketName (str) : Name of the AWS s3 bucket to download data
                        from. DEFAULT: 'noaa-nexrad-level2'
        concurrency (int ): Number of concurrent downloads to allow

    Author and History:
        Kyle R. Wodzicki     Created 2019-07-06

    """

    log = logging.getLogger(__name__)
    t0  = time.time()

    scheduler = NWP_AWS_Scheduler( resource, bucketName, clobber, maxAttempt, concurrency )

    type    = 'pgrb2'
    res     = f'{resolution:0.2f}'.replace('.', 'p' )
    pattern = f'{type}.{res}'

    outdir, nSuccess, nFail, size = scheduler.download('gfs', domain, pattern,
        fcstlen     = fcstlen,
        fcststep    = fcststep,
        initstegp    = initstep,
        date1       = date1,
        date2       = date2,
        outroot     = outroot,
        outPathFMT  = outPathFMT,
        outFileFMT  = outFileFMT,
        resolution  = resolution,
        type        = type,
        res         = res)

    scheduler.close()
