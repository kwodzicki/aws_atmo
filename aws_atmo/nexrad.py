import logging
import signal
import os, shutil, glob, time
from datetime import datetime, timedelta

import boto3
import boto3.session

from . import NCPU
from .pathUtils.nexrad import nexrad_level2_directory

from .downloader import AWS_Scheduler, TIMEOUT

_dateFMT   = "%Y%m%d_%H%M%S"                                                   # Time format in NEXRAD files

###############################################################################
class NEXRAD_AWS_Scheduler( AWS_Scheduler ):
  """
  This class was developed to cleanly close downloading processes when
  an interupt signal is received. On interupt, an event is set that
  tells the download processes to stop. While this will not happen
  instantly, trust that the processes are finishing what they are
  working on and closing.
  """

  ############################################################################
  def download(self,
          date0       = datetime(2011, 2, 28),
          date1       = None,
          station     = 'KHGX',
          outroot     = '/traid1/NEXRAD/level2/',
          no_MDM      = True,
          no_tar      = True,
          verbose     = False):
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

    if not isinstance( station, (list,tuple,) ): station = [station]                    # If stations is not an iterable, assume is string and make iterable

    stationdir, self.outdir, _ = nexrad_level2_directory(date0, station, root=outroot)
    
    self.log.info( 'NEXRAD_LEVEL2_AWS_DOWNLOAD - sync NEXRAD Level 2 data from AWS' )
    self.log.info( '   Sync date        : {}'.format(date0.strftime('%Y-%m-%d' ) ) )
    self.log.info( '   Output directory : {}'.format(self.outdir) )
    
    if os.path.isdir(self.outdir) and self.clobber:
      self.log.info( '   Deleting existing output directory and its contents' )
      shutil.rmtree( self.outdir )
    if not os.path.isdir( self.outdir ): os.makedirs( self.outdir )

    date  = datetime(date0.year, date0.month, date0.day, 0)                             # Create date for current date with hour at 0
    if (date1 is None):                                                                 # If date1 is None
      date1 = date + timedelta(days=1)                                                  # Set date1 to one day after date

    queueIndex = 0                                                                     # Index for which queue to put files in
    while (date1 > date) and (not self.kilEvent.is_set()):                             # While the end date is greater than date
      stationdir, self.outdir, _ = nexrad_level2_directory(date, station, root=outroot)

      datePrefix = date.strftime('%Y/%m/%d/')                                           # Set date prefix for key filtering of bucket

      for i in range( len(stationdir) ):                                                # Iterate over all stations in the station list
        if not os.path.isdir( stationdir[i] ): os.makedirs( stationdir[i] )             # If the output diretory does NOT exist, create it
        info       = []
        statPrefix = datePrefix + station[i]                                            # Create station prefix for bucket filter using datePrefix and the station ID
        statKeys   = self.bucket.objects.filter( Prefix = statPrefix )                  # Apply filter to bucket objects
        for statKey in statKeys:                                                        # Iterate over all the objects in the filter
          fBase = statKey.key.split('/')[-1]                                            # Get the base name of the file
          if (no_MDM and fBase.endswith('MDM')): continue                               # If the no_MDM keyword is set and the file ends in MDM, then skip it
          if (no_tar and fBase.endswith('tar')): continue                               # If the no_tar keyword is set and the file ends in tar, then skip it
          fDate = datetime.strptime(fBase[4:19], _dateFMT)                              # Create datetime object for file using information in file name
          if (fDate >= date0) and (fDate <= date1):                                     # If the date/time of the file is within the date0 -- date1 range
            self.log.debug( f'File : {statKey.key}; date : {fDate }' )
            localFile = os.path.join(stationdir[i], fBase)                              # Create local file path
            #files.append( (statKey.key, statKey.size, localFile,) )
            info = (station[i], statKey.key, statKey.size, localFile,)
 
            while not self.killEvent.is_set():                                          # If the killEvent is set, then return from method; we don't want to put anything else into the queue
              try:
                self.fileQueue.put( info, True, TIMEOUT )
              except Exception as err:
                self.log.error( err )
                pass
              else:
                break

        if self.killEvent.is_set():                                                     # If the killEvent is set, then return from method; we don't want to put anything else into the queue
          date = date1
          break
      date += timedelta(days = 1)                                                # Increment date by one (1) day

    return self._wait()

###############################################################################
def level2(
        date0       = datetime(2011, 2, 28),
        date1       = None,
        station     = 'KHGX',
        resource    = 's3',
        bucketName  = 'noaa-nexrad-level2',
        outroot     = '/traid1/NEXRAD/level2/',
        no_MDM      = True,
        no_tar      = True,
        clobber     = False,
        maxAttempt  = 3,
        verbose     = False,
        concurrency = NCPU):
  """
  Name:
      nexrad_aws_level2_download
  Purpose:
      Function for downloading NEXRAD Level 2 data from AWS.
  Inputs:
      None.
  Keywords:
      date0      : datetime object with starting date/time for
                      data to download.
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
  Author and History:
      Kyle R. Wodzicki     Created 2019-07-06
  """
  log = logging.getLogger( __name__ )

  scheduler = NEXRAD_AWS_Scheduler( resource, bucketName, clobber, maxAttempt, concurrency ) 

  outdir, nSuccess, nFail, size = scheduler.download( 
      date0       = date0,
      date1       = date1,
      station     = station,
      outroot     = outroot,
      no_MDM      = no_MDM,
      no_tar      = no_tar,
      verbose     = verbose)

  filelist = glob.iglob( os.path.join(outdir,'**'), recursive=True )
  filelist = [f for f in filelist if os.path.isfile(f)]
  nfiles   = len(filelist) 

  return outdir, filelist, size 
