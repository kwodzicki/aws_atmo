import logging
import boto3
import boto3.session
import os, time, re
from datetime import datetime, timedelta

from ..downloader import AWS_Scheduler, TIMEOUT
from ..downloader.utils import downloadBytes

from .pathUtils import nwpPath
from .utils import parseIDX

GFS_DEFAULTS = {
  'outPathFMT' : ['{resolution:0.2f}', '{initDate:%Y}', '{initDate:%Y%m}', '{initDate:%Y%m%dT%H}'],
  'outFileFMT' : '{model}.t{initDate:%H}z.{type}.{res}.f{fHour:03d}',
  'domain'     : 'atmos',
  'fcstlen'    : 120,
  'fcststep'   :   6,
  'initstep'   :   6,
  'resolution' : 0.5
}

HRRR_DEFAULTS = {
  'outPathFMT' : None,#['{resolution:0.2f}', '{initDate:%Y}', '{initDate:%Y%m}', '{initDate:%Y%m%dT%H}'],
  'outFileFMT' : None,#'{model}.t{initDate:%H}z.{type}.{res}.f{fHour:03d}',
  'domain'     : 'conus',
  'fcstlen'    : 18,
  'fcststep'   :  1,
  'initstep'   :  1
}


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
        subset      = None,
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
        retries : Maximum number of times to try to download
                        file. DEFAULT: 3
        concurrency: Number of concurrent downloads to allow
        **kwargs : All extra keywords are passed to the formatter strings
          for directory and file paths
    Outputs:
        Returns output directory for data files, # successful downloads,
        # failed downloads, and total size of all downloaded files.
    """

    super().download()

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
      if isinstance(outPathFMT, (list, tuple)):
        localDir = [ fmt.format( model=model, initDate=initDate, **kwargs ) for fmt in outPathFMT ]
      else:
        localDir = dataDir

      prefix   = '/'.join( [*dataDir, f'{model}.t{initDate:%H}z.{pattern}'] )
      outDir   = os.path.join( outroot, *localDir )
      if not os.path.isdir( outDir ): os.makedirs( outDir )                   # If output directory NOT exist, create it

      objs = self.bucket.objects.filter( Prefix = prefix )                      # Filter to objects that match prefix
      if subset:                                                                # If the subset keyword is set
        self.log.debug( 'Finding all idx files')
        objs = [obj for obj in objs if obj.key.endswith('.idx')]                # Filter objects to only those that end in .idx
          
      for obj in objs:                                                          # Iterate over all objects for downloading
        key     = obj.key                                                       # Get key for given object
        fBase   = key.split('/')[-1]                                            # Get file base name for given object
        fHour   = re.findall( 'f(\d+)', fBase )                                 # Get forecast hour from base name
        if len(fHour) != 1 or int(fHour[0]) not in fcstTimes: continue          # If no forecast hour found in base name OR the forecast hour is NOT in the requested forecast times; skip file

        offsets = None                                                          # Set offsets to None by default
        if subset:                                                              # If subset is set
          idx = downloadBytes( obj )                                            # Download the data for the given object; it's and IDX file
          if idx:                                                               # If the data are valid
            offsets, idx = parseIDX( idx, *subset )                             # Get offsets into the GRIB file and NEW idx data
            fBase,   _   = os.path.splitext( fBase )                            # Get the file basename with NO extension; i.e., strip off .idx
            key,     _   = os.path.splitext( key )                              # Get the key with NO file extension; i.e., strip off .idx
          else:                                                                 # Else, log error and skip to next object
            self.log.error( f'Failed to get IDX data : {key}' ) 
            continue

        fHour = int(fHour[0])                                                   # Get the forecast hour
        if isinstance(outFileFMT, str):                                         # If out file format is set
          localFile = outFileFMT.format( model=model, initDate=initDate, fHour=fHour, **kwargs )# Build base name
          if fBase.endswith( '.idx' ) and not localFile.endswith('.idx'):
            localFile += '.idx'
        else:                                                                   # Else
          localFile = fBase                                                     # Use fBase as the local file name

        localFile = os.path.join( outDir, localFile )
        if subset:                                                              # If subset set
          with open( f'{localFile}.idx', 'w' ) as fid:                          # Open idx file for writing
            fid.write( os.linesep.join( idx ) )                                 # Write subset idx data to file

        info = (prefix, key, localFile, offsets, )                              # Build info tuple of data to enqueue
        while not self.killEvent.is_set():                                      # While kill event is NOT set, try to enqueue information
          try:
            self.fileQueue.put( info, True, TIMEOUT )
          except Exception as err:
            pass
          else:
            break

      if self.killEvent.is_set(): initDate = date2                              # If killEvent is set, set initDate to last date to download
      initDate += timedelta( hours = initstep )                                 # Increment date; if killEvent was set, then this incrementing will push initDate past date2

    return self.wait()
