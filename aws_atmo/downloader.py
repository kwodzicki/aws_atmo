import logging
from logging.handlers import QueueHandler
import signal
import os, glob, time
from datetime import datetime, timedelta

from threading import Thread
from multiprocessing import Process, Event, Queue

import boto3
import boto3.session

from .handlers import mpLogHandler

TIMEOUT   = 1.0


class DownloadStats( dict ):
  """Store statistics about downloads in an AWS_Downloader process"""

  TEMPLATE = {'nFail'    : 0,
              'nSuccess' : 0,
              'totSize'  : 0,
              'dt'       : 0.0}

  def __getitem__(self, key):

    if key not in self:                                                         # If key is NOT in instance
      self[key] = self.TEMPLATE.copy()                                          # Add copy of TEMPLATE to the instance under key
    return self.get(key)                                                        # Return data

###############################################################################
class AWS_Downloader( Process ):

  ATTEMPT_FMT = '     Download attempt {:2d} of {:2d} : {}' 
  EXISTS_FMT  = '     File already downloaded : {}'
  FAILED_FMT  = '     Failed to download : {}'
  DLRATE_FMT  = '     {} sync complete. Rate: {:5.1f} MB/s' 
  PDONE_FMT   = '     AWS Download process finished; Downloaded {} bytes in {} seconds'

  def __init__(self, resource, bucketName, fileQueue, logQueue = None, **kwargs):
    """
    Inputs:
        resource   : The AWS resource to use for boto3 initialization
        bucketName : Name of the bucket to download data from
        event      : A multiprocess.Event object used to cleanly
                        end process
        All other arguments accepted by multiprocess.Process
    Keywords:
        attempts   : Maximum number of times to try to download
                        a file. Default is 3
        clobber    : Set to overwrite exisiting files.
                        Default is False
        All other keywords accepted by multiprocess.Process
    """
    super().__init__( )

    self._resource    = resource                                                # Resource to use for downloading
    self._bucketName  = bucketName                                              # Bucket to download from
    self._fileQueue   = fileQueue                                               # Queue for files to download
    self._logQueue    = logQueue                                                # Queue for logging
    self._returnQueue = Queue(1)                                                # Create Queue with depth of one (1) for value returned from process
    self._killEvent   = kwargs.get('killEvent', Event())                        # If no killEvent keywords, Initialize Event
    self._stopEvent   = kwargs.get('stopEvent', Event())                        # If no stopEvent keyword, initialize Event
    self._attempts    = kwargs.get('attempts',  3)                              # If no attempt keyword set to 3
    self._clobber     = kwargs.get('clobber',   False)                          # If no clobber keyword set to False

  def _running(self):
    """Check if processes should still be running"""
    check1 = not self._stopEvent.is_set() or not self._fileQueue.empty()
    return check1 and not self._killEvent.is_set()

  def run(self):
    """This code is run in separte process"""

    t0      = time.monotonic()                                                  # Start time of process
    stats   = DownloadStats()                                                   # To store download statistics
    totSize = 0                                                                 # Total download size for process

    session = boto3.session.Session()                                           # Create own session as per https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html
    s3conn  = session.resource(self._resource )                                 # Start client to AWS s3
    bucket  = s3conn.Bucket( self._bucketName )                                 # Connect to bucket
    log     = logging.getLogger( __name__ )
    log.setLevel( logging.DEBUG )
    if self._logQueue:
       log.addHandler( QueueHandler( self._logQueue ) )                         # Add Queue Handler to the log
    
    while self._running():
      try:
        label, key, size, localFile = self._fileQueue.get(True, TIMEOUT)            # Try to get information from the queue, waiting half a second
      except Exception as err:                                                      # If failed to get something from the queue
        continue                                                                    # Continue to beginning of while loop

      dt = 0.0                                                                      # Inititlize download time
      if (len( glob.glob('{}*'.format(localFile)) ) > 0) and self._clobber is False:  # If the file has already been downloaded, or is being downloaded; aws puts .RANDOMHASH on file names while downloading
          log.debug( self.EXISTS_FMT.format( key ) )
          stats[label]['nSuccess'] += 1                                             # Increment number of successful downloads; size variables NOT incremented because didn't download anything
      else:                                                                         # Else, we will try to download it
        statObj = bucket.Object( key )                                              # Get object from bucket so that we can download
        attempt = 0                                                                 # Set attempt number to zero
        while (attempt < self._attempts) and not self._killEvent.is_set():          # While we have not reached maximum attempts
          log.debug( self.ATTEMPT_FMT.format(attempt+1, self._attempts, key) )                          # Log some info
          t1 = time.monotonic()                                                     # Start time of download
          try:
            statObj.download_file(localFile)                                        # Try to download the file
            info = os.stat(localFile)                                               # Get file info
            if (info.st_size != size):                                              # If file size is NOT correct OR clobber is set
                raise Exception('File size mismatch!')
          except:
            attempt += 1                                                            # On exception, increment attempt counter
          else:
            dt      = dt + (time.monotonic() - t1)                                  # Increment dt by the time it took to download current file
            attempt = self._attempts + 1                                            # Else, file donwloaded so set attempt to 1 greater than maxAttempt
 
        if (attempt == self._attempts):                                             # If the download attempt matches maximum number of attempts,then all attempts failed
          stats[label]['nFail'] += 1                                                # Number of failed donwloads for thread
          log.error( self.FAILED_FMT.format(key) )                   # Log error
          try:
            os.remove( localFile )                                                  # Delete local file if it exists
          except:
            pass
        else:
          stats[label]['nSuccess'] += 1                                             # Number of successful downloads for thread
          stats[label]['totSize']  += size                                          # Size of downloads for given label
          totSize += size                                                           # Size of downloads for process
        statObj = None                                                              # Set to None for garbage collection of object

      stats[label]['dt'] += dt

      dlRate = (size / 1.0e6 / dt) if (dt > 0.0) else 0.0                           # Compute the download rate for the station
      log.info( self.DLRATE_FMT.format( key, dlRate ) )

    # Set all to None for garbage collection; may fix the SSLSocket error issue
    bucket  = None 
    s3conn  = None
    session = None      
    if self._killEvent.is_set():                                                        # If killEvent set
      log.error('Received SIGINT; download cancelled.')                                 # Log an errory
      while not self._fileQueue.empty():                                                    # While the queue is NOT empty
        try:
          label, key, size, localFile = self._fileQueue.get_nowait()
        except:
          break
        else:
          stats[label]['nFail'] += 1

    log.debug( self.PDONE_FMT.format(totSize, time.monotonic()-t0)  )

    self._returnQueue.put( stats )                                                  # Return # success, # failed, and download size to the queue

  def stop(self):
    """Call to have process stop when queue becomes empty"""

    self._stopEvent.set()

  def kill(self):
    """Call to have process stop once current download is finished"""

    self._killEvent.set()

  def join(self, *args):
    """Wait for process to finish and return process return value"""

    super().join(*args)
    if self.is_alive():
      return None
    try:
      val = self._returnQueue.get()
    except:
      val = None
    finally:
      self._returnQueue.close()
    
    return val

class AWS_Scheduler(object):
  """
  This class was developed to cleanly close downloading processes when
  an interupt signal is received. On interupt, an event is set that
  tells the download processes to stop. While this will not happen
  instantly, trust that the processes are finishing what they are
  working on and closing.
  """

  def __init__(self, resource, bucketName, clobber=False, maxAttempt=3, concurrency=4):
    """
    Initialize downloader processes for concurrent downloading of data.

    Arguments:
      resource (str) : The AWS resource to use
      bucketName (str) : Name of the AWS bucket to use

    Keyword arguments:
      clobber (bool) : Boolean that enables/disables file clobbering
      maxAttempt (int)  : Integer maximum number of download retries
      concurrency (int) : Integer number of concurrent downloads to allow

    """

    self.log        = logging.getLogger(__name__)                                 # Initialize logger for the class

    self.outdir     = None                                                 # Attribute for output directory
    self.t0         = None                                                 # Attribute for start time of download 

    self.clobber    = clobber
    self.s3conn     = boto3.resource(resource)                             # Start client to AWS s3
    self.bucket     = self.s3conn.Bucket(bucketName)                       # Connect to bucket

    self.fileQueue  = Queue( 10 )
    self.logQueue   = Queue( )
    self.killEvent  = Event()
    self.stopEvent  = Event()

    self.logThread  = Thread(target=mpLogHandler, args=(self.logQueue,))   # Initialize thread to consume log message from queue
    self.logThread.start()                                                 # Start the thread

    self.tids       = []                                                   # List to store download process objects
    for i in range( concurrency ):                                          # Iterate over number of concurrency allowed
        tid = AWS_Downloader(
                resource, bucketName, self.fileQueue, self.logQueue, 
                attempts = maxAttempt, clobber = clobber,
                killEvent = self.killEvent, stopEvent = self.stopEvent )         # Initialize a download process
        tid.start()                                                                     # Start the process
        self.tids.append( tid )                                                         # Append process to the list of processes

    signal.signal( signal.SIGINT, self.cancel )                            # On SIGINT, set the killEvent
    
  def cancel( self, *args, **kwargs ):
    """Cancel all downloads as soon as current download finishes"""

    self.log.critical( 'Cancelling download!!!' )
    self.killEvent.set()

  ############################################################################
  def download(self, *args, **kwargs ):
    """Overload with filtering for given dataset"""

    pass


  def close(self):

    while not self.fileQueue.empty():
      _ = self.fileQueue.get()
    self.fileQueue.close()

    self.logQueue.put(None)                                                                  # Put None in to the logQueue, this will cause the thread the stop
    self.logThread.join()                                                               # Join the thread to make sure it finishes 
    self.logQueue.close()

  def wait(self):
    """
    Wait for all download processes to finish

    Returns:
      tuple : Output directory for data files, # successful downloads,
        # failed downloads, and total size of all downloaded files.

    """

    for tid in self.tids: tid.stop()                                            # Tell each process to stop once no more data in queue

    allStats = {}
    for tid in self.tids:                                                   # Iterate over the process objects
      vals = tid.join()                                                        # Join process, blocks until finished
      if vals is None:
        self.log.warning('There was an error with a download process!')
      else:
        for label, stats in vals.items():
          if label not in allStats:
            allStats[label] = stats 
          else:
            for key, val in stats.items():
              allStats[label][key] += val

    nSuccess = 0
    nFail    = 0
    totSize  = 0
    for label, stats in allStats.items():
      nSuccess += stats['nSuccess']
      nFail    += stats['nFail']
      totSize  += stats['totSize']


    elapsed = time.time() - self.t0                                                     # Compute elpased time
    self.log.info( 'AWS_Scheduler - complete' )
    self.log.info( '   Downloaded       : {:10d} files'.format(  nSuccess) )
    self.log.info( '   Failed           : {:10d} files'.format(  nFail))
    self.log.info( '   Data transferred : {:10.1f} MB'.format(   totSize / 1.0e6))
    self.log.info( '   Transfer Rate    : {:10.1f} MB/s'.format( totSize / 1.0e6 / elapsed ) )
    self.log.info( '   Elapsed time     : {:10.1f} s'.format(elapsed))

    if (nFail == 0):
      self.log.info('No failed file syncs.')
    else:
      self.log.warning('Some files failed to sync!')

    return self.outdir, nSuccess, nFail, totSize


#signal.signal( signal.SIGINT, lambda a, b: KILLEVENT.set() )                            # On SIGINT, set the killEvent
