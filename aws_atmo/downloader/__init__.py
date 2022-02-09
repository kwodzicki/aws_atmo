import logging
from logging.handlers import QueueHandler

import os, signal, glob, time

from threading import Thread
from multiprocessing import Process, Event, Queue

import boto3

from ..handlers import mpLogHandler
from .utils import download
from .stats import StatsCollection, humanReadable

TIMEOUT   = 1.0
 
class AWS_Downloader( Process ):

  ATTEMPT_FMT = '     Download attempt {:2d} of {:2d} : {}' 
  EXISTS_FMT  = '     File already downloaded : {}'
  FAILED_FMT  = '     Failed to download : {}'
  DLRATE_FMT  = '     {} sync complete. Rate: {}' 
  PDONE_FMT   = '     AWS Download process finished; Downloaded {} in {:0.3f} seconds - Rate : {}'

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
    self._retries     = kwargs.get('retries',   3)                              # If no attempt keyword set to 3
    self._clobber     = kwargs.get('clobber',   False)                          # If no clobber keyword set to False

  def _running(self):
    """Check if processes should still be running"""

    check1 = not self._stopEvent.is_set() or not self._fileQueue.empty()
    return check1 and not self._killEvent.is_set()

  def run(self):
    """This code is run in separte process"""

    _ = signal.signal(signal.SIGINT, signal.SIG_IGN)                            # Set to ingnore interupt signals in the process; we handle things nicely from top-level

    session = boto3.session.Session()                                           # Create own session as per https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html
    s3conn  = session.resource(self._resource )                                 # Start client to AWS s3
    bucket  = s3conn.Bucket( self._bucketName )                                 # Connect to bucket
    log     = logging.getLogger( __name__ )
    if self._logQueue:
       log.addHandler( QueueHandler( self._logQueue ) )                         # Add Queue Handler to the log

    t0      = time.monotonic()                                                  # Start time of this download process
    stats   = StatsCollection()                                                 # To store download statistics
    totSize = 0                                                                 # Total download size for process
    
    while self._running():                                                      # While running
      try:
        label, key, localFile, offsets = self._fileQueue.get(True, TIMEOUT)     # Try to get information from the queue, waiting half a second
      except Exception as err:                                                  # If failed to get something from the queue
        continue                                                                # Continue to beginning of while loop

      dt   = 0.0                                                                # Inititlize download time
      size = 0                                                                  # Initialize file download size
#      if (len( glob.glob('{}*'.format(localFile)) ) > 0) and self._clobber is False:  # If the file has already been downloaded, or is being downloaded; aws puts .RANDOMHASH on file names while downloading
#          log.debug( self.EXISTS_FMT.format( key ) )
#          stats[label].success( 0, 0)                                          # Increment number of successful downloads; size variables NOT incremented because didn't download anything
      if os.path.isfile( localFile ) and self._clobber is False:                # If the file has already been downloaded, or is being downloaded; aws puts .RANDOMHASH on file names while downloading
        log.debug( self.EXISTS_FMT.format( key ) )
        stats[label].success( 0, 0)                                           # Increment number of successful downloads; size variables NOT incremented because didn't download anything
      else:                                                                     # Else, we will try to download it
        s3obj   = bucket.Object( key )                                          # Get object from bucket so that we can download
        retries = attempt = self._retries                                       # Set retries and attempt to the retry limit 
        t1      = time.monotonic()                                              # Start time of download
        log.debug( f'Attempting download to : {localFile}' )
        while (retries > 0) and not self._killEvent.is_set():                   # While we have not reached maximum attempts
          log.debug( 
            self.ATTEMPT_FMT.format(attempt-retries+1, self._retries, key)
          )                                                                     # Log some info

          size = download( s3obj, localFile, offsets )                          # Attempt a download
          if size == 0:                                                         # If the size returned from download is zero (0)
            retries -= 1                                                        # Download failed so decrement retries
          else:                                                                 # Else
            break                                                               # Break out of the while loop

        if retries != 0:                                                        # If the download attempt matches maximum number of attempts,then all attempts failed
          dt       = (time.monotonic() - t1)                                    # Increment dt by the time it took to download current file
          totSize += size                                                       # Size of downloads for process
          stats[label].success( size, dt )                                      # Number of failed donwloads for thread
        else:                                                                   # Else, downloaded the chunk/file
          stats[label].fail( )                                                  # Number of successful downloads for thread
          log.error( self.FAILED_FMT.format(key) )                              # Log error
          try:                                                                  # To to remove the file
            os.remove( localFile )                                              # Delete local file if it exists
          except:
            pass
        s3obj = None                                                            # Set to None for garbage collection of object

      log.info( self.DLRATE_FMT.format( key, humanReadable( size, dt ) ) )

    # Set all to None for garbage collection; may fix the SSLSocket error issue
    bucket  = None 
    s3conn  = None
    session = None      
    if self._killEvent.is_set():                                                # If killEvent set
      log.error('Received SIGINT; download cancelled.')                         # Log an errory
      while not self._fileQueue.empty():                                        # While the queue is NOT empty
        try:
          label, key, localFile, offsets = self._fileQueue.get_nowait()
        except:
          break
        else:
          stats[label].fail()

    dt      = time.monotonic() - t0                                             # Compute runtime for the process
    rate    = humanReadable( totSize, dt )                                      # Compute average download rate of process
    totSize = humanReadable( totSize )                                          # Total size downloaded by process
    log.debug( self.PDONE_FMT.format( totSize, dt, rate ) )                     # Log information

    self._returnQueue.put( stats )                                              # Place stats object into the return queue

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

  def __init__(self, resource, bucketName, clobber=False, retries=3, jobs=4):
    """
    Initialize downloader processes for concurrent downloading of data.

    Arguments:
      resource (str) : The AWS resource to use
      bucketName (str) : Name of the AWS bucket to use

    Keyword arguments:
      clobber (bool) : Boolean that enables/disables file clobbering
      retries  (int)  : Integer maximum number of download retries
      jobs (int) : Integer number of concurrent downloads to allow

    """

    self.log        = logging.getLogger(__name__)                               # Initialize logger for the class

    self.outdir     = None                                                      # Attribute for output directory
    self.t0         = None                                                      # Attribute for start time of download 

    self.clobber    = clobber
    self.s3conn     = boto3.resource(resource)                                  # Start client to AWS s3
    self.bucket     = self.s3conn.Bucket(bucketName)                            # Connect to bucket

    self.fileQueue  = Queue( 10 )                                               # Limit fileQueue to 10 so that doesn't grow too large
    self.logQueue   = Queue( )                                                  # Queue for logging from separate processes
    self.killEvent  = Event()                                                   # Event to cleanly kill downloads
    self.stopEvent  = Event()                                                   # Event to cleanly stop download processes when the fileQueue is empty

    self.logThread  = Thread(target=mpLogHandler, args=(self.logQueue,))        # Initialize thread to consume log message from queue
    self.logThread.start()                                                      # Start the thread

    self.tids       = []                                                        # List to store download process objects
    for i in range( jobs ):                                                     # Iterate over number of concurrency allowed
        tid = AWS_Downloader(
                resource, bucketName, self.fileQueue, self.logQueue, 
                retries = retries, clobber = clobber,
                killEvent = self.killEvent, stopEvent = self.stopEvent )        # Initialize a download process
        tid.start()                                                             # Start the process
        self.tids.append( tid )                                                 # Append process to the list of processes

    signal.signal( signal.SIGINT, self.cancel )                                 # On SIGINT, set the killEvent
    
  def cancel( self, *args, **kwargs ):
    """Cancel all downloads as soon as current download finishes"""

    self.log.critical( 'Cancelling download!!!' )
    self.killEvent.set()

  ############################################################################
  def download(self, *args, **kwargs ):
    """
    Overload with filtering for given dataset

    NOTE :
      You should call this method using super().download() at the beginning
      of your overloaded method so that download timings can be computed
    """

    self.t0 = time.monotonic()
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

    stats = StatsCollection()
    for tid in self.tids:                                                   # Iterate over the process objects
      vals = tid.join()                                                        # Join process, blocks until finished
      if vals is None:
        self.log.warning('There was an error with a download process!')
      else:
        stats = stats + vals

    nSuccess, nFail, totSize, dt = stats.totals()

    self.log.info( 'AWS_Scheduler - complete' )
    self.log.info( '   Downloaded       : {:10d} files'.format(  nSuccess) )
    self.log.info( '   Failed           : {:10d} files'.format(  nFail))
    self.log.info( '   Data transferred : {:>10}'.format( humanReadable( totSize ) ) )
    try:                                                                        # Try to
      elapsed = time.monotonic() - self.t0                                      # Compute elpased time using the start time of the full download
    except:                                                                     # On exception (perhaps someone forgot to call super().download()
      pass                                                                      # Do nothing
    else:                                                                       # Else, we know the elapsed time, so print some more statistics
      self.log.info( '   Transfer Rate    : {:>10}'.format( humanReadable( totSize, elapsed ) ) )
      self.log.info( '   Elapsed time     : {:10.1f} s'.format(elapsed))

    if (nFail == 0):
      self.log.info('No failed file syncs.')
    else:
      self.log.warning('Some files failed to sync!')

    return self.outdir, nSuccess, nFail, totSize
