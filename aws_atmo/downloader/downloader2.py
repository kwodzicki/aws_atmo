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
    

###############################################################################
class AWS_Downloader( Process ):

  def __init__(self, resource, bucketName, stopEvent, killEvent, fileQueue, logQueue = None, **kwargs):
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
    attempts = kwargs.pop('attempts', 3)
    clobber  = kwargs.pop('clobber', False)

    super().__init__( **kwargs )

    self._resource    = resource
    self._bucketName  = bucketName
    self._stopEvent   = stopEvent
    self._killEvent   = killEvent
    self._fileQueue   = fileQueue
    self._logQueue    = logQueue
    self._returnQueue = Queue(1)
    self._attempts    = attempts
    self._clobber     = clobber

  def run(self):
    t0      = time.time()
    session = boto3.session.Session()                                      # Create own session as per https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html
    s3conn  = boto3.resource(self._resource )                              # Start client to AWS s3
    bucket  = s3conn.Bucket( self._bucketName )                             # Connect to bucket
    log     = logging.getLogger(__name__)
    if self._logQueue:
       log.addHandler( QueueHandler( self._logQueue ) )                       # Add Queue Handler to the log

    stats     = {}
    statsTemp = {'nFail'    : 0,
                 'nSuccess' : 0,
                 'totSize'  : 0,
                 'dt'       : 0.0}

    totSize   = 0
    while (not self._stopEvent.is_set() or not self._fileQueue.empty()) and not self._killEvent.is_set():  # While the event is NOT set OR the queue is NOT empty
      log.debug( self._killEvent )
      #log.debug( f'Stop event : {self.STOPEVENT.is_set()}' )
      #log.debug( f'file queue : {self._fileQueue.empty()}' ) 
      #log.debug( f'Kill event : {self.KILLEVENT.is_set()}' )
      #break
      try:
        label, key, size, localFile = self._fileQueue.get(True, TIMEOUT)                # Try to get information from the queue, waiting half a second
      except Exception as err:                                                          # If failed to get something from the queue
        continue                                                                        # Continue to beginning of while loop

      dt = 0.0
      if label not in stats:
        stats[label] = statsTemp.copy()
      
      if (len( glob.glob('{}*'.format(localFile)) ) > 0) and self._clobber is False:  # If the file has already been downloaded, or is being downloaded; aws puts .RANDOMHASH on file names while downloading
          log.debug(
              '        File already downloaded : {}'.format(key))
          stats[label]['nSuccess'] += 1                                                               # Increment number of successful downloads; size variables NOT incremented because didn't download anything
      else:                                                                           # Else, we will try to download it
        statObj = bucket.Object( key )                                                # Get object from bucket so that we can download
        attempt = 0                                                                   # Set attempt number to zero
        while (attempt < self._attempts) and not self.KILLEVENT.is_set():                  # While we have not reached maximum attempts
          log.debug(
              '        Download attempt {:2d} of {:2d} : {}'.format(
                          attempt+1, self._attempts, key))                            # Log some info
          t0 = time.time()                                                            # Start time of download
          try:
            statObj.download_file(localFile)                                          # Try to download the file
            info = os.stat(localFile)                                                 # Get file info
            if (info.st_size != size):                                                # If file size is NOT correct OR clobber is set
                raise Exception('File size mismatch!')
          except:
            attempt += 1                                                              # On exception, increment attempt counter
          else:
            dt      = dt + (time.time() - t0)                                         # Increment dt by the time it took to download current file
            attempt = self._attempts + 1                                              # Else, file donwloaded so set attempt to 1 greater than maxAttempt
 
        if (attempt == self._attempts):                                               # If the download attempt matches maximum number of attempts,then all attempts failed
          stats[label]['nFail'] += 1                                                  # Number of failed donwloads for thread
          log.error('        Failed to download : {}'.format(key) )                   # Log error
          try:
            os.remove( localFile )                                                    # Delete local file if it exists
          except:
            pass
        else:
          stats[label]['nSuccess'] += 1                                                 # Number of successful downloads for thread
          stats[label]['totSize']  += size                                              # Size of downloads for thread
          totSize += size                                                               # Size of downloads for given station
        statObj = None                                                                  # Set to None for garbage collection of object

      stats[label]['dt'] += dt

      dlRate = (size / 1.0e6 / dt) if (dt > 0.0) else 0.0                           # Compute the download rate for the station
      log.info( f'      {key} sync complete. Rate: {dlRate:5.1f} MB/s' )

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

    log.debug( f'     AWS Download process finished; Download {totSize} bytes in {time.time()-t0} seconds' )

    self._returnQueue.put( stats )                                                  # Return # success, # failed, and download size to the queue

  def stop(self):
    self.STOPEVENT.set()

  def join(self, *args):
    print( 'Joining thread' )
    super().join(*args)
    if self.is_alive():
      return None
    try:
      val = self._returnQueue.get()
    except:
      return None
    else:
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

  STOPEVENT  = Event()
  KILLEVENT  = Event()
  
  def __init__(self, resource, bucketName, clobber, maxAttempt, concurrency):
    """
    Name:
        _initProcesses
    Purpose:
        Private method to initialize downloader processes for concurrent
        downloading of data.
    Inputs:
        resource    : The AWS resource to use
        bucketName  : Name of the AWS bucket to use
        clobber     : Boolean that enables/disables file clobbering
        maxAttempt  : Integer maximum number of download retries
        concurrency : Integer number of concurrent downloads to allow
    Keywords:
        None.
    Outputs:
        None; updates class attributes
    """

    self.log        = logging.getLogger(__name__)                                 # Initialize logger for the class

    self.outdir     = None                                                 # Attribute for output directory
    self.t0         = None                                                 # Attribute for start time of download 

    self.clobber    = clobber
    self.s3conn     = boto3.resource(resource)                             # Start client to AWS s3
    self.bucket     = self.s3conn.Bucket(bucketName)                       # Connect to bucket

    self.fileQueue  = Queue( 10 )
    self.logQueue   = Queue( )


    self.logThread  = Thread(target=mpLogHandler, args=(self.logQueue,))   # Initialize thread to consume log message from queue
    self.logThread.start()                                                 # Start the thread

#    self.STOPEVENT.clear()
#    self.KILLEVENT.clear()

    self.tids       = []                                                   # List to store download process objects
    for i in range( concurrency ):                                          # Iterate over number of concurrency allowed
        tid = AWS_Downloader(
                resource, bucketName, self.STOPEVENT, self.KILLEVENT, self.fileQueue, self.logQueue, 
                attempts = maxAttempt, clobber = clobber)         # Initialize a download process
        tid.start()                                                                     # Start the process
        self.tids.append( tid )                                                         # Append process to the list of processes

  #@property
  #def STOPEVENT( self ):
  #  try:
  #    return self.tids[0]._stopEvent
  #  except:
  #    None

  #@property
  #def KILLEVENT( self ):
  #  try:
  #    return self.tids[0]._killEvent
  #  except:
  #    None

  ############################################################################
  def download(self, *args, **kwargs ):
    """Overload with filtering for given dataset"""

    pass


  def _wait(self):
    """
    Name:
        _wait
    Purpose:
        Private method to wait for all download processes to finish
    Inputs:
        None.
    Keywords:
        None.
    Outputs:
        Returns output directory for data files, # successful downloads,
        # failed downloads, and total size of all downloaded files.
    """

    print(self.STOPEVENT.is_set())                                                                     # Set the event to kill the download processes once their queue empties
    self.STOPEVENT.set()                                                                     # Set the event to kill the download processes once their queue empties
    print(self.STOPEVENT.is_set())                                                                     # Set the event to kill the download processes once their queue empties
    #self.KILLEVENT.set()                                                                     # Set the event to kill the download processes once their queue empties

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

    self.logQueue.put(None)                                                                  # Put None in to the logQueue, this will cause the thread the stop
    self.logThread.join()                                                               # Join the thread to make sure it finishes 

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


signal.signal( signal.SIGINT, lambda a, b: AWS_Scheduler.KILLEVENT.set() )                            # On SIGINT, set the killEvent
