import logging
import boto3
import boto3.session

from threading import Thread

class AWS_Downloader(Thread):

    def __init__(self, resource, bucketName, queue, event, attempts, *args, **kwargs):
        super().__init__(*args, **kwargs);
        self._log        = logging.getLogger(__name__)
        self._queue      = queue
        self._event      = event
        self._resource   = resource
        self._bucketName = bucketName
        self._attempts   = attempts
        self._nFail      = 0
        self._nSuccess   = 0
        self._Size       = 0

    def run(self):
        """Code to run in separte thread that handles downloading"""

        session = boto3.session.Session()                                       # Create own session as per https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html
        s3conn  = boto3.resource(self._resource )                               # Start client to AWS s3
        bucket  = s3conn.Bucket( self._bucketName )                             # Connect to bucket
        
        while (not self._event.is_set()) or (not self._queue.empty()):          # While the event is NOT set OR the queue is NOT empty
            try:
                key, size, localFile = self._queue.get( timeout = 0.5 )
            except:
                continue
            else:
                self._queue.task_done()
            statObj = bucket.Object( key );
            attempt = 0
            while (attempt < self._attempts):                                   # While we have not reached maximum attempts
                self._log.info('Download attempt {:2d} of {:2d} : {}'.format(
                                attempt+1, self._attempts, key));               # Log some info
                try:
                    statObj.download_file(localFile);                           # Try to download the file
                except:
                    attempt += 1;                                               # On exception, increment attempt counter
                else:
                    attempt = self._attempts + 1;                               # Else, file donwloaded so set attempt to 1 greater than maxAttempt

            if (attempt == self._attempts):                                     # If the download attempt matches maximum number of attempts,then all attempts failed
                self._nFail += 1
                self._log.error('Failed to download : {}'.format(key) );        # Log error
            else:
                self._nSuccess += 1
                self._Size     += size

    def getStats(self):
        """Get status of downloading"""

        return self._nSuccess, self._nFail, self._Size
