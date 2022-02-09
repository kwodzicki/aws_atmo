import logging

def downloadBytes( obj ):
  """
  Download bytes from AWS object 

  Arguments:
    obj (s3.Object) : An AWS boto3 object to download

  Returns:
    bytes : Raw byte data from remote

  """

  log = logging.getLogger( __name__ )
  try:
    resp = obj.get()
    data = resp['Body'].read()
  except Exception as err:
    log.debug( err )
    return None
  finally:
    resp = None
  
  return data

def downloadChunk( obj, fid, offsets ):
  """
  Download chunk of data from AWS object to file-like object

  Arguments:
    obj (s3.Object) : An AWS boto3 object to download
    fid (file-like) : Full local file path to download data to
    offsets (tuple) : starting and ending bytes to download
  Returns:
    int : Size of data downloaded. If size is 0, then download failed

  """

  log = logging.getLogger( __name__ )
  try:
    resp = obj.get( Range = 'bytes={}-{}'.format( *offsets ) )              # Get response for given range
    size = resp['ContentLength']                                            # Size of chunk to download
    data = resp['Body'].read()                                              # Actually read the data
  except Exception as err:
    log.debug( err )
    return 0
  finally:
    resp = None

  if len(data) == size:
    fid.write( data )                                                       # Write data to the file 
    return size

  return 0 

def downloadFile( obj, fid ):
  """
  Download all data from AWS object to file-like object

  Arguments:
    obj (s3.Object) : An AWS boto3 object to download
    fid (file-like) : Full local file path to download data to

  Returns:
    int : Size of data downloaded. If size is 0, then download failed

  """

  log = logging.getLogger( __name__ )
  try:                                                                      # Try to
    size = obj.content_length
    obj.download_fileobj( fid )
  except Exception as err:
    log.debug( err )
    return 0 
  else:
    fid.seek(0, 2)                                                              # Ensure we are at end of file; AWS does some weird things with threaded downloading that messes up file position?

  if fid.tell() == size:
    return size

  return 0
 
def download( obj, fpath, offsets = None ):
  """
  Download data from AWS to local file

  Arguments:
    obj (s3.Object) : An AWS boto3 object to download
    fpath (str) : Full local file path to download data to

  Keyword arguments:
    offsets (iter) : 

  """

  with open( fpath, 'wb' ) as fid:                                            # Open local file for writing
    if offsets is None:
      return downloadFile( obj, fid )
    else:
      totSize = 0 
      for offset in offsets:                                                              # Iterate over ranges
        size = downloadChunk( obj, fid, offset )
        if size > 0:
          totSize += size
        else:
          return 0
      return totSize 
