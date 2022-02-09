FACTORS = (1.0e9, 1.0e6, 1.0e3)
PREFIX  = ( 'GB',  'MB',  'KB')

def humanReadable(size, dt = None):
  """
  Convert number of bytes to a human-readable format

  Given a number of bytes, convert to GB, MB, KB, or B format with
  proper units. If a time (seconds) is supplied, then the rate
  is determined and formatted nicely as GB/s, MB/s, KB/s, B/s

  Example:
    >>>print( humanReadable( 10**6 ):
    1.0MB

    >>>print( humanReadable( 10**6, 1.0 )
    1.0MB/s

  Arguments:
    size (int) : Number of bytes

  Keyword arguments:
    dt (int,float) : Download time (seconds)

  Returns:
    str : Human-readable size

  """

  strfmt = '{:0.1f} {}'                                                         # Format for pretty human readable
  if isinstance( dt, (int, float) ):                                            # If dt is int or float 
    strfmt += '/s'                                                              # Append per second to strfmt
    if dt > 0.0: size /= dt                                                     # Divide size by dt if dt > 0

  for factor, prefix in zip( FACTORS, PREFIX ):                                 # Iterate over factors and prefixes
    if size >= factor:                                                          # If the size is greater or equal to factor
      return strfmt.format( size/factor, prefix )                               # Return formatted string
  return strfmt.format( size, 'B' )                                             # If made here, then is only bytes, return formatted string


class DownloadStats( object ):
  """Store statistics downloads in a download process"""

  def __init__(self, nSuccess = 0, nFail = 0, size = 0, dt = 0.0 ):
    """
    Arguments:
      None

    Keyword arguments:
      nSuccess (int) : Number of successful downloads
      nFail (int) : Number of failed downloads
      size (int) : Size of all successful downloads; in bytes
      dt (float) : Time it took for all downloads; in seconds

    """

    self._nSuccess = nSuccess
    self._nFail    = nFail
    self._size     = size
    self._dt       = dt

  def __repr__(self):

    attempts = self._nSuccess + self._nFail
    size     = humanReadable( self.size )
    rate     = humanReadable( self.size, self.dt ) 
    return f"<Success : {self._nSuccess} - Failed : {self._nFail} - Attempts : {attempts} - Size : {size} - Time : {self._dt} - Rate : {rate}/S>"
 
  def __add__(self, other):

    if isinstance(other, DownloadStats):
      return DownloadStats( 
        self._nSuccess + other._nSuccess,
        self._nFail    + other._nFail,
        self._size     + other._size,
        self._dt       + other._dt
      )

  @property
  def nSuccess(self):
    return self._nSuccess
  @property
  def nFail(self):
    return self._nFail
  @property
  def size(self):
    return self._size
  @property
  def dt(self):
    return self._dt

  def success(self, size, dt):
    """
    Method to signal successful download

    Arguments:
      size (int) : Size of the download
      dt (float) : Time it took to download

    """

    self._nSuccess += 1                                                         # Increment # of successful downloads
    self._size     += size                                                      # Increment total size of downloads
    self._dt       += dt                                                        # Increment download time

  def fail(self):
    """Method to signal failed download"""

    self._nFail += 1                                                            # Increment # of fails

class StatsCollection( dict ):
  """Store statistics about downloads in an AWS_Downloader process"""

  def __repr__(self):

    nSuccess, nFail, size, dt = self.totals()
    attempts = nSuccess + nFail
    rate     = humanReadable( size, dt )
    txt      = f"<Successes : {nSuccess} - Failures : {nFail} - Attempts : {attempts} - Size : {size} - Time : {dt} - Rate : {rate}/S>"
    #for key, val in self.items():
    #  txt += f'{os.linesep}  {key} {val}'
    return txt 

  def __getitem__(self, key):

    if key not in self:                                                         # If key is NOT in instance
      self[key] = DownloadStats()                                               # Initialize new DownloadStats under key
    return self.get(key)                                                        # Return data

  def __add__(self, other):

    if isinstance(other, StatsCollection):
      out = StatsCollection( **self )                                           # Create new collection to return
      for key, val in other.items():                                            # Iterate over all key/values in other
        if key in out:                                                          # If key is in out
          out[key] = out[key] + val                                             # Add the values together
        else:                                                                   # Else
          out[key] = val                                                        # Store value from other
      return out                                                                # Return out

  def totals(self):
    """
    Get sum of all statistics for all objects in the collection

    Returns:
      tuple : # of successful downloads, # of failed downloads,
        total size of downloaded data (bytes), time for downloads
        to complete (seconds)

    """

    nSuccess, nFail, size, dt = 0, 0, 0, 0.0                                    # Initialze values for summing
    for key, val in self.items():                                               # Iterate over all stats in the collection
      nSuccess += val.nSuccess                                                  # Sum # success
      nFail    += val.nFail                                                     # Sum # fail
      size     += val.size                                                      # Sum size
      dt       += val.dt                                                        # Sum time

    return nSuccess, nFail, size, dt                                            # Return totals
