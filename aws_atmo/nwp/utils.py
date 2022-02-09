import logging

import re
def parseIDX( idxData, *args):
  """
  Parse IDX file for patterns of interest

  Use REGEX to find records in the IDX file that match the
  pattern(s) provided. If NO patterns found, then a None value
  is returned. Otherwise, the start/stop indices of all variables
  found are returned.

  Arguments:
    idxData (bytes,str) : Data from the idx file
    *args (str) : Any number of substrings to match records to

  Keyword arguments:
    None.

  Returns:
    list : List of strings containing ranges of bytes offsets into grib file

  """

  log = logging.getLogger(__name__)
  if isinstance(idxData, bytes): idxData = idxData.decode()                     # If the idxData in bytes, decode to string
  pattern = "^.*(?:{}).*$".format( '|'.join( args ) )                           # Generate regex pattern to search for
  matches = re.findall( pattern, idxData, re.MULTILINE )                        # Search the idx data for the patterns
  if len(matches) > 0:                                                          # If at least one (1) match found
    if len(matches) != len(args):
      log.debug('Missing some variables!')                                      # If not all matched; warning
    records = idxData.splitlines()                                              # Split records by line
    ranges  = [None] * len(matches)                                             # Initialize list of strings for matches
    starts  = [0]
    for i, match in enumerate( matches ):                                       # Iterate over matches
      index     = records.index( match )
      offset    = match.split(':')[1]
      ranges[i] = [offset, '']                                                  # Store the offset in ranges at index i
      try:                                                                      # Try to
        nextRec = records[ index+1 ]                                            # Get the next record; remember record indices start at one (1), whereas python indexes starting at zero (0), so no need to add one to get next record
      except:                                                                   # On exception
        pass                                                                    # We are at end of records, so we don't need to do anything
      else:                                                                     # Else, we got a next record
        index        = records.index( nextRec )                                 # Get index of next record in list
        offset       = int(nextRec.split(':')[1])                               # Get the index and offset of the next record
        ranges[i][1] = offset-1                                                 # Add and end point to the range at index i; remember that the offset is the start of the next record, so we need to decrement by one (1) to get end of record of interest
        starts.append( offset - int(ranges[i][0]) + starts[-1] )                # Compute offset for NEXT record (if there is one) based on width of GRIB block AND the start of record
    log.debug( 'Will grab data in range: {}'.format( ranges[i] ) )

    for i in range( len(matches) ):                                             # Iterate over matches
      tmp = matches[i].split(':')                                               # Split match on colon
      tmp[0] = str(i)                                                           # Set new record index
      tmp[1] = str(starts[i])                                                   # Set new start offset
      matches[i] = ':'.join( tmp )                                              # Update record in matches

    return ranges, matches                                                      # Return the list of ranges and matches

  log.error('No variables found matching pattern' )                             # If made here, print warning

  return None                                                                   # Return None
