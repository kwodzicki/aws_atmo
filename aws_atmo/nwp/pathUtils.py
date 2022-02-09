import os


def hrrrPath( model, domain, initDate ):

  return initDate.strftime( f'{model}.%Y%m%d' ), domain

def namPath( model, domain, iniDate ):
  pass

def gfsPath( model, domain, initDate ):
  return initDate.strftime( f'{model}.%Y%m%d' ), initDate.strftime('%H'), domain

def nwpPath( model, domain, initDate ):

  model = model.lower()
  if model == 'hrrr':
    return hrrrPath( model, domain, initDate )
  elif model == 'gfs':
    return gfsPath( model, domain, initDate )
    
  return None
