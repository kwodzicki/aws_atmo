import logging
from subprocess import Popen, PIPE, STDOUT

from . import LOG 
###############################################################################
class EMailHandler( logging.Handler ):
  def __init__(self, toemails, subject, *args, **kwargs):
    super().__init__(*args, **kwargs);                                              # Initialize base class
    if not isinstance(toemails, (list)): toemails = [toemails];                     # Convert toemails variable to list
    self.toemails = toemails;                                                       # Set toemails class attribute
    self.subject  = subject;                                                        # Set subject class attribute
  ##################################
  def emit(self, record):                                                           # Overload the emit method                        
    logFiles = [];                                                                  # List for any log files attached to logger
    for handler in LOG.handlers:                                          # Iterate over all handlers in the DCOTSS_py.log
        if isinstance(handler, logging.FileHandler):                                # If the logger is a FileHandler
            handler.flush();                                                        # Flush all logs to file
            handler.close();                                                        # Close the file
            logFiles.append( handler.baseFilename );                                # Append the log file path to the logFiles list
            LOG.removeHandler( handler );                                 # Remove the handler from the logger

    body   = ['echo', record.getMessage()];                                         # Command for piping body of email into the mail utitlity
    mail1  = ['mail', '-s', '{} - {}'.format(record.levelname, self.subject)];      # Base mail command; NO attachements; this is fall back
    mail2  = mail1.copy();                                                          # Copy the mail1 command list into mail2 variable
    for f in logFiles: mail2 += ['-a', f];                                          # Iterate over all log files and add as attachments to mail2 command

    mail1 += self.toemails;                                                         # Add emails to mail1 command
    mail2 += self.toemails;                                                         # Add emails to mail2 command

    proc1  = Popen(body,  stdout=PIPE, stderr=STDOUT);                              # Run body command and send stdout and stderr to PIPE
    proc2  = Popen(mail2, stdin=proc1.stdout, stdout=PIPE, stderr=STDOUT);          # Run mail2 command with PIPE from body as input
    proc1.stdout.close();                                                           # Close the PIPE from body command
    proc2.communicate();                                                            # Wait for mail command to finish
    if (proc2.returncode != 0):                                                     # If return code from mail is NOT 0, then probably does NOT accept the attachments
        proc1  = Popen(body,  stdout=PIPE, stderr=STDOUT);                          # Run body command again
        proc2  = Popen(mail1, stdin=proc1.stdout, stdout=PIPE, stderr=STDOUT);      # Run mail1 command
        proc1.stdout.close();                                                       # Close the PIPE from body command
        proc2.communicate();                                                        # Wait for mail command to finish
 
###############################################################################
def mpLogHandler( queue ):
    while True:                                                                 # Iterate forever
        record= queue.get();                                                      # Apparently just getting the message from the queue will log it
        if (record is None): break                                                 # If the message is None, then break; this will kill the thread
        logger = logging.getLogger( record.name )
        logger.handle( record )
