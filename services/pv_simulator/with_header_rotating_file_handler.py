from logging.handlers import RotatingFileHandler
from logging import FileHandler
import os


class WithHeaderRotatingFileHandler(RotatingFileHandler):
    def __init__(self, filename, header, mode='a', maxBytes=0, backupCount=0, encoding=None, delay=False):
        self._header = header
        self._file_pre_exist = os.path.exists(filename)

        RotatingFileHandler.__init__(self, filename, mode, maxBytes, backupCount, encoding, delay)

        # Write the header if delay is False and a file stream was created.
        if not delay and self.stream is not None:
            self.stream.write('%s\n' % header)

    def emit(self, record):
        # Create the file stream if not already created.
        if self.stream is None:
            self.stream = self._open()

            # If the file pre_exists, it should already have a header.
            # Else write the header to the file so that it is the first line.
            if not self.file_pre_exists:
                self.stream.write('%s\n' % self.header)

        # Call the parent class emit function.
        FileHandler.emit(self, record)

