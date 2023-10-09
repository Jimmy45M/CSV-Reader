# Character Separated Values (CSV) Reader

This PL/SQL package allows you to read many types of character separated files; including the ubiquitous CSV and TSV files.

## Why?

There are loads of options to read CSV files - it's not hard.  So why use this package?

Well, reading _simple_ CSV files is pretty straightforward.  But it gets a lot harder once you start allowing for quoting or escaping of characters and/or have data that includes newlines.

This package handles all that.  Plus it has a reasonable set of options that should cater for most normal CSV type files.

## Usage

See the package specification for details on the procedures and options available plus  some simple use examples.

## Limitations / Notes

The maximum length of any field (including any whitespace that may later get stripped) is 32767 bytes.

The pipelined table functions (for use in SQL) only return the first 99 columns.

When reading from a file (not a file pointer), the package first reads the file into a CLOB and then parses the CLOB.  This is to work around issues that UTL_FILE has with supporting character sets, long line lengths and different line endings.

When reading from a file pointer:
   + the caller is responsible for opening and closing the pointer
   + the file must have been opened with a MAX_LINESIZE larger than any possible line length
   + the file must be open in text mode
   + the file must be in the DB character set
   + line endings are those allowed by UTL_FILE (equivalent to the Line Endings option being set to G_ANY)
   + line endings are normalised to Unix line endings (Normalise Embedded Newlines is G_UNIX)
