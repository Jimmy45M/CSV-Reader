CREATE OR REPLACE PACKAGE csv_reader AS

/*==============================================================================

   CHARACTER SEPARATED VALUES (CSV) READER

   Copyright (C) 2023  Cameron Marshall

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.

================================================================================

This package will parse flat file data where fields are separated by a specified
character and records are terminated by a line break.  Supports many options
including quoting or escaping of special characters and different line endings.

The obvious application for this is to read comma separated values (CSV) files
but it should be able to read many separator based files - provided they use a
single character as their separator.

Data can be read from a CLOB, file or an already open file pointer (see later
for restrictions when reading from a file pointer).


** OPTION SETS **

A number of pre-built option sets are available for common formats (see later
for more exact definitions):
   G_CSV        most CSV files especially those saved from Excel
   G_CSV_SIMPLE same as G_CSV but quoting and escaping are not allowed
   G_CSV_STRICT CSV file that conforms to RFC 4180
   G_TSV        most tab separated files
   G_TSV_SIMPLE same as G_TSV but quoting and escaping are not allowed

The only standard option set that allows embedded new lines is G_CSV_STRICT but
you can enable this after setting the other option sets.

If your file format doesn't match a pre-built set you can specify the closest
match and then override specific options as required.  For example, enabling
strict quoting / embedded newlines for G_CSV or disabling them for G_CSV_STRICT.


** DETAILED OPTIONS **

Separator - single character - required
   The character that is used to separate different fields on a line.
   Note! This is a separator so there is always one more field than separators.

Quote - single character - optional
   Add quote characters around a field to remove special significance from
   characters within the quotes.  To embed a quote character double it or, if
   options allow, escape it.

Escape - single character - optional
   To remove special significance from a character (including the escape char)
   precede it by the escape character.  If an escape is found in quoted data
   its handling is determined by the Quoted Escape Mode option.

Line Endings - enumerator - required
   Specifies the type of line endings used in the data.  Use G_DOS or G_WINDOWS
   for DOS (CR LF), G_UNIX for Unix (LF), G_MAC_OS for old-style Mac OS (CR) or
   G_RISC_OS for the now defunct but much venerated RiscOS (LF CR).

   Alternatively, and recommended, use G_ANY to accept any of the above line
   endings (theoretically, these could be mixed within the same file).

Skip Lines - integer
   Will skip this many lines at the start of the file.
   Can be set while reading the file to skip the next xx lines.

Skip Empty Lines Flag - boolean
   Lines that contain no data (or all spaces if trimming spaces) are ignored.

Skip All Null Lines Flag - boolean
   Lines where all the fields are null will be ignored.  This can be useful if
   reading files saved from Excel as that often writes out dummy lines at the
   end of file which have only null fields (are just a string of commas).

Quote Mode - enumerator
   Specifies how the quote character (if given) is handled.

   G_STRICT indicates quotes, if used, must enclose the entire field (excluding
   not relevant spaces).  All fields that include special characters must be
   quoted (unless escaping is allowed and you choose to escape that field).

   In G_LENIENT mode, if a field starts with a quote then data up to the next
   significant quote is taken to be quoted (if a quote is doubled within this
   data it is not significant, neither are escaped quotes - if quoted escapes
   are allowed).  However, if a field does not start with a quote then quotes
   within the rest of the data are not significant.

   Note! If the data conforms to the strict semantics it will be read the same
   using either G_STRICT or G_LENIENT mode.

   In G_INLINE mode, quotes work like Unix shell quotes.  If a quote is found
   the data up to the next significant quote is taken to be quoted.  You can
   have multiple quoting in the one field.  To match the shell mechanism you
   would normally use an escape and turn off doubled quote mode.

Quoted Escape Mode - enumerator
   Specifies how escape characters found within quoted data are handled.  This
   only applies if you have specified both quoting and escaping, which seems a
   little strange.

   G_NOT_SPECIAL will process them as normal data (escapes lose any significance
   when already quoted).

   G_ESCAPE_ALL will mean that they always escape the next character.

   G_ESCAPE_SPECIAL means they escape the next character if it is a separator,
   quote or escape but otherwise they are treated as normal data (for this case,
   it was deemed newlines are not special characters).

   G_ESCAPE_QUOTE means they only escape the next character if it is a quote.

   The last two options are mostly included for G_INLINE (Unix shell) quoting.

Allow Embedded Newlines Flag - boolean
   Specifies if newlines are allowed within data.  If your code does not handle
   embedded newlines it is best to have this turned off - you will then get an
   error if a newline is found in the data.

Normalise Embedded Newlines - enumerator
   If newlines are embedded within the field data they can be normalised to this
   value.  This may make it easier if further parsing or loading of the data is
   required.

   If set to NULL then newlines are passed through as they are.  Or use G_DOS
   (or G_WINDOWS), G_UNIX, G_MAC_OS or G_RISC_OS to translate to the line
   endings appropriate for that operating system.

Trim Spaces - enumerator
   Specifies if spaces at the start and/or end of the field will be trimmed. Any
   space that has been quoted or escaped will not be trimmed.  Options are G_NO,
   G_YES, G_LEFT or G_RIGHT.

Pad Columns - integer
   If a line contains fewer fields than this it will padded to this number.

Limit Columns - integer
   If a line contains more than the limit then the excess fields will not be
   returned to the caller (they still have to be parsed though).

Max Field Size - integer
   If a field is longer than this (in characters) an error will be generated.
   It does not apply to fields that exceed the limit columns setting.

   This is useful if there are external restrictions on the field length (e.g.
   if reading in SQL with it's 4000 character limit) as it should generate a
   better error message than would otherwise be shown.

   Note! This limit applies across all fields.  There is no way to specify a
   separate limit for different columns.


** RESTRICTIONS / NOTES **

YOU CAN ONLY READ ONE DATA SOURCE AT A TIME!!!

An individual field cannot exceed 32767 bytes (including spaces that may later
be trimmed).  In multi-byte or varying-byte character sets, 32767 bytes may be
less than 32767 characters.

Fields should not contain binary data.  Theoretically, you may be able to get
away with this if you have an 8-bit character set but it'd be really dodgy.

We do not handle UTF byte order marks (BOM) - this makes sense for CLOBs but it
is a bit of a miss when reading files.

There is no support for national character set datatypes (NCLOB and NVARCHAR2).

Unless you can guarantee this is the first use of CSV_READER in your current
session you should always call SET_OPTIONS first to ensure all options are set
to a known point.  Using G_CSV will reset options to their default values.

When reading a file (not a file pointer) the file is first read into a CLOB and
processed from there.  This is simply the easiest solution we came up to work
around limitations with UTL_FILE.

When reading a file, the directory must be the name or path of an accessible
Oracle directory or, for older DB versions, a path specified in the utl_file_dir
database parameter.

If reading from a file pointer, there are a number of limitations
   + the caller is responsible for opening and closing the file pointer
   + the file must have been opened with a MAX_LINESIZE that is larger than any
     possible record length (the default MAX_LINESIZE varies by operating system
     but I believe it can be as low as 1024)
   + the file must not be opened in binary mode
   + the line endings option is always forced to G_ANY
   + embedded newlines (if allowed) are always normalised to Unix newlines
   + if the file is larger than 32k it must use either DOS or Unix line endings
   + the file must be in the DB character set (there is no support for reading a
     file in the national character set - in fact, even UTL_FILE.FOPEN_NCHAR
	 doesn't support this - it requires the file to be in UTF-8)

When using the pipelined functions, only the first 99 fields on a line will be
returned (each as a separate column).

Theoretically, you can change options mid-way through a file but it is up to you
to ensure you do so at a point and in a manner that doesn't break anything.

Performance is reasonable but not great.


** EXAMPLES **

To read a simple CSV with no quotes or escapes, any line endings, skipping the first line
   declare
      l_fields  csv_reader.table_of_fields;
   begin
      csv_reader.set_options (CSV_READER.G_CSV_SIMPLE);
      csv_reader.set_skip_lines (1);
      csv_reader.open ('DIR', 'FILE');
      while csv_reader.read_line (l_fields) loop
         -- ... do something ...
      end loop;
      csv_reader.close;
   end;

To read a simple tab separated file that is encoded in WE8ISO8859P1
   declare
      l_fields  csv_reader.table_of_fields;
   begin
      csv_reader.set_options (CSV_READER.G_TSV_SIMPLE);
      csv_reader.open ('DIR', 'FILE', 'WE8ISO8859P1');
      while csv_reader.read_line (l_fields) loop
         -- ... do something ...
      end loop;
      csv_reader.close;
   end;

To access from SQL a pipe separated file that uses '\' as an escape (but no quotes)
   exec csv_reader.set_options (CSV_READER.G_CSV);          -- always best to reset to a "known" state
   exec csv_reader.set_separator ('|');
   exec csv_reader.set_escape ('\');

   SELECT *
     FROM table (csv_reader.read_lines ('IMPORT', 'test.csv'))

You can also open the reader in PL/SQL and drop back to SQL to parse the file.
So the previous example can also be done as.
   exec csv_reader.set_options (CSV_READER.G_CSV);          -- always best to reset to a "known" state
   exec csv_reader.set_separator ('|');
   exec csv_reader.set_escape ('\');
   exec csv_reader.open ('IMPORT', 'test.csv');

   SELECT *
     FROM table (csv_reader.read_lines)

To access, from SQL, a standard CSV file held in a CLOB in a table, allowing embedded newlines using any line endings but normalising them to LF characters
   exec csv_reader.set_options (CSV_READER.G_CSV);
   exec csv_reader.set_allow_embedded_newlines (TRUE);
   exec csv_reader.set_normalise_embedded_newline (CSV_READER.G_UNIX);

   SELECT csv.*
     FROM table_with_a_clob tab
        , table (csv_reader.read_lines (tab.clob_column))  csv

==============================================================================*/

-- what sort of line endings should be looked for in the data stream
G_DOS             CONSTANT NUMBER := 1;         -- DOS uses CR / LF line endings
G_WINDOWS         CONSTANT NUMBER := 1;         -- a synonym for DOS (for those of us who haven't been around long enough to know what DOS is)
G_UNIX            CONSTANT NUMBER := 2;         -- Unix uses LF line endings (includes Linux and iOS)
G_MAC_OS          CONSTANT NUMBER := 3;         -- Mac OS (up to version 9) used CR line endings
G_RISC_OS         CONSTANT NUMBER := 4;         -- good old RiscOS used LF / CR line endings
G_ANY             CONSTANT NUMBER := 5;         -- allows any line endings (recommended)

-- quote mode controls how quote characters are interpreted
-- if inside quotes, to embed a quote as data you must double it or, if allowed by your options, escape it
G_STRICT          CONSTANT NUMBER := 10;        -- quotes must only appear at start/end of a field (or be doubled if part of data)
G_LENIENT         CONSTANT NUMBER := 11;        -- if a quote did not start a field any other quotes are taken as data
G_INLINE          CONSTANT NUMBER := 12;        -- similar to Unix (e.g. "abc""def" would be abcdef)

-- quoted escape mode controls how escape characters within quotes are interpreted
G_NOT_SPECIAL     CONSTANT NUMBER := 20;        -- have no special meaning
G_ESCAPE_ALL      CONSTANT NUMBER := 21;        -- escape is always special (escape not copied, next char is always copied)
G_ESCAPE_SPECIAL  CONSTANT NUMBER := 22;        -- escape is only special if followed by a special char (newlines are not special)
G_ESCAPE_QUOTE    CONSTANT NUMBER := 23;        -- escape is only special if followed by a quote

-- trim spaces controls whether spaces are trimmed from data (can also be G_YES or G_NO)
-- note! quoted and escaped spaces are never trimmed
G_NO              CONSTANT NUMBER := 30;
G_YES             CONSTANT NUMBER := 31;
G_LEFT            CONSTANT NUMBER := 32;
G_RIGHT           CONSTANT NUMBER := 33;

-- option sets allow you to control options en-masse with some pre-defined sets to handle standard CSV formats
TYPE options_rec IS RECORD
   ( separator                   VARCHAR2(1 CHAR)  -- the character between each field (MANDATORY)
   , quote                       VARCHAR2(1 CHAR)  -- a single character
   , escape                      VARCHAR2(1 CHAR)  -- removes special meaning from next character
   , line_endings                INTEGER           -- G_DOS, G_WINDOWS, G_UNIX, G_MAC_OS, G_RISC_OS, G_ANY
   , skip_lines                  INTEGER           -- number of lines to skip at the start of the file
   , skip_empty_lines            BOOLEAN           -- skip lines that are blank (if trimming spaces skips all space lines)
   , skip_all_null_lines         BOOLEAN           -- skip lines where all (returned) fields are empty
   , quote_mode                  INTEGER           -- G_STRICT, G_LENIENT, G_INLINE
   , quoted_escape_mode          INTEGER           -- G_NOT_SPECIAL, G_ESCAPE_ALL, G_ESCAPE_SPECIAL, G_ESCAPE_QUOTE
   , allow_embedded_newlines     BOOLEAN           -- are newlines allowed within data
   , normalise_embedded_newlines INTEGER           -- G_DOS, G_WINDOWS, G_UNIX, G_MAC_OS, G_RISC_OS or NULL to not normalise
   , trim_spaces                 INTEGER           -- G_NO, G_YES, G_LEFT, G_RIGHT
   , pad_columns                 INTEGER           -- always return at least this many fields
   , limit_columns               INTEGER           -- return no more than this many fields (throws away remaining fields)
   , max_field_size              INTEGER );        -- generate an error if any field exceeds this many characters

-- pre-defined options suitable for different types of files - if needed, you can later override any setting
-- PLEASE TREAT THESE AS IF THEY WERE CONSTANTS
G_CSV             options_rec;                     -- comma separated with strict double quotes, any line endings, no escapes, no embedded newlines, empty/null lines skipped
G_CSV_SIMPLE      options_rec;                     -- as for G_CSV but no quoting or escaping (hence commas, newlines not allowed in data)
G_CSV_STRICT      options_rec;                     -- as for G_CSV but DOS line ending and embedded newlines allowed (RFC 4180)
G_TSV             options_rec;                     -- tab separated using strict double quotes, any line endings, embedded newlines not allowed
G_TSV_SIMPLE      options_rec;                     -- tab separated with no quoting or escaping (tabs, newlines not allowed in data)

SUBTYPE field_type IS VARCHAR2(32767 BYTE);

TYPE line_rec IS RECORD
   ( c01    field_type
   , c02    field_type
   , c03    field_type
   , c04    field_type
   , c05    field_type
   , c06    field_type
   , c07    field_type
   , c08    field_type
   , c09    field_type
   , c10    field_type
   , c11    field_type
   , c12    field_type
   , c13    field_type
   , c14    field_type
   , c15    field_type
   , c16    field_type
   , c17    field_type
   , c18    field_type
   , c19    field_type
   , c20    field_type
   , c21    field_type
   , c22    field_type
   , c23    field_type
   , c24    field_type
   , c25    field_type
   , c26    field_type
   , c27    field_type
   , c28    field_type
   , c29    field_type
   , c30    field_type
   , c31    field_type
   , c32    field_type
   , c33    field_type
   , c34    field_type
   , c35    field_type
   , c36    field_type
   , c37    field_type
   , c38    field_type
   , c39    field_type
   , c40    field_type
   , c41    field_type
   , c42    field_type
   , c43    field_type
   , c44    field_type
   , c45    field_type
   , c46    field_type
   , c47    field_type
   , c48    field_type
   , c49    field_type
   , c50    field_type
   , c51    field_type
   , c52    field_type
   , c53    field_type
   , c54    field_type
   , c55    field_type
   , c56    field_type
   , c57    field_type
   , c58    field_type
   , c59    field_type
   , c60    field_type
   , c61    field_type
   , c62    field_type
   , c63    field_type
   , c64    field_type
   , c65    field_type
   , c66    field_type
   , c67    field_type
   , c68    field_type
   , c69    field_type
   , c70    field_type
   , c71    field_type
   , c72    field_type
   , c73    field_type
   , c74    field_type
   , c75    field_type
   , c76    field_type
   , c77    field_type
   , c78    field_type
   , c79    field_type
   , c80    field_type
   , c81    field_type
   , c82    field_type
   , c83    field_type
   , c84    field_type
   , c85    field_type
   , c86    field_type
   , c87    field_type
   , c88    field_type
   , c89    field_type
   , c90    field_type
   , c91    field_type
   , c92    field_type
   , c93    field_type
   , c94    field_type
   , c95    field_type
   , c96    field_type
   , c97    field_type
   , c98    field_type
   , c99    field_type );

TYPE table_of_fields IS TABLE OF field_type;
TYPE table_of_lines  IS TABLE OF line_rec;

-- when using the individual set functions you may have to be careful about ordering (to avoid conflicts)
PROCEDURE set_options                    (p_options                     IN options_rec);
PROCEDURE set_separator                  (p_separator                   IN VARCHAR2);
PROCEDURE set_quote                      (p_quote                       IN VARCHAR2);
PROCEDURE set_escape                     (p_escape                      IN VARCHAR2);
PROCEDURE set_line_endings               (p_line_endings                IN INTEGER);
PROCEDURE set_skip_lines                 (p_skip_lines                  IN INTEGER);
PROCEDURE set_skip_empty_lines           (p_skip_empty_lines            IN BOOLEAN);
PROCEDURE set_skip_all_null_lines        (p_skip_all_null_lines         IN BOOLEAN);
PROCEDURE set_quote_mode                 (p_quote_mode                  IN INTEGER);
PROCEDURE set_quoted_escape_mode         (p_quoted_escape_mode          IN INTEGER);
PROCEDURE set_allow_embedded_newlines    (p_allow_embedded_newlines     IN BOOLEAN);
PROCEDURE set_normalise_embedded_newline (p_normalise_embedded_newlines IN INTEGER);
PROCEDURE set_trim_spaces                (p_trim_spaces                 IN INTEGER);
PROCEDURE set_pad_columns                (p_pad_columns                 IN INTEGER);
PROCEDURE set_limit_columns              (p_limit_columns               IN INTEGER);
PROCEDURE set_max_field_size             (p_max_field_size              IN INTEGER);

FUNCTION get_options
RETURN options_rec;

FUNCTION get_line_number   RETURN NUMBER;          -- the physical line number
FUNCTION get_record_number RETURN NUMBER;          -- the logical line number (may differ to physical if a field has embedded newlines)

-- open a CLOB for reading
PROCEDURE open (p_clob IN CLOB);

-- open a file pointer for reading
--
-- the caller must open and close the file.  the file must be opened in text mode with MAX_LINESIZE greater
-- than any possible record length.  line ending options are not properly supported - effectively we always
-- use G_ANY line endings with embedded newlines, if allowed, normalised to G_UNIX.  files larger than 32k
-- DOS or Unix line endings (but we doubt there will be much call for Mac OS or RISC OS line endings).
PROCEDURE open (p_file_ptr IN utl_file.file_type);

-- open an external file for reading
--
-- directory and filename use the same rules as UTL_FILE (or BFILENAME).  character set needs to be an
-- Oracle character set name.  to map a CSID to its name use NLS_CHARSET_NAME.  to map an IANA character
-- set name use UTL_I18N.MAP_CHARSET ('iana name', flag -> UTL_I18N.IANA_TO_ORACLE)
PROCEDURE open (p_directory     IN VARCHAR2,                      -- must be an Oracle directory name (in OLD DB versions, can be a UTL_FILE_DIR path)
                p_filename      IN VARCHAR2,
                p_character_set IN VARCHAR2 := NULL);             -- defaults to the database character set

-- read the next record from the currently open store and parse it into a table of fields
FUNCTION read_line (x_fields OUT NOCOPY table_of_fields)
RETURN BOOLEAN;

-- close the current data store and free up any associated memory
-- note: if reading from a file pointer, the caller must close the file pointer themselves (as they opened it)
PROCEDURE close;

-- convenience functions to make it slightly easier to access a CSV from within SQL (max. 99 columns)
FUNCTION read_lines (p_clob IN CLOB)
RETURN table_of_lines
PIPELINED;

FUNCTION read_lines (p_directory      IN VARCHAR2,                -- must be an Oracle directory name (or path)
                     p_filename       IN VARCHAR2,
                     p_character_set  IN VARCHAR2 := NULL)        -- defaults to the database character set
RETURN table_of_lines
PIPELINED;

FUNCTION read_lines
RETURN table_of_lines
PIPELINED;

-- a standalone utility function added to this package as it is complementary
FUNCTION tokenise (p_line IN CLOB, p_separator IN VARCHAR2 := ',')
RETURN table_of_fields;

END csv_reader;
/
