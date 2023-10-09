CREATE OR REPLACE PACKAGE BODY csv_reader AS

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

See the spec for usage details.

NOTE: YOU CAN ONLY READ FROM ONE DATA SOURCE AT A TIME!!!

There are two different parsers available.  A "complex" version that handles all
options and works on a character-by-character basis.  Then there is a "simple"
version that does not allow quoting or escaping and works in chunks.  Tests
indicate the simple version is 2-4 times faster (not as fast as I expected).

Fields are read into VARCHAR2 fields so have a maximum length of 32767 bytes.
We could switch to using CLOBs but there are quite a few drawbacks and, so far,
we have no requirement to handle fields that long.

Reading from files first loads the file to a CLOB and processes from there. This
is to work around limitations with UTL_FILE - mostly around character sets.  We
can't do this when reading a file pointer so we apply quite a few restrictions
about the file pointers (see spec for more details).

Performance is not great but better than expected.  It would benefit hugely if
PL/SQL allowed more efficient character-by-character scanning of strings - along
the lines of the character pointer/array/stream abilities of C or Java or etc.

Ver   Date     Author      Description
----- -------- ----------- -----------------------------------------------------
1.0   22/08/17 C Marshall  Initial Version
==============================================================================*/

-- supported data sources
G_DS_CLOB            CONSTANT NUMBER := 1;
G_DS_FILE_PTR        CONSTANT NUMBER := 2;

-- special characters supported by the simple parser
G_SP_SEPARATOR       CONSTANT NUMBER := 1;
G_SP_END_OF_BUFFER   CONSTANT NUMBER := 2;
G_SP_END_OF_LINE     CONSTANT NUMBER := 3;
G_SP_END_OF_FILE     CONSTANT NUMBER := 4;

FUNCTION local_chr (p_ascii_chr IN NUMBER)
RETURN VARCHAR2;

-- a few constants representing special characters (in the database character set)
G_TAB          CONSTANT VARCHAR2(1 CHAR) := local_chr (9);
G_LF           CONSTANT VARCHAR2(1 CHAR) := local_chr (10);
G_CR           CONSTANT VARCHAR2(1 CHAR) := local_chr (13);

g_data_source  NUMBER;
g_clob         CLOB;
g_clob_pos     NUMBER;                 -- if reading a CLOB, the number of chars already read
g_file_ptr     utl_file.file_type;
g_options      options_rec;

g_buffer       VARCHAR2(32767 BYTE);
g_buf_size     NUMBER;
g_buf_len      NUMBER;
g_buf_pos      NUMBER;

-- globals used by the simple parser
g_simple_f     BOOLEAN;                -- TRUE if we can use a simple parsing scheme (no quotes or escapes)
g_nl_string    VARCHAR2(2 CHAR);       -- if using a specific line ending, the characters that represent a newline
g_sep_pos      NUMBER := -1;           -- the pos fields keep track of the next occurrence of that character in the buffer
g_cr_pos       NUMBER := -1;
g_lf_pos       NUMBER := -1;
g_nl_pos       NUMBER := -1;

-- globals used by the complex parser
g_peek_f       BOOLEAN;
g_peek_char    VARCHAR2(2 CHAR);
g_nl_type      INTEGER;                -- if we read a newline what were the exact characters (G_DOS, G_UNIX, G_MAC_OS, G_RISC_OS)

-- mostly these are used for error reporting
g_record_num   NUMBER;                 -- the current logical line number
g_field_num    NUMBER;                 -- the current field
g_line_num     NUMBER;                 -- the current physical line number
g_char_num     NUMBER;                 -- the current character in the physical line
g_field_line   NUMBER;                 -- the line the current field started on
g_field_char   NUMBER;                 -- the character the current field started on

e_numeric_or_value_error   EXCEPTION;
PRAGMA exception_init (e_numeric_or_value_error, -6502);

e_subscript_beyond_count   EXCEPTION;
PRAGMA exception_init (e_subscript_beyond_count, -6533);


--------------------------------------------------------------------------------
--
-- LOCAL_CHR:  Converts an ASCII character to the database character set.
--
-- Similar to FND_GLOBAL.LOCAL_CHR but uses supported (in 11g) methods.  Of
-- course under any character set we use this will be simply CHR(x).
--

FUNCTION local_chr (p_ascii_chr IN NUMBER)
RETURN VARCHAR2 IS
BEGIN
   RETURN (utl_i18n.raw_to_char (hextoraw (to_char (p_ascii_chr, 'fmXX')), 'US7ASCII'));
END local_chr;


--------------------------------------------------------------------------------
--
-- RAISE_DATA_ERROR:  Raises an error due to a problem in the data stream.
--
-- P_ERROR_NUM must be in range -20001 to -20999 (preferably should be -208xx).
--

PROCEDURE raise_data_error (p_error_num IN NUMBER, p_error_message IN VARCHAR2) IS
BEGIN
   IF g_simple_f THEN
      -- the simple parser only tracks record and field numbers (but the record number will be the line number)
      raise_application_error (p_error_num, 'CSV Reader: Line ' || g_record_num || ', field ' || g_field_num || ': ' || p_error_message);
   ELSE
      raise_application_error (p_error_num, 'CSV Reader: ' ||
                                            'Record ' || g_record_num || ', field ' || g_field_num ||
                                            CASE WHEN g_record_num = g_field_line
                                               THEN ' (starting at char ' || g_field_char || '): '
                                               ELSE ' (starting at line ' || g_field_line || ', char ' || g_field_char || '): '
                                            END ||
                                            p_error_message);
   END IF;
END raise_data_error;


--------------------------------------------------------------------------------
--
-- CHECK_OPTIONS:  Check the current options are valid.
--

PROCEDURE check_options IS

   l_nl_special_1  VARCHAR2(1 CHAR);
   l_nl_special_2  VARCHAR2(1 CHAR);

BEGIN
   -- apply defaults for mandatory fields (equivalent to G_CSV settings)
   g_options.separator                 := nvl (g_options.separator, ',');
   g_options.line_endings              := nvl (g_options.line_endings, G_ANY);
   g_options.skip_empty_lines          := nvl (g_options.skip_empty_lines, TRUE);
   g_options.skip_all_null_lines       := nvl (g_options.skip_all_null_lines, FALSE);
   g_options.quote_mode                := nvl (g_options.quote_mode, G_STRICT);
   g_options.quoted_escape_mode        := nvl (g_options.quoted_escape_mode, G_NOT_SPECIAL);
   g_options.allow_embedded_newlines   := nvl (g_options.allow_embedded_newlines, FALSE);
   g_options.trim_spaces               := nvl (g_options.trim_spaces, G_NO);

   -- if reading from a file pointer, some settings are required
   IF g_data_source = G_DS_FILE_PTR THEN
      g_options.line_endings                := G_ANY;
      g_options.normalise_embedded_newlines := G_UNIX;
   END IF;

   -- check for conflicts between special characters
   IF g_options.line_endings = G_MAC_OS THEN
      l_nl_special_1 := G_CR;
   ELSIF g_options.line_endings = G_UNIX THEN
      l_nl_special_1 := G_LF;
   ELSIF g_options.line_endings IN (G_DOS, G_RISC_OS, G_ANY) THEN
      l_nl_special_1 := G_CR;
      l_nl_special_2 := G_LF;
   ELSE
      raise_application_error (-20800, 'CSV Reader: Invalid option - line endings must be G_DOS, G_WINDOWS, G_UNIX, G_MAC_OS, G_RISC_OS or G_ANY');
   END IF;

   IF g_options.separator IN (g_options.quote, g_options.escape, l_nl_special_1, l_nl_special_2) THEN
      raise_application_error (-20800, 'CSV Reader: Invalid option - conflict between separator and quote, escape or newline characters');
   END IF;

   IF g_options.escape IS NOT NULL  AND  g_options.escape IN (g_options.quote, l_nl_special_1, l_nl_special_2) THEN
      raise_application_error (-20800, 'CSV Reader: Invalid option - conflict between escape and quote or newline characters');
   END IF;

   IF g_options.quote IS NOT NULL  AND  g_options.quote IN (l_nl_special_1, l_nl_special_2) THEN
      raise_application_error (-20800, 'CSV Reader: Invalid option - conflict between quote and newline characters');
   END IF;

   IF g_options.quote_mode NOT IN (G_STRICT, G_LENIENT, G_INLINE) THEN
      raise_application_error (-20800, 'CSV Reader: Invalid option - quote mode must be G_STRICT, G_LENIENT or G_INLINE');
   END IF;

   IF g_options.quoted_escape_mode NOT IN (G_NOT_SPECIAL, G_ESCAPE_ALL, G_ESCAPE_SPECIAL, G_ESCAPE_QUOTE) THEN
      raise_application_error (-20800, 'CSV Reader: Invalid option - quoted escape mode must be G_NOT_SPECIAL, G_ESCAPE_ALL, G_ESCAPE_SPECIAL or G_ESCAPE_QUOTE');
   END IF;

   IF g_options.normalise_embedded_newlines IS NOT NULL THEN
      IF g_options.normalise_embedded_newlines NOT IN (G_DOS, G_WINDOWS, G_UNIX, G_MAC_OS, G_RISC_OS) THEN
         raise_application_error (-20800, 'CSV Reader: Invalid option - normalise embedded newlines must be NULL or one of G_DOS, G_WINDOWS, G_UNIX, G_MAC_OS, G_RISC_OS');
      END IF;
   END IF;

   IF g_options.trim_spaces NOT IN (G_NO, G_YES, G_LEFT, G_RIGHT) THEN
      raise_application_error (-20800, 'CSV Reader: Invalid option - trim spaces must be G_NO, G_YES, G_LEFT or G_RIGHT');
   END IF;

   IF g_options.pad_columns <= 1 THEN
      raise_application_error (-20800, 'CSV Reader: Invalid option - pad columns must be an integer greater than 1');
   END IF;

   -- note! it is valid, but quite strange, for limit columns < pad columns (the fields from limit+1 to pad will just be NULL)
   IF g_options.limit_columns <= 1 THEN
      raise_application_error (-20800, 'CSV Reader: Invalid option - limit columns must be an integer greater than 1');
   END IF;

   -- you can specify a field size larger than 32k but it won't help (this is in case CLOB support is added in the future)
   IF g_options.max_field_size <= 0 THEN
      raise_application_error (-20800, 'CSV Reader: Invalid option - max field size must be a positive integer');
   END IF;

   -- and work out if we can use a quick and dirty approach to parsing or need something a bit more comples
   g_simple_f  := ( g_options.quote IS NULL  AND  g_options.escape IS NULL );
   g_nl_string := CASE g_options.line_endings
                     WHEN G_DOS     THEN G_CR || G_LF
                     WHEN G_UNIX    THEN G_LF
                     WHEN G_MAC_OS  THEN G_CR
                     WHEN G_RISC_OS THEN G_LF || G_CR
                     ELSE NULL
                  END;

EXCEPTION
   WHEN OTHERS THEN
      -- if there is a problem with the options you aren't allowed to open the data stream
      g_data_source := NULL;
      RAISE;
END check_options;


--------------------------------------------------------------------------------
--
-- SET/GET_OPTIONS:  Sets or gets options that control the parsing of the CSV data.
--

PROCEDURE set_options (p_options IN options_rec) IS
BEGIN
   g_options := p_options;
   check_options;
END set_options;

----------------

PROCEDURE set_separator (p_separator IN VARCHAR2) IS
BEGIN
   g_options.separator := p_separator;
   check_options;
END set_separator;

----------------

PROCEDURE set_quote (p_quote IN VARCHAR2) IS
BEGIN
   g_options.quote := p_quote;
   check_options;
END set_quote;

----------------

PROCEDURE set_escape (p_escape IN VARCHAR2) IS
BEGIN
   g_options.escape := p_escape;
   check_options;
END set_escape;

----------------

PROCEDURE set_line_endings (p_line_endings IN INTEGER) IS
BEGIN
   g_options.line_endings := p_line_endings;
   check_options;
END set_line_endings;

----------------

PROCEDURE set_skip_lines (p_skip_lines IN INTEGER) IS
BEGIN
   g_options.skip_lines := p_skip_lines;
   check_options;
END set_skip_lines;

----------------

PROCEDURE set_skip_empty_lines (p_skip_empty_lines IN BOOLEAN) IS
BEGIN
   g_options.skip_empty_lines := p_skip_empty_lines;
   check_options;
END set_skip_empty_lines;

----------------

PROCEDURE set_skip_all_null_lines (p_skip_all_null_lines IN BOOLEAN) IS
BEGIN
   g_options.skip_all_null_lines := p_skip_all_null_lines;
   check_options;
END set_skip_all_null_lines;

----------------

PROCEDURE set_quote_mode (p_quote_mode IN INTEGER) IS
BEGIN
   g_options.quote_mode := p_quote_mode;
   check_options;
END set_quote_mode;

----------------

PROCEDURE set_quoted_escape_mode (p_quoted_escape_mode IN INTEGER) IS
BEGIN
   g_options.quoted_escape_mode := p_quoted_escape_mode;
   check_options;
END set_quoted_escape_mode;

----------------

PROCEDURE set_allow_embedded_newlines (p_allow_embedded_newlines IN BOOLEAN) IS
BEGIN
   g_options.allow_embedded_newlines := p_allow_embedded_newlines;
   check_options;
END set_allow_embedded_newlines;

----------------

PROCEDURE set_normalise_embedded_newline (p_normalise_embedded_newlines IN INTEGER) IS
BEGIN
   g_options.normalise_embedded_newlines := p_normalise_embedded_newlines;
   check_options;
END set_normalise_embedded_newline;

----------------

PROCEDURE set_trim_spaces (p_trim_spaces IN INTEGER) IS
BEGIN
   g_options.trim_spaces := p_trim_spaces;
   check_options;
END set_trim_spaces;

----------------

PROCEDURE set_pad_columns (p_pad_columns IN INTEGER) IS
BEGIN
   g_options.pad_columns := p_pad_columns;
   check_options;
END set_pad_columns;

----------------

PROCEDURE set_limit_columns (p_limit_columns IN INTEGER) IS
BEGIN
   g_options.limit_columns := p_limit_columns;
   check_options;
END set_limit_columns;

----------------

PROCEDURE set_max_field_size (p_max_field_size IN INTEGER) IS
BEGIN
   g_options.max_field_size := p_max_field_size;
   check_options;
END set_max_field_size;

----------------

FUNCTION get_options
RETURN options_rec IS
BEGIN
   RETURN (g_options);
END get_options;


--------------------------------------------------------------------------------
--
-- GET_LINE/RECORD_NUMBER:  Returns the current physical/logical line number.
--
-- We only ever read full lines so anytime this can be called we will already
-- be on the next line so we have to wind things back.
--
-- The line number and record number will be the same unless the data contains
-- embedded newlines.  In this case, the line number reported is the last
-- physical line for that record.
--

FUNCTION get_line_number
RETURN NUMBER IS
BEGIN
   IF g_data_source IS NULL THEN
      RETURN (NULL);
   ELSE
      RETURN (g_line_num - 1);
   END IF;
END get_line_number;

----------------

FUNCTION get_record_number
RETURN NUMBER IS
BEGIN
   IF g_data_source IS NULL THEN
      RETURN (NULL);
   ELSE
      RETURN (g_record_num - 1);
   END IF;
END get_record_number;


--------------------------------------------------------------------------------
--
-- OPEN:  Sets up the CSV reader to read from a particular data source.
--

PROCEDURE init (p_data_source IN INTEGER, p_clob IN CLOB, p_file_ptr IN utl_file.file_type) IS
BEGIN
   g_data_source := p_data_source;
   g_clob        := p_clob;
   g_clob_pos    := 0;
   g_file_ptr    := p_file_ptr;

   -- some options are dependent on the data source so we must check them even if we didn't change them
   check_options;

   g_buffer  := NULL;
   g_buf_len := 0;
   g_buf_pos := 1;
   g_peek_f  := FALSE;

   -- the size of the buffer used to read from a CLOB (file pointers have to be done line-by-line).
   -- we have seen performance issues with large buffer sizes but it is only on certain files and
   -- we can't identify the cause.  however, it isn't much of a hit to keep the buffer small-ish.
   g_buf_size := 512;

   -- keep track of where we are so we can report it out on error messages
   g_record_num := 1;
   g_field_num  := 1;
   g_line_num   := 1;
   g_char_num   := 1;
   g_field_line := 1;
   g_field_char := 1;

   -- used by the simple parser to keep track of special characters in the current buffer
   g_sep_pos := -1;
   g_cr_pos  := -1;
   g_lf_pos  := -1;
   g_nl_pos  := -1;
END init;

----------------

PROCEDURE open (p_clob IN CLOB) IS
BEGIN
   init (G_DS_CLOB, p_clob, NULL);
END open;

----------------

PROCEDURE open (p_file_ptr IN utl_file.file_type) IS
BEGIN
	-- when reading a file using UTL_FILE the file must
	--   + use the DB character set encoding
	--   + be opened with a MAX_LINESIZE larger than any possible record length
	--   + be opened in character (not binary) mode
	--   + use either DOS or Unix line endings
	--
	-- we don't get the actual terminator back when reading a line so we are
	-- forced to act as if the line endings option was G_ANY.  it also means
	-- embedded newlines (if allowed) are always normalised to Unix newlines.
	--
	-- doing better would mean RAW reads and on-the-fly character set conversions
	-- which is not realistic without in-built PL/SQL support.

   init (G_DS_FILE_PTR, NULL, p_file_ptr);
END open;

----------------

PROCEDURE open (p_directory     IN VARCHAR2,
                p_filename      IN VARCHAR2,
                p_character_set IN VARCHAR2 := NULL) IS

   l_bfile        BFILE;
   l_bfile_csid   NUMBER;
   l_clob         CLOB;
   l_dest_offset  INTEGER;
   l_src_offset   INTEGER;
   l_lang_context INTEGER;
   l_warning      INTEGER;

BEGIN
   -- we don't use UTL_FILE to read files as it only supports files that are in
   -- the DB character set (or, via FOPEN_NCHAR, UTF-8) plus it limits the max
   -- line length to 32k bytes.  instead, we read the first read the file into
   -- a CLOB and process that.
   --
   -- tests indicate this method is at least as performant as using UTL_FILE
   -- although there may be memory impacts.  it's a real shame that DBMS_LOB
   -- doesn't provide decent stream reading support...

   l_bfile := bfilename (p_directory, p_filename);
   dbms_lob.open (l_bfile, DBMS_LOB.LOB_READONLY);

   IF p_character_set IS NULL THEN
      l_bfile_csid := DBMS_LOB.DEFAULT_CSID;
   ELSE
      l_bfile_csid := nls_charset_id (p_character_set);
      IF l_bfile_csid IS NULL THEN
         raise_application_error (-20800, 'CSV Reader: Character Set must be a valid Oracle character set name or left NULL (defaults to DB character set)');
      END IF;
   END IF;

   dbms_lob.createtemporary (l_clob, TRUE);

   l_dest_offset  := 1;
   l_src_offset   := 1;
   l_lang_context := DBMS_LOB.DEFAULT_LANG_CTX;

   dbms_lob.loadclobfromfile (dest_lob     => l_clob,
                              src_bfile    => l_bfile,
                              amount       => DBMS_LOB.LOBMAXSIZE,
                              dest_offset  => l_dest_offset,
                              src_offset   => l_src_offset,
                              bfile_csid   => l_bfile_csid,
                              lang_context => l_lang_context,
                              warning      => l_warning);

   dbms_lob.close (l_bfile);

   init (G_DS_CLOB, l_clob, NULL);
END open;


--------------------------------------------------------------------------------
--
-- GET_NEXT_BUFFER:  Returns the next chunk of data from the data stream.
--
-- If no more data is available (end-of-file) the buffer will be NULL and
-- G_BUF_LEN will be set to 0.  This is the only time this is possible.
--

PROCEDURE get_next_buffer IS
BEGIN
   IF g_data_source = G_DS_CLOB THEN
      -- DBMS_LOB.SUBSTR should be faster than SUBSTR as it returns VARCHAR2 not
      -- a CLOB but we have seen odd things in some DB versions so you could try
      -- SUBSTR instead.
      --
      -- chunk size affects performance quite a bit but the best size seems to
      -- vary dependent on the data being processed and the DB version.  so you
      -- might want to fiddle with chunk size but be aware that bigger does not
      -- necessarily mean better.
      --
      -- we have seen some strange overhead when invoking procedures that refer
      -- to CLOBs.  it's really odd and doesn't always happen but the best and
      -- most consistent solution we have is to ensure any CLOB access is done
      -- from a procedure that is called infrequently.  hence we only access
      -- the source CLOB here and we also turn off inlining of this procedure.

      g_buffer   := dbms_lob.substr (g_clob, g_buf_size, g_clob_pos + 1);
--      g_buffer   := substr (g_clob, g_clob_pos + 1, g_buf_size);
      g_buf_len  := nvl (length (g_buffer), 0);
      g_clob_pos := g_clob_pos + g_buf_len;
      g_buf_pos  := 1;

   ELSE
      -- read from the UTL_FILE file pointer
      --
      -- UTL_FILE only provides GET_LINE and GET_RAW to read from files.  the first
      -- isn't nice as it only reads a line at a time and we can't control how it
      -- handles newlines.  but the second is probably worse as, due to character
      -- set conversion, you would have to read the entire file into a BLOB first
      -- or assume a single-byte database character set.
      --
      -- GET_LINE will strip the CR LF (DOS) or LF (Unix) so we have to add it back.
      -- we don't know what it was so we will just add back a LF.  this is OK as we
      -- already force line endings to use G_ANY and normalise to G_UNIX.

      utl_file.get_line (g_file_ptr, g_buffer, 32767);
      g_buffer  := g_buffer || G_LF;
      g_buf_len := length (g_buffer);
      g_buf_pos := 1;
   END IF;

EXCEPTION
   WHEN utl_file.read_error THEN
      -- should only be possible if the line exceeded 32767 bytes or was opened with a smallish linesize
      raise_data_error (-20812, 'Line too long - either exceeds MAX_LINESIZE given when opening the file or ' ||
                                'exceeds the maximum data length for a VARCHAR2 (32767 bytes)');

   WHEN no_data_found THEN
      -- reached end-of-file
      g_buffer  := NULL;
      g_buf_len := 0;
      g_buf_pos := 1;
END get_next_buffer;


--------------------------------------------------------------------------------
--
-- READ_LINE_SIMPLE:  Parses a data line allowing for a small set of options.
--
-- Returns FALSE when the end of the file/data has been reached.
--
-- This is used when the data does not need quoting or escaping (i.e. a field
-- won't contain a separator or newline character).  This also means there is no
-- need for quote mode, quoted escape mode or the embedded newline options.
--

FUNCTION read_line_simple (x_fields IN OUT NOCOPY table_of_fields)
RETURN BOOLEAN IS

   l_field           VARCHAR2(32767 BYTE);
   l_special_type    INTEGER;
   l_chunk_start     NUMBER;
   l_chunk_end       NUMBER;
   l_all_null_f      BOOLEAN;

--------

   PROCEDURE get_next_special (x_type OUT NOCOPY INTEGER, x_chunk_start OUT NOCOPY NUMBER, x_chunk_end OUT NOCOPY NUMBER) IS
   BEGIN
      -- the position returned is the first character of the special but we
      -- will leave the buffer position on the character after the special

      -- first ensure we've got data in the buffer
      IF g_buf_pos > g_buf_len THEN
PRAGMA INLINE (get_next_buffer, 'NO');
         get_next_buffer;

         -- we've gotten a new buffer so we no longer know where the specials are
         g_sep_pos := -1;
         g_cr_pos  := -1;
         g_lf_pos  := -1;
         g_nl_pos  := -1;
      END IF;

      -- set the start of this chunk (must call this after the above in case we have to read another buffer)
      x_chunk_start := g_buf_pos;

      IF g_buf_len = 0 THEN
         -- can only happen when all data has been read
         x_type       := G_SP_END_OF_FILE;
         x_chunk_end  := 1;

      ELSE
         -- be sure we know where the next specials are within the buffer
         IF g_sep_pos != 0   AND  g_sep_pos < g_buf_pos THEN
            g_sep_pos := instr (g_buffer, g_options.separator, g_buf_pos);
         END IF;

         IF g_nl_pos != 0  AND  g_nl_pos < g_buf_pos THEN
            IF g_options.line_endings != G_ANY THEN
               -- piece of piss to search for a newline when we know what it has to be
               g_nl_pos := instr (g_buffer, g_nl_string, g_buf_pos);

            ELSE
               -- do a G_ANY search - so we need to know where the next LF and CR chars are
               IF g_cr_pos != 0  AND  g_cr_pos < g_buf_pos THEN
                  g_cr_pos := instr (g_buffer, G_CR, g_buf_pos);
               END IF;

               IF g_lf_pos != 0  AND  g_lf_pos < g_buf_pos THEN
                  g_lf_pos := instr (g_buffer, G_LF, g_buf_pos);
               END IF;

               -- newline starts at the first of the LF and CR
               IF g_cr_pos = 0 THEN
                  g_nl_pos := g_lf_pos;
               ELSIF g_lf_pos = 0 THEN
                  g_nl_pos := g_cr_pos;
               ELSE
                  g_nl_pos := least (g_lf_pos, g_cr_pos);
               END IF;
            END IF;
         END IF;

         -- and work out which special character is next
         IF g_sep_pos = 0  AND  g_nl_pos = 0 THEN
            x_type      := G_SP_END_OF_BUFFER;
            x_chunk_end := g_buf_len + 1;

         ELSIF g_sep_pos = 0 THEN
            x_type      := G_SP_END_OF_LINE;
            x_chunk_end := g_nl_pos;

         ELSIF g_nl_pos = 0 THEN
            x_type      := G_SP_SEPARATOR;
            x_chunk_end := g_sep_pos;

         ELSIF g_sep_pos < g_nl_pos THEN
            x_type      := G_SP_SEPARATOR;
            x_chunk_end := g_sep_pos;

         ELSE
            x_type      := G_SP_END_OF_LINE;
            x_chunk_end := g_nl_pos;
         END IF;

         -- and move the buffer on past the special character
         IF x_type = G_SP_END_OF_BUFFER THEN
            g_buf_pos := x_chunk_end;

         ELSIF x_type = G_SP_SEPARATOR THEN
            g_buf_pos := x_chunk_end + length (g_options.separator);

         ELSIF g_options.line_endings != G_ANY THEN
            g_buf_pos := x_chunk_end + length (g_nl_string);

         ELSE
            IF g_cr_pos = 0  OR  g_lf_pos = 0 THEN
               g_buf_pos := x_chunk_end + 1;
            ELSIF g_cr_pos = g_lf_pos + 1  OR  g_lf_pos = g_cr_pos + 1 THEN
               g_buf_pos := x_chunk_end + 2;
            ELSE
               g_buf_pos := x_chunk_end + 1;
            END IF;
         END IF;
      END IF;

   END get_next_special;

--------

   PROCEDURE finalise_field IS
   BEGIN
      -- called when the end of a field is reached (i.e. got to a field separator, a newline or end-of-file)
      IF g_field_num > g_options.limit_columns THEN
         NULL;          -- user doesn't want this field returned

      ELSE
         -- no quoting or escaping so trimming is pretty darn simple
         x_fields.extend;
         CASE g_options.trim_spaces
            WHEN G_YES THEN
               x_fields(g_field_num) := trim (l_field);
            WHEN G_LEFT THEN
               x_fields(g_field_num) := ltrim (l_field);
            WHEN G_RIGHT THEN
               x_fields(g_field_num) := rtrim (l_field);
            ELSE     -- G_NO
               x_fields(g_field_num) := l_field;
         END CASE;

         IF g_options.max_field_size IS NOT NULL THEN
            IF length (x_fields(g_field_num)) > g_options.max_field_size THEN
               raise_data_error (-20810, 'Field exceeds the specified maximum field size ' ||
                                         '(length is ' || length (x_fields(g_field_num)) ||
                                         ', maximum allowed ' || g_options.max_field_size || ')');
            END IF;
         END IF;
      END IF;

      g_field_num := g_field_num + 1;
      l_field     := NULL;
   END finalise_field;

--------

   PROCEDURE add_to_field (l_start IN NUMBER, l_length IN NUMBER) IS
   BEGIN
      -- mostly this procedure is just here so that we can wrap the exception handler around
      IF l_length IS NULL THEN      -- null means copy to end of buffer
         l_field := l_field || substr (g_buffer, l_start);
      ELSE
         l_field := l_field || substr (g_buffer, l_start, l_length);
      END IF;

   EXCEPTION
      WHEN e_numeric_or_value_error THEN
         -- assume the only ORA-06502 error will be "character string buffer too small"
         raise_data_error (-20810, 'Field exceeds the maximum data length for a VARCHAR2 (32767 bytes)');
   END add_to_field;

--------

BEGIN

<<START_AGAIN>>
   x_fields    := table_of_fields();         -- if starting again this is equivalent to deleting the existing fields
   g_field_num := 1;
   l_field     := NULL;

   LOOP
      -- the chunk will include the special character (the buffer position is left after the special)
      get_next_special (l_special_type, l_chunk_start, l_chunk_end);

      CASE l_special_type
         WHEN G_SP_SEPARATOR THEN
            add_to_field (l_chunk_start, l_chunk_end - l_chunk_start);
            finalise_field;

         WHEN G_SP_END_OF_BUFFER THEN
            -- no specials left in the current buffer so all the remaining stuff must be data
            add_to_field (l_chunk_start, NULL);

         WHEN G_SP_END_OF_LINE THEN
            -- no way to quote or escape a newline so this must be the end of record
            add_to_field (l_chunk_start, l_chunk_end - l_chunk_start);
            EXIT;

         WHEN G_SP_END_OF_FILE THEN
            -- there shouldn't be anything in the buffer but let's be sure and copy it over anyway
            add_to_field (l_chunk_start, NULL);
            EXIT;
      END CASE;
   END LOOP;

   -- we've either reached end-of-line or end-of-file
   -- note! this will increment the field num
   finalise_field;

   g_record_num := g_record_num + 1;

   IF l_special_type = G_SP_END_OF_FILE  AND  g_field_num = 2  AND  x_fields(1) IS NULL  THEN
      -- reached end-of-file and there was no data on the line so we're done - no more data to return
      x_fields.delete;
      RETURN (FALSE);
   END IF;

   -- check if we are skipping this line - if so we just start reading again
   IF g_options.skip_lines > 0 THEN
      g_options.skip_lines := g_options.skip_lines - 1;
      GOTO start_again;
   END IF;

   IF g_options.skip_empty_lines THEN
      IF g_field_num = 2  AND  x_fields(1) IS NULL THEN
         GOTO start_again;
      END IF;
   END IF;

   IF g_options.skip_all_null_lines THEN
      -- we'll skip the line if all the fields we are returning are blank
      -- (if we have applied a limit this check is not done on ignored fields)
      l_all_null_f := TRUE;
      FOR i IN 1 .. x_fields.count LOOP
         IF x_fields(i) IS NOT NULL THEN
            l_all_null_f := FALSE;
            EXIT;
         END IF;
      END LOOP;

      IF l_all_null_f THEN
         GOTO start_again;
      END IF;
   END IF;

   -- we are going to return this line of data - be sure it has at least as many columns as requested
   IF g_options.pad_columns IS NOT NULL THEN
      IF x_fields.count < g_options.pad_columns THEN
         x_fields.extend (g_options.pad_columns - x_fields.count);
      END IF;
   END IF;

   RETURN (TRUE);
END read_line_simple;


--------------------------------------------------------------------------------
--
-- GET_CHAR/PEEK_CHAR:  Gets or peeks at the next character in the data stream.
--
-- Used by the complex parser as that is a character-by-character parser.
--
-- Note! You must never call PEEK_CHAR immediately after GET_CHAR returns 'NL'
-- (if you do and you peek a NL then G_NL_TYPE will be for the peek not the get).
--

FUNCTION peekaboo
RETURN VARCHAR2 IS
BEGIN
   -- peeks at the next character in the input stream but does not consume it (NULL indicates end of data)
   IF g_buf_pos > g_buf_len THEN
PRAGMA INLINE (get_next_buffer, 'NO');
      get_next_buffer;
   END IF;

   RETURN (substr (g_buffer, g_buf_pos, 1));
END peekaboo;

----------------

PROCEDURE get (x_char IN OUT NOCOPY VARCHAR2) IS
   l_next_char    VARCHAR2(1 CHAR);
BEGIN
   -- gets the next character from the data stream
   -- NULL indicates end-of-file, 'NL' indicates end-of-line (and G_NL_TYPE indicates exactly the characters found)
   IF g_buf_pos > g_buf_len THEN
PRAGMA INLINE (get_next_buffer, 'NO');
      get_next_buffer;
   END IF;

   x_char    := substr (g_buffer, g_buf_pos, 1);
   g_buf_pos := g_buf_pos + 1;

   -- check to see if we have a newline (based on our current line endings setting)
   -- if it is a two character newline we consume both characters
   IF x_char = G_CR THEN
      IF g_options.line_endings = G_MAC_OS THEN
         x_char    := 'NL';
         g_nl_type := G_MAC_OS;

      ELSIF g_options.line_endings = G_DOS THEN
         IF peekaboo = G_LF THEN
            x_char    := 'NL';
            g_nl_type := G_DOS;
            g_buf_pos := g_buf_pos + 1;
         END IF;

      ELSIF g_options.line_endings = G_ANY THEN
         IF peekaboo = G_LF THEN
            x_char    := 'NL';
            g_nl_type := G_DOS;
            g_buf_pos := g_buf_pos + 1;
         ELSE
            x_char    := 'NL';
            g_nl_type := G_MAC_OS;
         END IF;
      END IF;

   ELSIF x_char = G_LF THEN
      IF g_options.line_endings = G_UNIX THEN
         x_char    := 'NL';
         g_nl_type := G_UNIX;

      ELSIF g_options.line_endings = G_RISC_OS THEN
         IF peekaboo = G_CR THEN
            x_char    := 'NL';
            g_nl_type := G_RISC_OS;
            g_buf_pos := g_buf_pos + 1;
         END IF;

      ELSIF g_options.line_endings = G_ANY THEN
         IF peekaboo = G_CR THEN
            x_char    := 'NL';
            g_nl_type := G_RISC_OS;
            g_buf_pos := g_buf_pos + 1;
         ELSE
            x_char    := 'NL';
            g_nl_type := G_UNIX;
         END IF;
      END IF;
   END IF;
END get;

----------------

PROCEDURE get_char (x_char IN OUT NOCOPY VARCHAR2) IS
BEGIN
   -- gets the next character from the data stream (NULL indicates end of data)
   -- 'NL' indicates a newline has been detected (and G_NL_TYPE indicates exactly the characters found)
   IF g_peek_f THEN
      g_peek_f := FALSE;
      x_char   := g_peek_char;
   ELSE
PRAGMA INLINE (get, 'YES');
      get (x_char);
   END IF;

   IF x_char = 'NL' THEN
      g_line_num := g_line_num + 1;
      g_char_num := 1;
   ELSE
      g_char_num := g_char_num + 1;
   END IF;
END get_char;

----------------

PROCEDURE peek_char (x_char OUT NOCOPY VARCHAR2) IS
BEGIN
   -- peeks at the next character from the data stream but does not consume it
   -- do not call this after getting a NL from GET_CHAR (as the G_NL_TYPE will not be set correctly)
   --
   -- note! we need peek variables as we can't necessarily rewind the buffer position as we may have
   -- read in a new buffer to do the peeking.
   IF NOT g_peek_f THEN
PRAGMA INLINE (get, 'YES');
      get (g_peek_char);
      g_peek_f := TRUE;
   END IF;

   x_char := g_peek_char;
END peek_char;


--------------------------------------------------------------------------------
--
-- READ_LINE_COMPLEX:  Parses a data line allowing for all options.
--
-- Returns FALSE when the end of the file/data has been reached.
--

FUNCTION read_line_complex (x_fields IN OUT NOCOPY table_of_fields)
RETURN BOOLEAN IS

   l_field           VARCHAR2(32767 BYTE);
   l_field_len       NUMBER;

   l_char            VARCHAR2(2 CHAR);    -- normally 1 char but if found end-of-line will be 'NL'
   l_peek_char       VARCHAR2(2 CHAR);
   l_in_quote_f      BOOLEAN;
   l_post_quote_f    BOOLEAN;
   l_last_non_space  NUMBER;
   l_all_null_f      BOOLEAN;

--------

   PROCEDURE finalise_field IS
   BEGIN
      -- called when end of a field is reached (i.e. got to a field separator, newline or end-of-file)
      IF g_field_num > g_options.limit_columns THEN
         NULL;          -- user doesn't want this field returned

      ELSE
         -- left spaces are trimmed while parsing but we can only do the right once we know where the end of the field is
         x_fields.extend;
         IF g_options.trim_spaces IN (G_YES, G_RIGHT)  AND  l_last_non_space > 0 THEN
            x_fields(g_field_num) := substr (l_field, 1, l_last_non_space);
         ELSE
            x_fields(g_field_num) := l_field;
         END IF;

         IF g_options.max_field_size IS NOT NULL THEN
            IF length (x_fields(g_field_num)) > g_options.max_field_size THEN
               raise_data_error (-20810, 'Field exceeds the specified maximum field size ' ||
                                         '(length is ' || length (x_fields(g_field_num)) ||
                                         ', maximum allowed ' || g_options.max_field_size || ')');
            END IF;
         END IF;
      END IF;

      g_field_num      := g_field_num + 1;
      l_field          := NULL;
      l_field_len      := 0;
      l_in_quote_f     := FALSE;
      l_post_quote_f   := FALSE;
      l_last_non_space := -1;

      -- and if there are any errors we report the line/char the field started on
      g_field_line := g_line_num;
      g_field_char := g_char_num;
   END finalise_field;

--------

   PROCEDURE add_to_field (p_char IN OUT NOCOPY VARCHAR2, p_force_data_f IN BOOLEAN) IS
      l_nl_type   NUMBER;
   BEGIN
      -- add the current character to the field data
      -- if force data is set it indicates the character is not to be trimmed if it is a space
      IF p_char = 'NL' THEN
         IF NOT g_options.allow_embedded_newlines THEN
            raise_data_error (-20812, 'Unterminated quote');
         END IF;

         -- newlines are allowed but check if we are normalising them to a standard value
         l_nl_type := nvl (g_options.normalise_embedded_newlines, g_nl_type);
         CASE l_nl_type
            WHEN G_DOS THEN
               l_field     := l_field || G_CR || G_LF;
               l_field_len := l_field_len + 2;
            WHEN G_MAC_OS THEN
               l_field     := l_field || G_CR;
               l_field_len := l_field_len + 1;
            WHEN G_RISC_OS THEN
               l_field     := l_field || G_LF || G_CR;
               l_field_len := l_field_len + 2;
            ELSE     -- G_UNIX
               l_field     := l_field || G_LF;
               l_field_len := l_field_len + 1;
         END CASE;

         l_last_non_space := l_field_len;       -- we only trim spaces not whitespace (hence a newline is data)

      ELSE
         -- not a newline so must be a single character we're adding
         l_field     := l_field || p_char;
         l_field_len := l_field_len + 1;

         IF p_force_data_f  OR  l_char != ' ' THEN
            l_last_non_space := l_field_len;
         END IF;
      END IF;

   EXCEPTION
      WHEN e_numeric_or_value_error THEN
         -- assume the only ORA-06502 error will be "character string buffer too small"
         raise_data_error (-20810, 'Field exceeds the maximum data length for a VARCHAR2 (32767 bytes)');
   END add_to_field;

--------

BEGIN

<<START_AGAIN>>
   x_fields         := table_of_fields();       -- if starting again this is equivalent to deleting the existing fields
   g_field_num      := 1;                       -- this is global so we can use it during error reporting
   l_field          := NULL;
   l_field_len      := 0;
   l_in_quote_f     := FALSE;
   l_post_quote_f   := FALSE;
   l_last_non_space := -1;

   LOOP
      get_char (l_char);

      IF NOT l_in_quote_f THEN
         -- exit when we've finished reading the line or reached end-of-file
         EXIT WHEN l_char = 'NL'  OR  l_char IS NULL;

         CASE l_char
            WHEN g_options.separator THEN
               finalise_field;

            WHEN g_options.quote THEN
               CASE g_options.quote_mode
                  WHEN G_INLINE THEN
                     l_in_quote_f := TRUE;
                  WHEN G_STRICT THEN
                     -- in strict mode, quoting has to be applied around the whole field (excluding trimmed spaces)
                     IF l_field IS NOT NULL THEN
                        raise_data_error (-20811, 'Field not quoted correctly');
                     ELSE
                        l_in_quote_f := TRUE;
                     END IF;
                  ELSE     -- G_LENIENT
                     -- in lenient mode, if the quotes aren't at the start of the field (as per strict) we assume they are part of the data
                     IF l_field IS NULL THEN
                        l_in_quote_f := TRUE;
                     ELSE
                        add_to_field (l_char, TRUE);
                     END IF;
               END CASE;

            WHEN g_options.escape THEN
               get_char (l_char);

               IF l_char IS NOT NULL THEN          -- ignore escapes immediately before end-of-file
                  add_to_field (l_char, TRUE);     -- if escaping a space then it won't be trimmed
               END IF;

            ELSE
               -- not a special character so just add as a bit of data
               IF l_char = ' '  AND  g_options.trim_spaces IN (G_LEFT, G_YES)  AND  l_field IS NULL THEN
                  NULL;       -- don't add spaces at the start of the field (if trimming spaces)

               ELSE
                  IF l_post_quote_f  AND  l_char != ' ' THEN
                     raise_data_error (-20811, 'Field not quoted correctly');
                  END IF;

                  add_to_field (l_char, FALSE);
               END IF;
         END CASE;

      ELSE     -- l_in_quote_f
         IF l_char IS NULL THEN
            IF g_options.quote_mode = G_STRICT THEN
               raise_data_error (-20812, 'Unterminated quote');
            END IF;

            EXIT;
         END IF;

         CASE l_char
            WHEN 'NL' THEN
               add_to_field (l_char, TRUE);

            WHEN g_options.quote THEN
               peek_char (l_peek_char);
               -- to embed a quote when quoted you must double the quote
               IF l_peek_char = g_options.quote THEN
                  get_char (l_char);
                  add_to_field (l_char, TRUE);

               ELSE
                  l_in_quote_f := FALSE;
                  IF g_options.quote_mode = G_STRICT THEN
                     l_post_quote_f := TRUE;       -- in strict mode, there can be no data after the closing quote
                  END IF;
               END IF;

            WHEN g_options.escape THEN
               CASE g_options.quoted_escape_mode
                  WHEN G_ESCAPE_ALL THEN
                     -- escape always applies even if the next character is not special
                     get_char (l_char);

                     IF l_char IS NOT NULL THEN       -- ignore escapes immediately before end-of-file
                        add_to_field (l_char, TRUE);  -- if escaping a space then it won't be trimmed
                     END IF;

                  WHEN G_ESCAPE_SPECIAL THEN
                     -- I have decided the newlines are not special characters - you may disagree
                     peek_char (l_peek_char);
                     IF l_peek_char IN (g_options.separator, g_options.quote, g_options.escape) THEN
                        get_char (l_char);
                        add_to_field (l_char, TRUE);
                     END IF;

                  WHEN G_ESCAPE_QUOTE THEN
                     peek_char (l_peek_char);
                     IF l_peek_char = g_options.quote THEN
                        get_char (l_char);
                        add_to_field (l_char, TRUE);
                     END IF;

                  ELSE     -- G_NOT_SPECIAL
                     -- nothing special about escapes when quoted so just add it to the field
                     add_to_field (l_char, TRUE);
               END CASE;

            ELSE
               -- not a special character so just add as a bit of data
               -- no checks about trimming spaces as quoted spaces are always considered part of data
               add_to_field (l_char, TRUE);
         END CASE;
      END IF;
   END LOOP;

   -- we've either reached end-of-line or end-of-file
   -- note! this will increment the field num
   finalise_field;

   g_record_num := g_record_num + 1;

   IF l_char IS NULL  AND  g_field_num = 2  AND  x_fields(1) IS NULL  THEN
      -- reached end-of-file and there was no data on the line so we're done - no more data to return
      x_fields.delete;
      RETURN (FALSE);
   END IF;

   -- check if we are skipping this line - if so we just start reading again
   IF g_options.skip_lines > 0 THEN
      g_options.skip_lines := g_options.skip_lines - 1;
      GOTO start_again;
   END IF;

   IF g_options.skip_empty_lines THEN
      IF g_field_num = 2  AND  x_fields(1) IS NULL THEN
         GOTO start_again;
      END IF;
   END IF;

   IF g_options.skip_all_null_lines THEN
      -- we'll skip the line if all the fields we are returning are blank
      -- (if we have applied a limit this check is not done on ignored fields)
      l_all_null_f := TRUE;
      FOR i IN 1 .. x_fields.count LOOP
         IF x_fields(i) IS NOT NULL THEN
            l_all_null_f := FALSE;
            EXIT;
         END IF;
      END LOOP;

      IF l_all_null_f THEN
         GOTO start_again;
      END IF;
   END IF;

   -- we are going to return this line of data - be sure it has at least as many columns as requested
   IF g_options.pad_columns IS NOT NULL THEN
      IF x_fields.count < g_options.pad_columns THEN
         x_fields.extend (g_options.pad_columns - x_fields.count);
      END IF;
   END IF;

   RETURN (TRUE);
END read_line_complex;


--------------------------------------------------------------------------------
--
-- READ_LINE:  Reads the next line from the data stream and parses it to separate fields.
--
-- Returns FALSE when no more data is available (i.e. end of the file has been reached).
--

FUNCTION read_line (x_fields OUT NOCOPY table_of_fields)
RETURN BOOLEAN IS
BEGIN
   IF g_data_source IS NULL THEN
      raise_application_error (-20801, 'CSV Reader: You must OPEN a data source before reading lines');
   END IF;

   IF g_simple_f THEN
      RETURN (read_line_simple (x_fields));
   ELSE
      RETURN (read_line_complex (x_fields));
   END IF;
END read_line;


--------------------------------------------------------------------------------
--
-- CLOSE:  Cleans up resources used by the CSV reader.
--
-- Note! If the reader was openend on a file stream (UTL_FILE.FILE_TYPE) then it
-- is up to the caller to close the file stream.
--

PROCEDURE close IS
BEGIN
   g_data_source := NULL;
   g_buffer      := NULL;
   g_clob        := NULL;        -- hopefully will free up memory
END close;


--------------------------------------------------------------------------------
--
-- READ_LINES:  Reads all lines from a data source.
--
-- This is pipelined and returns each field split into a separate column so is
-- easy to use directly from a SQL statement.
--

FUNCTION get_line (x_line_rec OUT NOCOPY line_rec)
RETURN BOOLEAN IS

   l_fields    table_of_fields;

BEGIN
   IF NOT read_line (l_fields) THEN
      RETURN (FALSE);
   END IF;

   BEGIN
      -- copy each field across to the equivalent column
      x_line_rec.c01 := l_fields(1);
      x_line_rec.c02 := l_fields(2);
      x_line_rec.c03 := l_fields(3);
      x_line_rec.c04 := l_fields(4);
      x_line_rec.c05 := l_fields(5);
      x_line_rec.c06 := l_fields(6);
      x_line_rec.c07 := l_fields(7);
      x_line_rec.c08 := l_fields(8);
      x_line_rec.c09 := l_fields(9);
      x_line_rec.c10 := l_fields(10);
      x_line_rec.c11 := l_fields(11);
      x_line_rec.c12 := l_fields(12);
      x_line_rec.c13 := l_fields(13);
      x_line_rec.c14 := l_fields(14);
      x_line_rec.c15 := l_fields(15);
      x_line_rec.c16 := l_fields(16);
      x_line_rec.c17 := l_fields(17);
      x_line_rec.c18 := l_fields(18);
      x_line_rec.c19 := l_fields(19);
      x_line_rec.c20 := l_fields(20);
      x_line_rec.c21 := l_fields(21);
      x_line_rec.c22 := l_fields(22);
      x_line_rec.c23 := l_fields(23);
      x_line_rec.c24 := l_fields(24);
      x_line_rec.c25 := l_fields(25);
      x_line_rec.c26 := l_fields(26);
      x_line_rec.c27 := l_fields(27);
      x_line_rec.c28 := l_fields(28);
      x_line_rec.c29 := l_fields(29);
      x_line_rec.c30 := l_fields(30);
      x_line_rec.c31 := l_fields(31);
      x_line_rec.c32 := l_fields(32);
      x_line_rec.c33 := l_fields(33);
      x_line_rec.c34 := l_fields(34);
      x_line_rec.c35 := l_fields(35);
      x_line_rec.c36 := l_fields(36);
      x_line_rec.c37 := l_fields(37);
      x_line_rec.c38 := l_fields(38);
      x_line_rec.c39 := l_fields(39);
      x_line_rec.c40 := l_fields(40);
      x_line_rec.c41 := l_fields(41);
      x_line_rec.c42 := l_fields(42);
      x_line_rec.c43 := l_fields(43);
      x_line_rec.c44 := l_fields(44);
      x_line_rec.c45 := l_fields(45);
      x_line_rec.c46 := l_fields(46);
      x_line_rec.c47 := l_fields(47);
      x_line_rec.c48 := l_fields(48);
      x_line_rec.c49 := l_fields(49);
      x_line_rec.c50 := l_fields(50);
      x_line_rec.c51 := l_fields(51);
      x_line_rec.c52 := l_fields(52);
      x_line_rec.c53 := l_fields(53);
      x_line_rec.c54 := l_fields(54);
      x_line_rec.c55 := l_fields(55);
      x_line_rec.c56 := l_fields(56);
      x_line_rec.c57 := l_fields(57);
      x_line_rec.c58 := l_fields(58);
      x_line_rec.c59 := l_fields(59);
      x_line_rec.c60 := l_fields(60);
      x_line_rec.c61 := l_fields(61);
      x_line_rec.c62 := l_fields(62);
      x_line_rec.c63 := l_fields(63);
      x_line_rec.c64 := l_fields(64);
      x_line_rec.c65 := l_fields(65);
      x_line_rec.c66 := l_fields(66);
      x_line_rec.c67 := l_fields(67);
      x_line_rec.c68 := l_fields(68);
      x_line_rec.c69 := l_fields(69);
      x_line_rec.c70 := l_fields(70);
      x_line_rec.c71 := l_fields(71);
      x_line_rec.c72 := l_fields(72);
      x_line_rec.c73 := l_fields(73);
      x_line_rec.c74 := l_fields(74);
      x_line_rec.c75 := l_fields(75);
      x_line_rec.c76 := l_fields(76);
      x_line_rec.c77 := l_fields(77);
      x_line_rec.c78 := l_fields(78);
      x_line_rec.c79 := l_fields(79);
      x_line_rec.c80 := l_fields(80);
      x_line_rec.c81 := l_fields(81);
      x_line_rec.c82 := l_fields(82);
      x_line_rec.c83 := l_fields(83);
      x_line_rec.c84 := l_fields(84);
      x_line_rec.c85 := l_fields(85);
      x_line_rec.c86 := l_fields(86);
      x_line_rec.c87 := l_fields(87);
      x_line_rec.c88 := l_fields(88);
      x_line_rec.c89 := l_fields(89);
      x_line_rec.c90 := l_fields(90);
      x_line_rec.c91 := l_fields(91);
      x_line_rec.c92 := l_fields(92);
      x_line_rec.c93 := l_fields(93);
      x_line_rec.c94 := l_fields(94);
      x_line_rec.c95 := l_fields(95);
      x_line_rec.c96 := l_fields(96);
      x_line_rec.c97 := l_fields(97);
      x_line_rec.c98 := l_fields(98);
      x_line_rec.c99 := l_fields(99);

   EXCEPTION
      WHEN e_subscript_beyond_count THEN
         -- not actually an error - just reached the end of the fields on this line
         NULL;
   END;

   RETURN (TRUE);
END get_line;

----------------

FUNCTION read_lines (p_clob IN CLOB)
RETURN table_of_lines
PIPELINED IS
   l_fields    line_rec;
BEGIN
   csv_reader.open (p_clob);

   if g_options.max_field_size > 4000 then
      set_max_field_size (4000);
   end if;

   WHILE get_line (l_fields) LOOP
      PIPE ROW (l_fields);
   END LOOP;

   csv_reader.close;
END read_lines;

----------------

FUNCTION read_lines (p_directory     IN VARCHAR2,
                     p_filename      IN VARCHAR2,
                     p_character_set IN VARCHAR2 := NULL)         -- defaults to the database character set
RETURN table_of_lines
PIPELINED IS
   l_fields    line_rec;
BEGIN
   csv_reader.open (p_directory, p_filename, p_character_set);

   if g_options.max_field_size > 4000 then
      set_max_field_size (4000);
   end if;

   WHILE get_line (l_fields) LOOP
      PIPE ROW (l_fields);
   END LOOP;

   csv_reader.close;
END read_lines;

----------------

FUNCTION read_lines
RETURN table_of_lines
PIPELINED IS
   l_fields    line_rec;
BEGIN
   -- you must have already opened the data stream
   -- might be useful if you need full control of options but still want to work in SQL
   WHILE get_line (l_fields) LOOP
      PIPE ROW (l_fields);
   END LOOP;

   csv_reader.close;
END read_lines;


--------------------------------------------------------------------------------
--
-- TOKENISE:  Very basic parser to split a line of data into separate fields.
--
-- Multi-character separators are supported but quoting and escaping are not;
-- spaces are significant. You get back one more field than there are separators
-- EXCEPT you get back no fields for a NULL line (not sure about this though).
--
-- This is standalone utility that does not affect and is not affected by the
-- main CSV reader functions.
--

FUNCTION tokenise (p_line IN CLOB, p_separator IN VARCHAR2 := ',')
RETURN table_of_fields IS

   l_fields    table_of_fields;
   l_field_num NUMBER;
   l_prev_pos  NUMBER;
   l_sep_pos   NUMBER;
   l_sep_len   NUMBER;

BEGIN
   l_fields    := table_of_fields();
   l_field_num := 0;
   l_sep_len   := length (p_separator);

   IF p_line IS NOT NULL THEN
      l_prev_pos := 1;

      LOOP
         l_field_num := l_field_num + 1;
         l_sep_pos   := instr (p_line, p_separator, l_prev_pos);

         IF l_sep_pos = 0 THEN
            -- no more separators
            l_fields.extend;
            l_fields(l_field_num) := substr (p_line, l_prev_pos);
            EXIT;
         END IF;

         l_fields.extend;
         l_fields(l_field_num) := substr (p_line, l_prev_pos, l_sep_pos - l_prev_pos);
         l_prev_pos := l_sep_pos + l_sep_len;
      END LOOP;
   END IF;

   RETURN (l_fields);
END tokenise;


--------------------------------------------------------------------------------
--
-- PACKAGE INITIALISATION SECTION
--
-- Initialises the standard option sets.  It'd be better to declare these as
-- constants but we are allowing for older DBs where this is not that easy.
--

BEGIN
   -- standard CSV using double quotes for enclosures - embedded line breaks not allowed
   G_CSV.separator                        := ',';
   G_CSV.quote                            := '"';
   G_CSV.escape                           := NULL;
   G_CSV.line_endings                     := G_ANY;
   G_CSV.skip_lines                       := 0;
   G_CSV.skip_empty_lines                 := TRUE;
   G_CSV.skip_all_null_lines              := TRUE;
   G_CSV.quote_mode                       := G_STRICT;
   G_CSV.quoted_escape_mode               := G_NOT_SPECIAL;
   G_CSV.allow_embedded_newlines          := FALSE;
   G_CSV.normalise_embedded_newlines      := NULL;
   G_CSV.trim_spaces                      := G_NO;
   G_CSV.pad_columns                      := NULL;
   G_CSV.limit_columns                    := NULL;
   G_CSV.max_field_size                   := NULL;

   -- simple CSV with no quoting or escaping (embedded commas and line breaks not allowed)
   G_CSV_SIMPLE                           := G_CSV;
   G_CSV_SIMPLE.quote                     := NULL;
   G_CSV_SIMPLE.escape                    := NULL;

   -- strict CSV requires DOS line endings and allows embedded line breaks)
   G_CSV_STRICT                           := G_CSV;
   G_CSV_STRICT.line_endings              := G_DOS;            -- required under RFC 4180 but you may wish to override this
   G_CSV_STRICT.allow_embedded_newlines   := TRUE;

   -- tab separated allowing quoting (as Excel allows this) - embedded line breaks not allowed
   G_TSV.separator                        := G_TAB;
   G_TSV.quote                            := '"';
   G_TSV.escape                           := NULL;
   G_TSV.line_endings                     := G_ANY;
   G_TSV.skip_lines                       := 0;
   G_TSV.skip_empty_lines                 := TRUE;
   G_TSV.skip_all_null_lines              := TRUE;
   G_TSV.quote_mode                       := G_STRICT;
   G_TSV.quoted_escape_mode               := G_NOT_SPECIAL;
   G_TSV.allow_embedded_newlines          := FALSE;
   G_TSV.normalise_embedded_newlines      := NULL;
   G_TSV.trim_spaces                      := G_NO;
   G_TSV.pad_columns                      := NULL;
   G_TSV.limit_columns                    := NULL;
   G_TSV.max_field_size                   := NULL;

   -- simple tab separated with no quoting or escaping (embedded tabs and line breaks not allowed)
   G_TSV_SIMPLE                           := G_TSV;
   G_TSV_SIMPLE.quote                     := NULL;
   G_CSV_SIMPLE.escape                    := NULL;

   -- if no options are specified we default to standard CSV reading
   g_options := G_CSV;
END csv_reader;
/
