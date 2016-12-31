

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <strings.h>
#include <err.h>
#include <ctype.h>
#include <assert.h>

#include <sys/list.h>
#include <custr.h>
#include <strlist.h>
#include <jsonemitter.h>
#include <libnvpair.h>

#include "common.h"

typedef enum sql_state {
	STATE_SQL_REST = 1,
	STATE_SQL_DOLLAR1,
	STATE_SQL_DOLLAR_TAG,
	STATE_SQL_DOLLAR_STRING,
	STATE_SQL_DOLLAR_STRING_END_TAG,
	STATE_SQL_DASH1,
	STATE_SQL_DASH_COMMENT,
	STATE_SQL_SLASH1,
	STATE_SQL_OPERATOR,
	STATE_SQL_NUMBER0,
	STATE_SQL_NUMBER_DECIMAL,
	STATE_SQL_NUMBER_EXPONENT,
	STATE_SQL_STRING,
	STATE_SQL_STRING_QUOTE,
	STATE_SQL_QUOTED_ID,
	STATE_SQL_QUOTED_ID_QUOTE,
	STATE_SQL_NAME,
} sql_state_t;

typedef enum copy_state {
	STATE_COPY_REST = 1,
	STATE_COPY_NULL_CHECK,
	STATE_COPY_COLUMN,
	STATE_COPY_COLUMN_ESCAPED,
	STATE_COPY_MAYBE_EOD,
} copy_state_t;

typedef enum ingest_action {
	INGEST_AGAIN = 1,
	INGEST_NEXT,
	INGEST_ERROR,
} ingest_action_t;

typedef struct sqlt_copy {
	command_copy_t *sqcp_command;
	copy_state_t sqcp_state;
	strlist_t *sqcp_output;
	unsigned sqcp_output_ncols;
	custr_t *sqcp_accum;
	unsigned sqcp_rows;
	FILE *sqcp_file;
	json_emit_t *sqcp_json;
	nvlist_t *sqcp_nvl;
} sqlt_copy_t;

typedef struct sqlt {
	list_t sqlt_inq;
	list_t sqlt_state_stack;
	FILE *sqlt_file;
	sql_state_t sqlt_state;
	custr_t *sqlt_accum;
	custr_t *sqlt_dollar_token;
	list_t sqlt_command;
	unsigned sqlt_command_count;
	sqlt_copy_t *sqlt_copy;
} sqlt_t;

typedef struct inq {
	char *inq_buf;
	size_t inq_buf_size;	/* allocated size */
	size_t inq_pos;		/* reading position */
	size_t inq_len;		/* length of data in buffer */
	list_node_t inq_link;
} inq_t;

typedef struct state_frame {
	sql_state_t stfr_state;
	custr_t *stfr_accum;
	list_node_t stfr_link;
} state_frame_t;

static int
isoper(char chr)
{
	return (strchr("+-*/<>=~!@#%^&|`?", chr) != NULL);
}

static int
isspecial(char chr)
{
	return (strchr("$()[],;:*.", chr) != NULL);
}

void
sqlt_push_state(sqlt_t *sqlt, sql_state_t st)
{
	state_frame_t *stfr;

	if ((stfr = calloc(1, sizeof (*stfr))) == NULL) {
		err(1, "calloc");
	}

	stfr->stfr_state = sqlt->sqlt_state;
	stfr->stfr_accum = sqlt->sqlt_accum;
	list_insert_head(&sqlt->sqlt_state_stack, stfr);

	sqlt->sqlt_state = st;
	if (custr_alloc(&sqlt->sqlt_accum) != 0) {
		err(1, "custr_alloc");
	}
}

void
sqlt_pop_state(sqlt_t *sqlt)
{
	if (list_is_empty(&sqlt->sqlt_state_stack)) {
		errx(1, "state %d pop fail", sqlt->sqlt_state);
	}

	state_frame_t *stfr = list_remove_head(&sqlt->sqlt_state_stack);
	custr_free(sqlt->sqlt_accum);
	sqlt->sqlt_accum = stfr->stfr_accum;
	sqlt->sqlt_state = stfr->stfr_state;
	free(stfr);
}

int
sqlt_inq_alloc(inq_t **inqp, size_t sz)
{
	inq_t *inq;

	if ((inq = calloc(1, sizeof (*inq))) == NULL) {
		return (-1);
	}

	inq->inq_buf_size = sz;
	if ((inq->inq_buf = malloc(inq->inq_buf_size)) == NULL) {
		free(inq);
		return (-1);
	}

	*inqp = inq;
	return (0);
}

void
sqlt_inq_free(inq_t *inq)
{
	free(inq->inq_buf);
	free(inq);
}

int
sqlt_inq_read(inq_t *inq, FILE *fstr)
{
	if ((inq->inq_len = fread(inq->inq_buf, 1, inq->inq_buf_size, fstr)) < 1) {
		/*
		 * XXX
		 */
		errx(1, "return < 1");
	}

	return (0);
}

int
sqlt_alloc(sqlt_t **sqltp)
{
	sqlt_t *sqlt;

	if ((sqlt = calloc(1, sizeof (*sqlt))) == NULL) {
		return (-1);
	}

	if (custr_alloc(&sqlt->sqlt_accum) != 0) {
		free(sqlt);
		return (-1);
	}

	if (custr_alloc(&sqlt->sqlt_dollar_token) != 0) {
		custr_free(sqlt->sqlt_accum);
		free(sqlt);
		return (-1);
	}

	list_create(&sqlt->sqlt_inq, sizeof (inq_t),
	    offsetof(inq_t, inq_link));
	list_create(&sqlt->sqlt_state_stack, sizeof (state_frame_t),
	    offsetof(state_frame_t, stfr_link));
	list_create(&sqlt->sqlt_command, sizeof (event_t),
	    offsetof(event_t, evt_link));

	sqlt->sqlt_state = STATE_SQL_REST;

	*sqltp = sqlt;
	return (0);
}

void
sqlt_commit(sqlt_t *sqlt, event_type_t t)
{
	const char *val = custr_cstr(sqlt->sqlt_accum);

	if (t == EVENT_NEWLINE && sqlt->sqlt_command_count == 0) {
		/*
		 * Drop leading newlines from commands.
		 */
		return;
	}

	if (t == EVENT_SPECIAL && strcmp(val, ";") == 0) {
#if 0
		fprintf(stdout, "COMMAND: ");

		for (event_t *evt = list_head(&sqlt->sqlt_command);
		    evt != NULL; evt = list_next(&sqlt->sqlt_command, evt)) {
			switch (evt->evt_t) {
			case EVENT_NEWLINE:
				fprintf(stdout, "newline ");
				break;
			case EVENT_SPECIAL:
				fprintf(stdout, "special(%s) ", evt->evt_v);
				break;
			case EVENT_STRING:
				fprintf(stdout, "string(%s) ", evt->evt_v);
				break;
			case EVENT_QUOTED_NAME:
				fprintf(stdout, "quoted_name(%s) ", evt->evt_v);
				break;
			case EVENT_OPERATOR:
				fprintf(stdout, "operator(%s) ", evt->evt_v);
				break;
			case EVENT_NAME:
				fprintf(stdout, "name(%s) ", evt->evt_v);
				break;
			case EVENT_NUMBER:
				fprintf(stdout, "number(%s) ", evt->evt_v);
				break;
			}
		}

		fprintf(stdout, "\n");
#endif

		/*
		 * Parse the command!
		 */
		command_copy_t *copycmd = NULL;
		switch (parse_command(&sqlt->sqlt_command, &copycmd)) {
		case -1:
			errx(1, "SQL PARSE ERROR");
			break;

		case 0:
			break;

		case 1:
			sqlt->sqlt_copy = calloc(1, sizeof (*sqlt->sqlt_copy));
			/* XXX */
			sqlt->sqlt_copy->sqcp_command = copycmd;
			sqlt->sqlt_copy->sqcp_state = STATE_COPY_REST;
			if (strlist_alloc(&sqlt->sqlt_copy->sqcp_output, 0) != 0) {
				err(1, "strlist_alloc");
			}
			if (custr_alloc(&sqlt->sqlt_copy->sqcp_accum) != 0) {
				err(1, "custr_alloc");
			}

			fprintf(stderr, "COPY [%s]\n", sqlt->sqlt_copy->sqcp_command->cmdc_table_name);

			char buf[512];
			snprintf(buf, 512, "%s/%s.json", "OUTPUT_DIR", sqlt->sqlt_copy->sqcp_command->cmdc_table_name);
			if ((sqlt->sqlt_copy->sqcp_file = fopen(buf, "wx")) == NULL) {
				err(1, "fopen(%s)", buf);
			}
#if 0
			if ((sqlt->sqlt_copy->sqcp_json = json_create_stdio(sqlt->sqlt_copy->sqcp_file)) == NULL) {
				err(1, "json_create_stdio");
			}
#else
			if (nvlist_alloc(&sqlt->sqlt_copy->sqcp_nvl, NV_UNIQUE_NAME, 0) != 0) {
				err(1, "nvlist_alloc");
			}
#endif
			break;
		}

		while (!list_is_empty(&sqlt->sqlt_command)) {
			event_t *evt = list_remove_head(&sqlt->sqlt_command);

			free(evt->evt_v);
			free(evt);
		}

		return;
	}

	/*
	 * Append this token to the end of the current accumulating command.
	 */
	event_t *evt = calloc(1, sizeof (*evt));
	evt->evt_t = t;
	evt->evt_v = strdup(custr_cstr(sqlt->sqlt_accum));
	list_insert_tail(&sqlt->sqlt_command, evt);
}

static ingest_action_t
sqlt_ingest_copy_commit(sqlt_t *sqlt, const char *val, int is_last)
{
	sqlt_copy_t *sqcp = sqlt->sqlt_copy;

	if (sqcp->sqcp_output_ncols > strlist_contig_count(sqcp->sqcp_command->cmdc_column_names)) {
		errx(1, "too many columns on COPY row");
		return (INGEST_ERROR);
	}

//	strlist_set(sqcp->sqcp_output, sqcp->sqcp_output_ncols++, val);
	if (val == NULL) {
		nvlist_add_boolean(sqcp->sqcp_nvl, strlist_get(sqcp->sqcp_command->cmdc_column_names, sqcp->sqcp_output_ncols++));
	} else {
		nvlist_add_string(sqcp->sqcp_nvl, strlist_get(sqcp->sqcp_command->cmdc_column_names, sqcp->sqcp_output_ncols++), val);
	}

	if (!is_last) {
		/*
		 * Look for another column.
		 */
		sqcp->sqcp_state = STATE_COPY_NULL_CHECK;
		custr_reset(sqcp->sqcp_accum);
		return (INGEST_NEXT);
	}

	if (sqcp->sqcp_output_ncols != strlist_contig_count(sqcp->sqcp_command->cmdc_column_names)) {
		errx(1, "too few columns on COPY row");
		return (INGEST_ERROR);
	}

	/*
	 * This was the last column, and we have the correct number
	 * of columns.  Emit the entire row.
	 */
	/* XXX emit COPY_ROW */
#if 0
	json_object_begin(sqcp->sqcp_json, NULL);
	for (unsigned i = 0; i < sqcp->sqcp_output_ncols; i++) {
		const char *key = strlist_get(sqcp->sqcp_command->cmdc_column_names, i);
		const char *val = strlist_get(sqcp->sqcp_output, i);

		if (val == NULL) {
			json_null(sqcp->sqcp_json, key);
		} else {
			json_utf8string(sqcp->sqcp_json, key, val);
		}
	}
	json_object_end(sqcp->sqcp_json);
	json_newline(sqcp->sqcp_json);
#else
	/*
	 * XXX
	 */
	nvlist_print_json(sqcp->sqcp_file, sqcp->sqcp_nvl);
	fputc('\n', sqcp->sqcp_file);
#endif

#if 0
	for (unsigned i = 0; i < sqcp->sqcp_output_ncols; i++) {
		fprintf(stdout, "[%2u] %s:\n", i, strlist_get(sqcp->sqcp_command->cmdc_column_names, i));
		const char *x = strlist_get(sqcp->sqcp_output, i);
		fprintf(stdout, "\t%s\n", x == NULL ? "<NULL>" : x);
	}
	fprintf(stdout, "\n");
#endif
	/* XXX emit COPY_ROW */
	sqcp->sqcp_rows++;
	strlist_reset(sqcp->sqcp_output);
	sqcp->sqcp_output_ncols = 0;

	/*
	 * Look for another row.
	 */
	sqcp->sqcp_state = STATE_COPY_NULL_CHECK;
	custr_reset(sqcp->sqcp_accum);
	return (INGEST_NEXT);
}

ingest_action_t
sqlt_ingest_copy(sqlt_t *sqlt, char chr)
{
	sqlt_copy_t *sqcp = sqlt->sqlt_copy;

	switch (sqcp->sqcp_state) {
	case STATE_COPY_REST:
		if (chr != '\n') {
			errx(1, "expected new line after COPY command");
			return (INGEST_ERROR);
		}
		sqcp->sqcp_state = STATE_COPY_NULL_CHECK;
		return (INGEST_NEXT);

	case STATE_COPY_NULL_CHECK: {
		const char *nullstr = sqcp->sqcp_command->cmdc_null_string;
		size_t acclen = custr_len(sqcp->sqcp_accum);

		if (acclen < strlen(nullstr) && chr == nullstr[acclen]) {
			custr_appendc(sqcp->sqcp_accum, chr);
			return (INGEST_NEXT);
		} else if (acclen == strlen(nullstr)) {
			if (chr == sqcp->sqcp_command->cmdc_delimiter) {
				return (sqlt_ingest_copy_commit(sqlt, NULL, 0));
			} else if (chr == '\n') {
				return (sqlt_ingest_copy_commit(sqlt, NULL, 1));
			}
		}

		inq_t *inq;
		if (sqlt_inq_alloc(&inq, custr_len(sqcp->sqcp_accum)) != 0) {
			err(1, "sqlt_inq_alloc");
		}
		inq->inq_len = custr_len(sqcp->sqcp_accum);
		bcopy(custr_cstr(sqcp->sqcp_accum), inq->inq_buf, inq->inq_len);
		list_insert_head(&sqlt->sqlt_inq, inq);

		custr_reset(sqcp->sqcp_accum);
		sqcp->sqcp_state = STATE_COPY_COLUMN;
		return (INGEST_AGAIN);
	}

	case STATE_COPY_COLUMN:
		if (chr == sqcp->sqcp_command->cmdc_delimiter) {
			return (sqlt_ingest_copy_commit(sqlt, custr_cstr(sqcp->sqcp_accum), 0));
		} else if (chr == '\n') {
			return (sqlt_ingest_copy_commit(sqlt, custr_cstr(sqcp->sqcp_accum), 1));
		}

		if (chr == '\\') {
			sqcp->sqcp_state = STATE_COPY_COLUMN_ESCAPED;
		} else {
			custr_appendc(sqcp->sqcp_accum, chr);
		}
		return (INGEST_NEXT);

	case STATE_COPY_COLUMN_ESCAPED:
		if (chr == '.' && sqcp->sqcp_output_ncols == 0 &&
		    custr_len(sqcp->sqcp_accum) == 0) {
			sqcp->sqcp_state = STATE_COPY_MAYBE_EOD;
		} else {
			sqcp->sqcp_state = STATE_COPY_COLUMN;
		}
		custr_appendc(sqcp->sqcp_accum, chr);
		return (INGEST_NEXT);

	case STATE_COPY_MAYBE_EOD:
		if (chr != '\n') {
			/*
			 * False alarm!
			 */
			sqcp->sqcp_state = STATE_COPY_COLUMN;
			return (INGEST_AGAIN);
		}

		fprintf(stderr, "COPY END (%u ROWS)\n", sqcp->sqcp_rows);

		/*
		 * Return to the regular SQL state machine.
		 * XXX Free this.
		 */
#if 0
		json_fini(sqlt->sqlt_copy->sqcp_json);
#else
		nvlist_free(sqlt->sqlt_copy->sqcp_nvl);
#endif
		fclose(sqlt->sqlt_copy->sqcp_file);
		sqlt->sqlt_copy = NULL;
		return (INGEST_NEXT);
	}

	return (INGEST_ERROR);
}

ingest_action_t
sqlt_ingest_sql(sqlt_t *sqlt, char chr)
{
	switch (sqlt->sqlt_state) {
	case STATE_SQL_REST:
		custr_reset(sqlt->sqlt_accum);

		if (isalpha(chr) || chr == '_') {
			sqlt_push_state(sqlt, STATE_SQL_NAME);
			return (INGEST_AGAIN);
		}

		if (chr == '\n') {
			sqlt_commit(sqlt, EVENT_NEWLINE);
			return (INGEST_NEXT);
		}

		if (isspace(chr)) {
			return (INGEST_NEXT);
		}

		if (strchr(".;,()", chr) != NULL) {
			custr_appendc(sqlt->sqlt_accum, chr);
			sqlt_commit(sqlt, EVENT_SPECIAL);
			return (INGEST_NEXT);
		}

		if (chr == '$') {
			sqlt_push_state(sqlt, STATE_SQL_DOLLAR1);
			return (INGEST_NEXT);
		}

		if (chr == '-') {
			sqlt_push_state(sqlt, STATE_SQL_DASH1);
			return (INGEST_NEXT);
		}

		if (chr == '/') {
			sqlt_push_state(sqlt, STATE_SQL_SLASH1);
			return (INGEST_NEXT);
		}

		if (chr == '=' || chr == ':' || chr == '*' || chr == '+') {
			sqlt_push_state(sqlt, STATE_SQL_OPERATOR);
			return (INGEST_AGAIN);
		}

		if (isdigit(chr)) {
			sqlt_push_state(sqlt, STATE_SQL_NUMBER0);
			return (INGEST_AGAIN);
		}

		if (chr == '\'') {
			sqlt_push_state(sqlt, STATE_SQL_STRING);
			return (INGEST_NEXT);
		}

		if (chr == '"') {
			sqlt_push_state(sqlt, STATE_SQL_QUOTED_ID);
			return (INGEST_NEXT);
		}

		errx(1, "invalid character \"%c\"", chr);
		return (INGEST_ERROR);

	case STATE_SQL_DOLLAR1:
		if (chr == '$') {
			custr_reset(sqlt->sqlt_dollar_token);
			sqlt->sqlt_state = STATE_SQL_DOLLAR_STRING;
			return (INGEST_NEXT);
		}

		/*
		 * Technically any character is valid in the dollar quoting
		 * tag, but I have not yet seen anything but letters and the
		 * underscore.  If we relax this, we should be careful about
		 * embedded newline characters.
		 */
		if (chr == '_' || isalpha(chr)) {
			sqlt->sqlt_state = STATE_SQL_DOLLAR_STRING;
			sqlt_push_state(sqlt, STATE_SQL_DOLLAR_TAG);
			return (INGEST_AGAIN);
		}

		errx(1, "invalid sequence \"$%c\"", chr);
		return (INGEST_ERROR);

	case STATE_SQL_DOLLAR_TAG:
		if (chr == '$') {
			custr_reset(sqlt->sqlt_dollar_token);
			custr_append(sqlt->sqlt_dollar_token, custr_cstr(sqlt->sqlt_accum));
			sqlt_pop_state(sqlt);
			return (INGEST_NEXT);
		}

		if (chr == '_' || isalpha(chr)) {
			/*
			 * See comments for DOLLAR1 state.
			 */
			custr_appendc(sqlt->sqlt_accum, chr);
			return (INGEST_NEXT);
		}

		errx(1, "invalid sequence \"$%s%c\"",
		    custr_cstr(sqlt->sqlt_accum), chr);
		return (INGEST_ERROR);

	case STATE_SQL_DOLLAR_STRING:
		if (chr == '$') {
			sqlt_push_state(sqlt, STATE_SQL_DOLLAR_STRING_END_TAG);
			return (INGEST_NEXT);
		}

		if (chr == '\n') {
			errx(1, "unterminated string \"%s\"",
			    custr_cstr(sqlt->sqlt_accum));
			return (INGEST_ERROR);
		}

		custr_appendc(sqlt->sqlt_accum, chr);
		return (INGEST_NEXT);

	case STATE_SQL_DOLLAR_STRING_END_TAG:
		if (chr == '$') {
			if (strcmp(custr_cstr(sqlt->sqlt_accum),
			    custr_cstr(sqlt->sqlt_dollar_token)) == 0) {
				/*
				 * We have reached the end of the string.
				 * The actual string is stored in the
				 * frame above us, which should be a
				 * DOLLAR_STRING frame.
				 */
				sqlt_pop_state(sqlt);
				custr_reset(sqlt->sqlt_dollar_token);
				sqlt_commit(sqlt, EVENT_STRING);

				/*
				 * Pop the dollar string frame itself.
				 */
				assert(sqlt->sqlt_state == STATE_SQL_DOLLAR_STRING);
				sqlt_pop_state(sqlt);
				return (INGEST_NEXT);
			}

			/*
			 * False alarm.  Flush out the accumulator to the
			 * frame above us.
			 */
			char *t = strdup(custr_cstr(sqlt->sqlt_accum));
			sqlt_pop_state(sqlt);
			custr_append_printf(sqlt->sqlt_accum, "$%s$", t);
			free(t);
			return (INGEST_NEXT);
		}

		custr_appendc(sqlt->sqlt_accum, chr);

		if (custr_len(sqlt->sqlt_accum) < custr_len(sqlt->sqlt_dollar_token)) {
			/*
			 * Need to read more tag characters.
			 */
			return (INGEST_NEXT);
		}

		if (strcmp(custr_cstr(sqlt->sqlt_accum), custr_cstr(sqlt->sqlt_dollar_token)) == 0) {
			/*
			 * This is the token!  Now we need a closing dollar sign.
			 */
			return (INGEST_NEXT);
		}

		/*
		 * False alarm.  Flush out the accumulator to the frame above
		 * us.
		 */
		char *tt = strdup(custr_cstr(sqlt->sqlt_accum));
		sqlt_pop_state(sqlt);
		custr_append_printf(sqlt->sqlt_accum, "$%s", tt);
		free(tt);
		return (INGEST_NEXT);

	case STATE_SQL_QUOTED_ID:
		if (chr == '"') {
			sqlt->sqlt_state = STATE_SQL_QUOTED_ID_QUOTE;
			return (INGEST_NEXT);
		}

		if (chr == '\n') {
			errx(1, "unterminated quoted identifier \"%s\"",
			    custr_cstr(sqlt->sqlt_accum));
			return (INGEST_ERROR);
		}

		custr_appendc(sqlt->sqlt_accum, chr);
		return (INGEST_NEXT);

	case STATE_SQL_QUOTED_ID_QUOTE:
		if (chr == '"') {
			custr_appendc(sqlt->sqlt_accum, '"');
			sqlt->sqlt_state = STATE_SQL_QUOTED_ID;
			return (INGEST_NEXT);
		}

		sqlt_commit(sqlt, EVENT_QUOTED_NAME);
		sqlt_pop_state(sqlt);
		return (INGEST_AGAIN);

	case STATE_SQL_STRING:
		if (chr == '\'') {
			sqlt->sqlt_state = STATE_SQL_STRING_QUOTE;
			return (INGEST_NEXT);
		}

		if (chr == '\n') {
			errx(1, "unterminated string \"%s\"",
			    custr_cstr(sqlt->sqlt_accum));
			return (INGEST_ERROR);
		}

		custr_appendc(sqlt->sqlt_accum, chr);
		return (INGEST_NEXT);

	case STATE_SQL_STRING_QUOTE:
		if (chr == '\'') {
			custr_appendc(sqlt->sqlt_accum, '\'');
			sqlt->sqlt_state = STATE_SQL_STRING;
			return (INGEST_NEXT);
		}

		sqlt_commit(sqlt, EVENT_STRING);
		sqlt_pop_state(sqlt);
		return (INGEST_AGAIN);

	case STATE_SQL_OPERATOR:
		if (chr == '=' || chr == ':' || chr == '*' || chr == '+') {
			custr_appendc(sqlt->sqlt_accum, chr);
			return (INGEST_NEXT);
		}

		sqlt_commit(sqlt, EVENT_OPERATOR);
		sqlt_pop_state(sqlt);
		return (INGEST_AGAIN);

	case STATE_SQL_DASH1:
		if (chr == '-') {
			sqlt->sqlt_state = STATE_SQL_DASH_COMMENT;
			return (INGEST_NEXT);
		}

		errx(1, "invalid sequence \"-%c\"", chr);
		return (INGEST_ERROR);

	case STATE_SQL_DASH_COMMENT:
		if (chr == '\n') {
			sqlt_pop_state(sqlt);
		}
		return (INGEST_NEXT);

	case STATE_SQL_NAME:
		if (isalpha(chr) || isdigit(chr) || chr == '_' || chr == '$') {
			custr_appendc(sqlt->sqlt_accum, chr);
			return (INGEST_NEXT);
		}

		if (isspace(chr) || isoper(chr) || isspecial(chr)) {
			sqlt_commit(sqlt, EVENT_NAME);
			sqlt_pop_state(sqlt);
			return (INGEST_AGAIN);
		}

		errx(1, "invalid character \"%c\"", chr);
		return (INGEST_ERROR);

	case STATE_SQL_NUMBER0:
		if (isdigit(chr)) {
			custr_appendc(sqlt->sqlt_accum, chr);
			return (INGEST_NEXT);
		}

		if (chr == '.') {
			custr_appendc(sqlt->sqlt_accum, chr);
			sqlt->sqlt_state = STATE_SQL_NUMBER_DECIMAL;
			return (INGEST_NEXT);
		}

		if (chr == 'e') {
			custr_appendc(sqlt->sqlt_accum, chr);
			sqlt->sqlt_state = STATE_SQL_NUMBER_EXPONENT;
			return (INGEST_NEXT);
		}

		sqlt_commit(sqlt, EVENT_NUMBER);
		sqlt_pop_state(sqlt);
		return (INGEST_AGAIN);

	default:
		errx(1, "ended in state: %d\n", sqlt->sqlt_state);
	}
}

void
sqlt_ingest(sqlt_t *sqlt)
{
	for (;;) {
		inq_t *inq;

		if ((inq = list_head(&sqlt->sqlt_inq)) == NULL) {
			return;
		}

		if (inq->inq_pos >= inq->inq_len) {
			/*
			 * This chunk has no more characters to read.
			 */
			list_remove(&sqlt->sqlt_inq, inq);
			sqlt_inq_free(inq);
			continue;
		}

		char chr = inq->inq_buf[inq->inq_pos];

		ingest_action_t action = sqlt->sqlt_copy != NULL ?
		    sqlt_ingest_copy(sqlt, chr) : sqlt_ingest_sql(sqlt, chr);

		switch (action) {
		case INGEST_NEXT:
			inq->inq_pos++;
			break;

		case INGEST_AGAIN:
			break;

		case INGEST_ERROR:
			errx(1, "INGEST_ERROR!\n");
			break;
		}
	}
}

int
main(int argc, char *argv[])
{
	sqlt_t *sqlt;

	if (argc < 2) {
		errx(1, "usage: %s <input_file>", argv[0]);
	}

	if (sqlt_alloc(&sqlt) != 0) {
		err(1, "sqlt_alloc");
	}

	if ((sqlt->sqlt_file = fopen(argv[1], "r")) == NULL) {
		err(1, "fopen");
	}

	for (;;) {
		inq_t *inq;

		if ((sqlt_inq_alloc(&inq, 1024 * 1024)) != 0) {
			err(1, "sqlt_inq_alloc");
		}

		if (sqlt_inq_read(inq, sqlt->sqlt_file) != 0) {
			err(1, "sqlt_inq_read");
		}

		list_insert_head(&sqlt->sqlt_inq, inq);

		sqlt_ingest(sqlt);
	}

	return (0);
}
