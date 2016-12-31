
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <err.h>

#include <sys/list.h>
#include <strlist.h>

#include "common.h"


static char *prefix_keywords[] = {
	"abort",
	"alter",
	"analyze",
	"begin",
	"checkpoint",
	"close",
	"cluster",
	"comment",
	"commit",
	"copy",
	"create",
	"deallocate",
	"declare",
	"delete",
	"discard",
	"do",
	"drop",
	"end",
	"execute",
	"explain",
	"fetch",
	"grant",
	"insert",
	"listen",
	"load",
	"lock",
	"move",
	"notify",
	"prepare",
	"reassign",
	"reindex",
	"release",
	"reset",
	"revoke",
	"rollback",
	"savepoint",
	"security",
	"select",
	"set",
	"show",
	"start",
	"stderr",
	"stdin",
	"stdout",
	"truncate",
	"unlisten",
	"update",
	"vacuum",
	"values",
	NULL
};

static int
valid_initial_keyword(const char *nam)
{
	for (int i = 0; prefix_keywords[i] != NULL; i++) {
		if (strcasecmp(nam, prefix_keywords[i]) == 0) {
			return (1);
		}
	}

	return (0);
}

/*
 * XXX this function is structured poorly and leaks memory.
 */
static int
parse_command_copy(list_t *cmd, command_copy_t **out)
{
	char *table_name = NULL;
	strlist_t *column_names;
	enum parse_state {
		PCC_COPY_COMMAND = 1,
		PCC_TABLE_NAME,
		PCC_OPEN_PARENS,
		PCC_COLUMN_NAME,
		PCC_FROM,
		PCC_FROM_WHERE,
		PCC_COMMA,
	} state = PCC_COPY_COMMAND;

	if (strlist_alloc(&column_names, 0) != 0) {
		err(1, "strlist_alloc");
	}

	for (event_t *evt = list_head(cmd); evt != NULL;
	    evt = list_next(cmd, evt)) {
		event_type_t t = evt->evt_t;
		const char *v = evt->evt_v;

		switch (state) {
		case PCC_COPY_COMMAND:
			state = PCC_TABLE_NAME;
			break;

		case PCC_TABLE_NAME:
			if (t == EVENT_QUOTED_NAME) {
				table_name = strdup(v);
			} else if (t == EVENT_NAME) {
				if (valid_initial_keyword(v)) {
					errx(1, "invalid table name \"%s\"", v);
					return (-1);
				}

				table_name = strdup(v);
			} else {
				errx(1, "expected a table name");
				return (-1);
			}
			state = PCC_OPEN_PARENS;
			break;

		case PCC_OPEN_PARENS:
			if (t != EVENT_SPECIAL || strcmp(v, "(") != 0) {
				errx(1, "expected column name list");
				return (-1);
			}
			state = PCC_COLUMN_NAME;
			break;

		case PCC_COLUMN_NAME:
			if (t == EVENT_SPECIAL && strcmp(v, ")") == 0) {
				state = PCC_FROM;
				break;
			}

			const char *cnt = NULL;
			if (t == EVENT_QUOTED_NAME) {
				cnt = v;
			} else if (t == EVENT_NAME) {
				cnt = v; /* XXX lowercase */
			} else {
				errx(1, "expected a column name");
				return (-1);
			}

			/*
			 * XXX check for duplicate columns.
			 */

			strlist_set_tail(column_names, cnt);
			state = PCC_COMMA;
			break;

		case PCC_COMMA:
			if (t == EVENT_SPECIAL && strcmp(v, ",") == 0) {
				state = PCC_COLUMN_NAME;
			} else if (t == EVENT_SPECIAL && strcmp(v, ")") == 0) {
				state = PCC_FROM;
			} else {
				errx(1, "invalid column name list");
				return (-1);
			}
			break;

		case PCC_FROM:
			if (t != EVENT_NAME || strcasecmp(v, "from") != 0) {
				errx(1, "expected key word FROM");
				return (-1);
			}
			state = PCC_FROM_WHERE;
			break;

		case PCC_FROM_WHERE:
			if (t != EVENT_NAME || strcasecmp(v, "stdin") != 0) {
				errx(1, "only STDIN source is supported");
				return (-1);
			}

			command_copy_t *cmdc = NULL;
			if ((cmdc = calloc(1, sizeof (*cmdc))) == NULL) {
				err(1, "calloc");
			}

			cmdc->cmdc_table_name = table_name;
			cmdc->cmdc_column_names = column_names;
			cmdc->cmdc_delimiter = '\t';
			cmdc->cmdc_null_string = strdup("\\N");

			*out = cmdc;
			return (1);
		}
	}

	errx(1, "incomplete or invalid SQL command");
	return (-1);
}

int
parse_command(list_t *cmd, command_copy_t **out)
{
	*out = NULL;

	if (list_is_empty(cmd)) {
		return (0);
	}

	event_t *evt = list_head(cmd);
	if (evt->evt_t != EVENT_NAME) {
		errx(1, "command did not start with an unquoted name");
		return (-1);
	}

	if (!valid_initial_keyword(evt->evt_v)) {
		errx(1, "invalid key word \"%s\"", evt->evt_v);
		return (-1);
	}

	if (strcasecmp(evt->evt_v, "COPY") == 0) {
		return (parse_command_copy(cmd, out));
	} else {
		return (0);
	}
}
