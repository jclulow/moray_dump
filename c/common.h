
#include <sys/list.h>

typedef enum event_type {
	EVENT_NEWLINE = 1,
	EVENT_SPECIAL,
	EVENT_STRING,
	EVENT_QUOTED_NAME,
	EVENT_OPERATOR,
	EVENT_NAME,
	EVENT_NUMBER,
} event_type_t;

typedef struct event {
	event_type_t evt_t;
	char *evt_v;
	list_node_t evt_link;
} event_t;

typedef struct command_copy {
	char *cmdc_table_name;
	strlist_t *cmdc_column_names;
	char cmdc_delimiter;
	char *cmdc_null_string;
} command_copy_t;

extern int parse_command(list_t *, command_copy_t **);

