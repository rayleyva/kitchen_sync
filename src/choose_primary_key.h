#ifndef CHOOSE_PRIMARY_KEY_H
#define CHOOSE_PRIMARY_KEY_H

#include <set>

#include "schema.h"

void choose_primary_key_columns(Table &table, set<string> unique_but_nullable_keys) {
	// if the table has no primary key, we need to find a unique key with no nullable columns to act as a surrogate primary key
	Keys sorted_keys(table.keys);
	sort(sorted_keys.begin(), sorted_keys.end()); // order is arbitrary for keys, but both ends must be consistent, so we sort the keys by name

	for (Keys::const_iterator key = sorted_keys.begin(); key != sorted_keys.end() && table.primary_key_columns.empty(); ++key) {
		if (key->unique && !unique_but_nullable_keys.count(key->name)) {
			table.primary_key_columns = key->columns;
		}
	}

	if (table.primary_key_columns.empty()) {
		// of course this falls apart if there are no unique keys, so we don't allow that
		throw runtime_error("Couldn't find a primary or non-nullable unique key on table " + table.name);
	}
}

#endif
